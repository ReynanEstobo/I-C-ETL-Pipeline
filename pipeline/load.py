"""Stage 3 load: publish the approved daily branch summary (docs/Load/Load.md).

One-time database setup:

    python -m pipeline.load setup             create the load objects
    python -m pipeline.load setup --dry-run   validate them, then roll back

Setup creates the permanent target `analytics.daily_branch_summary` and the
restricted `etl_ops` tables the load writes: publication records, load logs,
pending load logs, rejections, and review approvals. The daily load never runs
DDL; an administrator runs this setup once and again only after a versioned
schema change.

Load core (Load.md Sections 1, 3, 4, 5, and 8):

- `LoadConsumer` receives the approved Stage 2 batch inside the read-only
  extraction snapshot and copies it unchanged into a session-scoped table.
- `publish()` runs after the snapshot commits, in a separate read-write
  transaction: pre-load checks, target upsert and publication record,
  post-load checks, then COMMIT or ROLLBACK, with limited retries.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter, sleep
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline import extraction, transformation
from pipeline.connection import SourceConnectionError, connect_source

LOAD_CODE_VERSION = '1.0.0'
TARGET_SCHEMA_VERSION = '1.0.0'
TARGET_TABLE = 'analytics.daily_branch_summary'

# The ten columns received from Stage 2, in contract order (Load.md Section 2).
CONTRACT_COLUMNS = {
    'report_date': 'date',
    'branch_id': 'uuid',
    'order_count': 'bigint',
    'released_order_count': 'bigint',
    'unique_customer_count': 'bigint',
    'total_weight_kg': 'numeric(14,2)',
    'order_value_php': 'numeric(14,2)',
    'payment_count': 'bigint',
    'cash_received_php': 'numeric(14,2)',
    'expense_total_php': 'numeric(14,2)',
}
# Publication columns supplied by Load.
PUBLICATION_COLUMNS = {
    'reporting_cutoff': 'date',
    'load_run_id': 'uuid',
    'refreshed_at': 'timestamp with time zone',
}
TARGET_COLUMNS = {**CONTRACT_COLUMNS, **PUBLICATION_COLUMNS}

LOAD_TABLES = (
    TARGET_TABLE,
    'etl_ops.load_publications',
    'etl_ops.load_logs',
    'etl_ops.pending_load_logs',
    'etl_ops.load_rejections',
    'etl_ops.load_review_approvals',
)

# Named constraints that the load and its checks rely on.
REQUIRED_CONSTRAINTS = {
    TARGET_TABLE: (
        'daily_branch_summary_pkey',
        'daily_branch_summary_branch_id_fkey',
        'daily_branch_summary_report_date_check',
        'daily_branch_summary_order_count_check',
        'daily_branch_summary_released_order_count_check',
        'daily_branch_summary_unique_customer_count_check',
        'daily_branch_summary_payment_count_check',
        'daily_branch_summary_amounts_check',
    ),
    'etl_ops.load_publications': (
        'load_publications_pkey',
        'load_publications_transformation_run_id_key',
        'load_publications_records_published_check',
    ),
    'etl_ops.load_logs': ('load_logs_pkey',),
    'etl_ops.load_rejections': ('load_rejections_pkey',),
    'etl_ops.load_review_approvals': (
        'load_review_approvals_pkey',
        'load_review_approvals_applied_check',
    ),
}

LOAD_SETUP_SQL = r'''
CREATE SCHEMA IF NOT EXISTS analytics;
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS etl_ops;
REVOKE ALL ON SCHEMA etl_ops FROM PUBLIC;

-- Target: one row per completed business date and branch (Load.md Section 2).
-- Measure columns have no defaults, so a missing value fails the write.
CREATE TABLE IF NOT EXISTS analytics.daily_branch_summary (
    report_date DATE NOT NULL,
    branch_id UUID NOT NULL,
    order_count BIGINT NOT NULL,
    released_order_count BIGINT NOT NULL,
    unique_customer_count BIGINT NOT NULL,
    total_weight_kg NUMERIC(14,2) NOT NULL,
    order_value_php NUMERIC(14,2) NOT NULL,
    payment_count BIGINT NOT NULL,
    cash_received_php NUMERIC(14,2) NOT NULL,
    expense_total_php NUMERIC(14,2) NOT NULL,
    reporting_cutoff DATE NOT NULL,
    load_run_id UUID NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT daily_branch_summary_pkey PRIMARY KEY (report_date, branch_id),
    CONSTRAINT daily_branch_summary_branch_id_fkey
        FOREIGN KEY (branch_id) REFERENCES public.branches (id),
    CONSTRAINT daily_branch_summary_report_date_check
        CHECK (report_date < reporting_cutoff),
    CONSTRAINT daily_branch_summary_order_count_check
        CHECK (order_count >= 0),
    CONSTRAINT daily_branch_summary_released_order_count_check
        CHECK (released_order_count BETWEEN 0 AND order_count),
    CONSTRAINT daily_branch_summary_unique_customer_count_check
        CHECK (unique_customer_count BETWEEN 0 AND order_count),
    CONSTRAINT daily_branch_summary_payment_count_check
        CHECK (payment_count >= 0),
    -- NUMERIC accepts NaN, which compares greater than every number.
    CONSTRAINT daily_branch_summary_amounts_check CHECK (
        total_weight_kg >= 0 AND total_weight_kg <> 'NaN'
        AND order_value_php >= 0 AND order_value_php <> 'NaN'
        AND cash_received_php >= 0 AND cash_received_php <> 'NaN'
        AND expense_total_php >= 0 AND expense_total_php <> 'NaN'
    )
);
CREATE INDEX IF NOT EXISTS daily_branch_summary_branch_date_idx
    ON analytics.daily_branch_summary (branch_id, report_date);

-- Publication record, written in the same transaction as the target, so the
-- duplicate-batch, freshness, and commit checks never depend on log delivery.
CREATE TABLE IF NOT EXISTS etl_ops.load_publications (
    load_run_id UUID NOT NULL,
    transformation_run_id UUID NOT NULL,
    extraction_run_id UUID NOT NULL,
    extraction_started_at TIMESTAMPTZ NOT NULL,
    reporting_cutoff DATE NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    records_published BIGINT NOT NULL,
    CONSTRAINT load_publications_pkey PRIMARY KEY (load_run_id),
    CONSTRAINT load_publications_transformation_run_id_key UNIQUE (transformation_run_id),
    CONSTRAINT load_publications_records_published_check CHECK (records_published >= 0)
);
CREATE INDEX IF NOT EXISTS load_publications_extraction_started_idx
    ON etl_ops.load_publications (extraction_started_at DESC);

-- One row per load attempt (Load.md Section 6). Values copied from the
-- handover are nullable so that a rejected handover can still be logged, and
-- unavailable counts stay NULL instead of being reported as zero.
CREATE TABLE IF NOT EXISTS etl_ops.load_logs (
    load_run_id UUID NOT NULL,
    transformation_run_id UUID,
    extraction_run_id UUID,
    retry_of_run_id UUID,
    batch_id TEXT,
    batch_window TEXT,
    published_date_range TEXT,
    reporting_cutoff DATE,
    extraction_started_at TIMESTAMPTZ,
    table_name TEXT NOT NULL,
    load_type TEXT NOT NULL CHECK (load_type IN ('FULL', 'INCREMENTAL')),
    load_started_at TIMESTAMPTZ NOT NULL,
    load_ended_at TIMESTAMPTZ NOT NULL,
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    input_acceptance_status TEXT,
    quarantined_input_records BIGINT CHECK (quarantined_input_records >= 0),
    records_received BIGINT CHECK (records_received >= 0),
    records_inserted BIGINT CHECK (records_inserted >= 0),
    records_updated BIGINT CHECK (records_updated >= 0),
    records_unchanged BIGINT CHECK (records_unchanged >= 0),
    records_deleted BIGINT CHECK (records_deleted >= 0),
    records_rejected BIGINT CHECK (records_rejected >= 0),
    measure_totals JSONB,
    revisions JSONB,
    pre_load_results JSONB NOT NULL DEFAULT '[]'::jsonb,
    post_load_results JSONB NOT NULL DEFAULT '[]'::jsonb,
    load_status TEXT NOT NULL CHECK (load_status IN ('PASS', 'WARNING', 'FAIL')),
    acceptance_status TEXT NOT NULL
        CHECK (acceptance_status IN ('APPROVED', 'MANUAL REVIEW', 'REJECTED')),
    log_delivery_status TEXT NOT NULL
        CHECK (log_delivery_status IN ('PENDING', 'SAVED', 'FAILED')),
    error_code TEXT,
    error_message TEXT,
    load_code_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    CONSTRAINT load_logs_pkey PRIMARY KEY (load_run_id),
    CONSTRAINT load_logs_time_check CHECK (load_ended_at >= load_started_at)
);

-- Scheduled runs queue metadata here before delivering it to load_logs.
CREATE TABLE IF NOT EXISTS etl_ops.pending_load_logs (
    LIKE etl_ops.load_logs INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
);

-- Rejected and held keys (Load.md Section 7). No customer or source records.
CREATE TABLE IF NOT EXISTS etl_ops.load_rejections (
    load_run_id UUID NOT NULL,
    item_number INTEGER NOT NULL CHECK (item_number >= 0),
    report_date DATE,
    branch_id UUID,
    check_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    offending_values JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT load_rejections_pkey PRIMARY KEY (load_run_id, item_number)
);

-- Approvals for removing previously published keys (Load.md Section 8).
CREATE TABLE IF NOT EXISTS etl_ops.load_review_approvals (
    approval_id UUID NOT NULL DEFAULT gen_random_uuid(),
    report_date DATE NOT NULL,
    branch_id UUID NOT NULL,
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    approved_by_role TEXT NOT NULL DEFAULT CURRENT_USER,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_load_run_id UUID,
    applied_at TIMESTAMPTZ,
    CONSTRAINT load_review_approvals_pkey PRIMARY KEY (approval_id),
    CONSTRAINT load_review_approvals_applied_check
        CHECK ((applied_load_run_id IS NULL) = (applied_at IS NULL))
);
-- At most one approval waiting to be applied for each key.
CREATE UNIQUE INDEX IF NOT EXISTS load_review_approvals_open_key_idx
    ON etl_ops.load_review_approvals (report_date, branch_id)
    WHERE applied_load_run_id IS NULL;

-- Retention: records are never updated or truncated, and are deleted only
-- after one year (Load.md Section 7).
CREATE OR REPLACE FUNCTION etl_ops.protect_load_logs()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'DELETE' THEN
        RAISE EXCEPTION 'Load logs cannot be updated or truncated';
    END IF;
    IF OLD.load_ended_at > now() - interval '1 year' THEN
        RAISE EXCEPTION 'Load logs must be retained for at least one year';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION etl_ops.protect_load_rejections()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'DELETE' THEN
        RAISE EXCEPTION 'Load rejections cannot be updated or truncated';
    END IF;
    IF OLD.recorded_at > now() - interval '1 year' THEN
        RAISE EXCEPTION 'Load rejections must be retained for at least one year';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION etl_ops.protect_load_publications()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'DELETE' THEN
        RAISE EXCEPTION 'Load publication records cannot be updated or truncated';
    END IF;
    IF OLD.published_at > now() - interval '1 year' THEN
        RAISE EXCEPTION 'Load publication records must be retained for at least one year';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM etl_ops.load_publications newer
        WHERE newer.extraction_started_at > OLD.extraction_started_at
    ) THEN
        RAISE EXCEPTION 'The latest load publication record cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

-- An approval may be marked as applied once. An approval that was never
-- applied may be withdrawn; an applied one is kept for a year after use.
CREATE OR REPLACE FUNCTION etl_ops.protect_load_review_approvals()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Load review approvals cannot be truncated';
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.applied_load_run_id IS NOT NULL
           OR NEW.applied_load_run_id IS NULL
           OR (NEW.approval_id, NEW.report_date, NEW.branch_id, NEW.reason,
               NEW.approved_by_role, NEW.approved_at)
              IS DISTINCT FROM
              (OLD.approval_id, OLD.report_date, OLD.branch_id, OLD.reason,
               OLD.approved_by_role, OLD.approved_at) THEN
            RAISE EXCEPTION 'An approval can only be marked as applied, once';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.applied_at IS NOT NULL AND OLD.applied_at > now() - interval '1 year' THEN
        RAISE EXCEPTION 'Applied approvals must be retained for at least one year after use';
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS protect_load_logs ON etl_ops.load_logs;
CREATE TRIGGER protect_load_logs
    BEFORE UPDATE OR DELETE ON etl_ops.load_logs
    FOR EACH ROW EXECUTE FUNCTION etl_ops.protect_load_logs();
DROP TRIGGER IF EXISTS prevent_load_logs_truncate ON etl_ops.load_logs;
CREATE TRIGGER prevent_load_logs_truncate
    BEFORE TRUNCATE ON etl_ops.load_logs
    FOR EACH STATEMENT EXECUTE FUNCTION etl_ops.protect_load_logs();

DROP TRIGGER IF EXISTS protect_load_rejections ON etl_ops.load_rejections;
CREATE TRIGGER protect_load_rejections
    BEFORE UPDATE OR DELETE ON etl_ops.load_rejections
    FOR EACH ROW EXECUTE FUNCTION etl_ops.protect_load_rejections();
DROP TRIGGER IF EXISTS prevent_load_rejections_truncate ON etl_ops.load_rejections;
CREATE TRIGGER prevent_load_rejections_truncate
    BEFORE TRUNCATE ON etl_ops.load_rejections
    FOR EACH STATEMENT EXECUTE FUNCTION etl_ops.protect_load_rejections();

DROP TRIGGER IF EXISTS protect_load_publications ON etl_ops.load_publications;
CREATE TRIGGER protect_load_publications
    BEFORE UPDATE OR DELETE ON etl_ops.load_publications
    FOR EACH ROW EXECUTE FUNCTION etl_ops.protect_load_publications();
DROP TRIGGER IF EXISTS prevent_load_publications_truncate ON etl_ops.load_publications;
CREATE TRIGGER prevent_load_publications_truncate
    BEFORE TRUNCATE ON etl_ops.load_publications
    FOR EACH STATEMENT EXECUTE FUNCTION etl_ops.protect_load_publications();

DROP TRIGGER IF EXISTS protect_load_review_approvals ON etl_ops.load_review_approvals;
CREATE TRIGGER protect_load_review_approvals
    BEFORE UPDATE OR DELETE ON etl_ops.load_review_approvals
    FOR EACH ROW EXECUTE FUNCTION etl_ops.protect_load_review_approvals();
DROP TRIGGER IF EXISTS prevent_load_review_approvals_truncate ON etl_ops.load_review_approvals;
CREATE TRIGGER prevent_load_review_approvals_truncate
    BEFORE TRUNCATE ON etl_ops.load_review_approvals
    FOR EACH STATEMENT EXECUTE FUNCTION etl_ops.protect_load_review_approvals();

-- Access (Load.md Section 7): owner only by default; row-level security on.
REVOKE ALL ON analytics.daily_branch_summary, etl_ops.load_publications,
    etl_ops.load_logs, etl_ops.pending_load_logs, etl_ops.load_rejections,
    etl_ops.load_review_approvals FROM PUBLIC;
REVOKE ALL ON FUNCTION etl_ops.protect_load_logs(), etl_ops.protect_load_rejections(),
    etl_ops.protect_load_publications(), etl_ops.protect_load_review_approvals() FROM PUBLIC;
ALTER TABLE analytics.daily_branch_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE etl_ops.load_publications ENABLE ROW LEVEL SECURITY;
ALTER TABLE etl_ops.load_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE etl_ops.pending_load_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE etl_ops.load_rejections ENABLE ROW LEVEL SECURITY;
ALTER TABLE etl_ops.load_review_approvals ENABLE ROW LEVEL SECURITY;

-- Supabase API roles never receive access to the load objects.
DO $revoke$
DECLARE
    api_role TEXT;
BEGIN
    FOREACH api_role IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = api_role) THEN
            EXECUTE format('REVOKE ALL ON SCHEMA analytics FROM %I', api_role);
            EXECUTE format(
                'REVOKE ALL ON analytics.daily_branch_summary, etl_ops.load_publications, '
                'etl_ops.load_logs, etl_ops.pending_load_logs, etl_ops.load_rejections, '
                'etl_ops.load_review_approvals FROM %I', api_role);
        END IF;
    END LOOP;
END
$revoke$;
'''


class LoadSetupError(RuntimeError):
    """The load objects are missing or do not match the Load.md contract."""


def verify_load_objects(cursor) -> list:
    """Return contract problems with the load objects; an empty list means valid.

    The pre-load "Target schema match" check reuses this verification.
    """
    problems = []
    for name in LOAD_TABLES:
        cursor.execute('SELECT to_regclass(%s)', (name,))
        if cursor.fetchone()[0] is None:
            problems.append(f'{name} does not exist')
    if problems:
        return problems

    cursor.execute('''
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    ''', (TARGET_TABLE,))
    actual = {name: (kind, not_null) for name, kind, not_null in cursor.fetchall()}
    for column, kind in TARGET_COLUMNS.items():
        if column not in actual:
            problems.append(f'{TARGET_TABLE}.{column} is missing')
        elif actual[column][0] != kind:
            problems.append(f'{TARGET_TABLE}.{column} is {actual[column][0]}, expected {kind}')
        elif not actual[column][1]:
            problems.append(f'{TARGET_TABLE}.{column} must be NOT NULL')
    for column in actual.keys() - TARGET_COLUMNS.keys():
        problems.append(f'{TARGET_TABLE}.{column} is not part of the target contract')

    for table, names in REQUIRED_CONSTRAINTS.items():
        cursor.execute(
            'SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(%s)', (table,)
        )
        present = {row[0] for row in cursor.fetchall()}
        problems.extend(f'{table} is missing constraint {name}'
                        for name in names if name not in present)

    for table in LOAD_TABLES:
        cursor.execute(
            'SELECT relrowsecurity FROM pg_class WHERE oid = to_regclass(%s)', (table,)
        )
        if cursor.fetchone() != (True,):
            problems.append(f'{table} must have row-level security enabled')
    return problems


def setup_load_objects(dry_run=False) -> None:
    """One-time administrator setup, applied in a single transaction.

    With dry_run, everything is created and verified, then rolled back.
    """
    with connect_source() as connection:
        connection.read_only = False
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(LOAD_SETUP_SQL)
                problems = verify_load_objects(cursor)
            if problems:
                raise LoadSetupError('Load setup verification failed: ' + '; '.join(problems))
            if dry_run:
                raise psycopg.Rollback()


# ---------------------------------------------------------------------------
# Load core: receipt, checks, publication, and decision
# ---------------------------------------------------------------------------

RECEIVED_TABLE = 'pg_temp.load_daily_branch_summary'
TRANSFORMED_TABLE = 'pg_temp.transformed_daily_branch_summary'
KEY_COLUMNS = ('report_date', 'branch_id')
MEASURE_COLUMNS = tuple(column for column in CONTRACT_COLUMNS if column not in KEY_COLUMNS)
ACCEPTED_INPUT_STATUSES = ('APPROVED', 'APPROVED_WITH_QUARANTINE')
SUPPORTED_INPUT_SCHEMA_VERSIONS = ('1.0.0',)
REQUIRED_HANDOVER_FIELDS = (
    'run_id', 'parent_run_id', 'extraction_started_at', 'batch_id', 'batch_window',
    'records_output', 'validation_results', 'code_version', 'rule_version', 'schema_version',
)
# Dedicated advisory key, separate from extraction (...460) and log recovery (...461).
LOAD_LOCK_KEY = (18755, 1163152462)
MAX_PUBLICATION_ATTEMPTS = 3
# Serialization failure, deadlock, and lock not available.
RETRYABLE_SQLSTATES = {'40001', '40P01', '55P03'}
STORAGE_LIMIT_SETTING = 'DATABASE_SIZE_LIMIT_MB'
REQUIRED_PRIVILEGES = {
    TARGET_TABLE: ('SELECT', 'INSERT', 'UPDATE', 'DELETE'),
    'etl_ops.load_publications': ('SELECT', 'INSERT'),
    'etl_ops.load_logs': ('SELECT', 'INSERT'),
    'etl_ops.pending_load_logs': ('SELECT', 'INSERT'),
    'etl_ops.load_rejections': ('SELECT', 'INSERT'),
    'etl_ops.load_review_approvals': ('SELECT', 'UPDATE'),
}
DATE_PATTERN = re.compile(r'\d{4}-\d{2}-\d{2}')
WINDOW_START_KEYS = ('start', 'from', 'lower', 'earliest', 'earliest_date',
                     'earliest_business_date', 'earliest_processed_date', 'window_start')
WINDOW_CUTOFF_KEYS = ('cutoff', 'reporting_cutoff', 'end', 'to', 'upper', 'window_end')


class LoadRejected(RuntimeError):
    """A critical check failed; the publication transaction rolls back."""

    def __init__(self, code, message, *, retryable=False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class LoadHeld(RuntimeError):
    """A holding warning: roll back and keep the batch for manual review."""

    code = 'HISTORY_REMOVAL_HELD'


@dataclass
class LoadBatch:
    """The received Stage 2 batch and its handover metadata."""

    transformation_run_id: str
    extraction_run_id: str
    extraction_started_at: object
    batch_id: object
    batch_window: object
    window_start: object
    window_cutoff: object
    input_acceptance_status: str
    quarantined_input_records: int
    records_output: int
    schema_version: str
    reporting_cutoff: object = None
    records_received: object = None
    measure_totals: dict = field(default_factory=dict)


@dataclass
class LoadResult:
    """Outcome of one publication attempt, identified by its load_run_id."""

    load_run_id: str
    retry_of_run_id: object
    load_started_at: datetime
    load_ended_at: object = None
    duration_ms: object = None
    load_status: str = 'FAIL'
    acceptance_status: str = 'REJECTED'
    records_inserted: object = None
    records_updated: object = None
    records_unchanged: object = None
    records_deleted: object = None
    records_rejected: object = None
    published_date_range: object = None
    refreshed_at: object = None
    pre_load_results: list = field(default_factory=list)
    post_load_results: list = field(default_factory=list)
    revisions: list = field(default_factory=list)
    rejections: list = field(default_factory=list)
    error_code: object = None
    error_message: object = None
    retryable: bool = False
    commit_verified: object = None


def record_check(results, name, passed, *, expected=None, actual=None,
                 warning=False, holds_publication=False):
    """Append a check outcome and return its status: PASS, WARNING, or FAIL.

    A failed check is a WARNING when `warning` is set, otherwise a critical
    FAIL. Only a WARNING with `holds_publication` leads to MANUAL REVIEW.
    """
    status = 'PASS' if passed else ('WARNING' if warning else 'FAIL')
    results.append({
        'check': name,
        'status': status,
        'holds_publication': holds_publication and status == 'WARNING',
        'expected': expected,
        'actual': actual,
    })
    return status


def decide_acceptance(checks) -> str:
    """Load.md Section 8 precedence: FAIL, then a holding WARNING, then APPROVED."""
    if any(check['status'] == 'FAIL' for check in checks):
        return 'REJECTED'
    if any(check['holds_publication'] for check in checks):
        return 'MANUAL REVIEW'
    return 'APPROVED'


def overall_load_status(checks, acceptance_status) -> str:
    """Validation outcome stored as load_status: PASS, WARNING, or FAIL."""
    if acceptance_status == 'REJECTED':
        return 'FAIL'
    if any(check['status'] == 'WARNING' for check in checks):
        return 'WARNING'
    return 'PASS'


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and DATE_PATTERN.match(value.strip()):
        return date.fromisoformat(value.strip()[:10])
    return None


def parse_batch_window(value):
    """Return (earliest processed date, exclusive cutoff) from a Stage 2 batch window.

    Accepts a mapping with start and cutoff keys, a two-item sequence, or text
    such as '2026-10-12 to 2026-11-12' or '[2026-10-12,2026-11-12)'. A NULL
    lower date, which Stage 2 records for empty input, returns None as start.
    """
    if isinstance(value, dict):
        start = next((value[key] for key in WINDOW_START_KEYS if key in value), None)
        cutoff = next((value[key] for key in WINDOW_CUTOFF_KEYS if key in value), None)
        return _as_date(start), _as_date(cutoff)
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _as_date(value[0]), _as_date(value[1])
    if isinstance(value, str):
        found = DATE_PATTERN.findall(value)
        if len(found) == 2:
            return date.fromisoformat(found[0]), date.fromisoformat(found[1])
        if len(found) == 1:
            return None, date.fromisoformat(found[0])
    return None, None


def storage_limit_bytes():
    """The configured plan database size limit in bytes, or None if unset."""
    try:
        limit_mb = float(os.environ.get(STORAGE_LIMIT_SETTING, ''))
    except ValueError:
        return None
    return int(limit_mb * 1024 * 1024) if limit_mb > 0 else None


def _columns(names, alias=None):
    return sql.SQL(', ').join(
        sql.Identifier(alias, name) if alias else sql.Identifier(name) for name in names
    )


def _measures_json(alias):
    return sql.SQL('json_build_object({})').format(sql.SQL(', ').join(
        sql.SQL('{}, {}').format(sql.Literal(name), sql.Identifier(alias, name))
        for name in MEASURE_COLUMNS
    ))


def _measures_differ():
    """True when a received row (r) and the published row (t) differ in any measure."""
    return sql.SQL('({}) IS DISTINCT FROM ({})').format(
        _columns(MEASURE_COLUMNS, 'r'), _columns(MEASURE_COLUMNS, 't')
    )


def measure_totals(cursor, table) -> dict:
    """Exact row count and per-measure totals of a summary table."""
    sums = sql.SQL(', ').join(
        sql.SQL('coalesce(sum({}), 0)').format(sql.Identifier(name)) for name in MEASURE_COLUMNS
    )
    cursor.execute(sql.SQL('SELECT count(*), {} FROM {}').format(sums, sql.SQL(table)))
    row = cursor.fetchone()
    return {'rows': row[0], **dict(zip(MEASURE_COLUMNS, row[1:]))}


def input_contract_problems(cursor) -> list:
    """Compare the Stage 2 output columns and types with the ten contract columns."""
    cursor.execute('''
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
    ''', (TRANSFORMED_TABLE,))
    actual = dict(cursor.fetchall())
    if not actual:
        return [f'{TRANSFORMED_TABLE} is not available in this session']
    problems = []
    for column, kind in CONTRACT_COLUMNS.items():
        if column not in actual:
            problems.append(f'{column} is missing')
        elif actual[column] != kind:
            problems.append(f'{column} is {actual[column]}, expected {kind}')
    return problems


class LoadConsumer:
    """Receives the approved Stage 2 batch inside the extraction snapshot.

    Stage 2 calls acknowledge(offered), then consume(connection, offered),
    while the read-only snapshot transaction is open (Load.md Section 1,
    steps 1-2). Load copies the rows unchanged into a session-scoped table so
    that publish() can run in a separate read-write transaction after the
    snapshot commits. Receipt never commits or closes the connection.
    """

    def __init__(self):
        self.load_run_id = None
        self.receipt_started_at = None
        self.receipt_checks = []
        self.handover_meta = None
        self.batch = None
        self.failure = None
        self.results = None
        self.publish_error = None
        self.log_entries = []
        self.log_error = None

    @staticmethod
    def prepare(connection):
        """Create the empty receipt table in the read-write setup transaction."""
        definitions = sql.SQL(', ').join(
            sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
            for column, kind in CONTRACT_COLUMNS.items()
        )
        connection.execute(sql.SQL(
            'CREATE TEMP TABLE IF NOT EXISTS load_daily_branch_summary ({}) '
            'ON COMMIT PRESERVE ROWS'
        ).format(definitions))
        connection.execute(f'DELETE FROM {RECEIVED_TABLE}')

    def _reject(self, code, message):
        self.failure = LoadRejected(code, message)
        raise self.failure

    def acknowledge(self, offered):
        """Receipt checks on the handover metadata: status and completeness."""
        self.load_run_id = str(uuid4())
        self.receipt_started_at = datetime.now(timezone.utc)
        self.receipt_checks = checks = []
        self.batch = None
        self.failure = None
        self.results = None
        # Kept for the load log even when the handover is rejected.
        self.handover_meta = {key: offered.get(key) for key in (
            'run_id', 'parent_run_id', 'extraction_started_at', 'batch_id',
            'batch_window', 'acceptance_status')}

        status = offered.get('acceptance_status')
        if record_check(checks, 'handover_status', status in ACCEPTED_INPUT_STATUSES,
                        expected=list(ACCEPTED_INPUT_STATUSES), actual=status) == 'FAIL':
            self._reject('HANDOVER_NOT_APPROVED',
                         f'Load accepts only approved handovers, not {status}.')

        missing = [name for name in REQUIRED_HANDOVER_FIELDS if offered.get(name) in (None, '')]
        if status == 'APPROVED_WITH_QUARANTINE' and offered.get('quarantined_records') is None:
            missing.append('quarantined_records')
        window_start, window_cutoff = parse_batch_window(offered.get('batch_window'))
        if offered.get('batch_window') not in (None, '') and window_cutoff is None:
            missing.append('readable batch_window cutoff')
        try:
            records_output = int(offered.get('records_output'))
            quarantined = int(offered.get('quarantined_records') or 0)
            if records_output < 0 or quarantined < 0:
                raise ValueError
        except (TypeError, ValueError):
            if 'records_output' not in missing:
                missing.append('valid records_output and quarantined_records counts')
        if record_check(checks, 'handover_completeness', not missing,
                        expected='all required handover fields',
                        actual=missing or 'complete') == 'FAIL':
            self._reject('HANDOVER_INCOMPLETE', 'Handover is missing: ' + ', '.join(missing) + '.')

        self.batch = LoadBatch(
            transformation_run_id=str(offered['run_id']),
            extraction_run_id=str(offered['parent_run_id']),
            extraction_started_at=offered['extraction_started_at'],
            batch_id=offered['batch_id'],
            batch_window=offered['batch_window'],
            window_start=window_start,
            window_cutoff=window_cutoff,
            input_acceptance_status=status,
            quarantined_input_records=quarantined,
            records_output=records_output,
            schema_version=str(offered['schema_version']),
        )
        return {'run_id': offered['run_id'], 'acceptance_status': status}

    def consume(self, connection, offered):
        """Receipt checks on the rows, then an unchanged copy with captured totals."""
        checks = self.receipt_checks
        if self.batch is None or str(offered.get('run_id')) != self.batch.transformation_run_id:
            record_check(checks, 'handover_status', False,
                         expected='the acknowledged run', actual=offered.get('run_id'))
            self._reject('HANDOVER_MISMATCH', 'Receipt does not match the acknowledged run.')
        batch = self.batch
        with connection.cursor() as cursor:
            # The cutoff is the Asia/Manila date on which extraction started.
            cursor.execute("SELECT (%s::timestamptz AT TIME ZONE 'Asia/Manila')::date",
                           (batch.extraction_started_at,))
            batch.reporting_cutoff = cursor.fetchone()[0]
            if batch.window_cutoff != batch.reporting_cutoff:
                record_check(checks, 'handover_completeness', False,
                             expected=f'cutoff {batch.reporting_cutoff}',
                             actual=f'batch_window cutoff {batch.window_cutoff}')
                self._reject('HANDOVER_INCOMPLETE',
                             'The batch window cutoff does not match the extraction start.')

            problems = input_contract_problems(cursor)
            if batch.schema_version not in SUPPORTED_INPUT_SCHEMA_VERSIONS:
                problems.append(f'unsupported Stage 2 schema_version {batch.schema_version}')
            if record_check(checks, 'input_contract', not problems,
                            expected='ten contract columns with the Section 2 types',
                            actual=problems or 'match') == 'FAIL':
                self._reject('SCHEMA_MISMATCH',
                             'Transformed output does not match the contract: ' + '; '.join(problems))

            cursor.execute(f'DELETE FROM {RECEIVED_TABLE}')
            cursor.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}').format(
                sql.SQL(RECEIVED_TABLE), _columns(CONTRACT_COLUMNS),
                _columns(CONTRACT_COLUMNS), sql.SQL(TRANSFORMED_TABLE)))
            copied = cursor.rowcount
            cursor.execute(f'SELECT count(*) FROM {TRANSFORMED_TABLE}')
            offered_rows = cursor.fetchone()[0]
            counts = {'copied': copied, 'transformed_output': offered_rows,
                      'records_output': batch.records_output}
            if record_check(checks, 'input_record_count',
                            copied == offered_rows == batch.records_output,
                            expected='copied = transformed output = records_output',
                            actual=counts) == 'FAIL':
                self._reject('INPUT_COUNT_MISMATCH', f'Received row counts differ: {counts}.')
            batch.records_received = copied
            batch.measure_totals = measure_totals(cursor, RECEIVED_TABLE)

    def _queue_unverified(self, result):
        """Before COMMIT, queue the load log with an unverified outcome (Load.md Section 6)."""
        try:
            queue_load_log(build_load_log(self, result, provisional=True))
        except (OSError, ValueError, subprocess.SubprocessError):
            raise LoadRejected('LOG_QUEUE_FAILED',
                               'Load metadata could not be queued before COMMIT.') from None

    def after_commit(self, connection):
        """After the snapshot commits: publish, persist load metadata, then clear
        the receipt table (Load.md Section 1, steps 3-4).

        Never raises, so a load problem is not reported as an extraction failure.
        """
        if self.batch is None or self.failure is not None or self.batch.records_received is None:
            return
        try:
            self.results = publish(connection, self, before_commit=self._queue_unverified)
        except Exception:  # noqa: BLE001 - recorded in the load log instead
            self.results = []
            self.publish_error = ('Publication stopped unexpectedly; check '
                                  'etl_ops.load_publications before rerunning.')
        self.log_entries = [build_load_log(self, result) for result in self.results] or [
            build_load_log(self, error_code='LOAD_FAILED', error_message=self.publish_error)]
        try:
            save_load_logs(self.log_entries)
        except LOG_DELIVERY_ERRORS:
            self.log_error = ('Load metadata was not fully delivered; it stays queued. '
                              'Run python -m pipeline.load --flush-logs.')
        try:
            discard_received_batch(connection)
        except psycopg.Error:
            pass


def discard_received_batch(connection):
    """Clear the session-scoped receipt table once load metadata is persisted."""
    with connection.transaction():
        connection.execute(f'DELETE FROM {RECEIVED_TABLE}')


def run_preload_checks(cursor, batch, result):
    """Load.md Section 4, before writing. Raises LoadRejected or LoadHeld."""
    checks = result.pre_load_results

    cursor.execute('SHOW transaction_read_only')
    mode = cursor.fetchone()[0]
    if record_check(checks, 'target_connection_and_write_mode', mode == 'off',
                    expected='transaction_read_only = off', actual=mode) == 'FAIL':
        raise LoadRejected('READ_ONLY_TRANSACTION', 'The publication transaction is not read-write.')

    missing = []
    for table, privileges in REQUIRED_PRIVILEGES.items():
        cursor.execute('SELECT to_regclass(%s) IS NOT NULL', (table,))
        if not cursor.fetchone()[0]:
            missing.append(f'{table} does not exist')
            continue
        for privilege in privileges:
            # has_table_privilege with a list is true if ANY privilege is held,
            # so each privilege is checked on its own.
            cursor.execute('SELECT has_table_privilege(%s, %s)', (table, privilege))
            if not cursor.fetchone()[0]:
                missing.append(f'{privilege} on {table}')
    if record_check(checks, 'write_permissions', not missing,
                    expected='all required privileges', actual=missing or 'granted') == 'FAIL':
        raise LoadRejected('PERMISSION_DENIED', 'Missing: ' + '; '.join(missing) + '.')

    problems = verify_load_objects(cursor)
    if record_check(checks, 'target_schema_match', not problems,
                    expected='Load.md Section 2 contract', actual=problems or 'match') == 'FAIL':
        raise LoadRejected('SCHEMA_MISMATCH', 'Target does not match the contract: ' + '; '.join(problems))

    cursor.execute('SELECT pg_try_advisory_xact_lock(%s, %s)', LOAD_LOCK_KEY)
    locked = cursor.fetchone()[0]
    if record_check(checks, 'overlap_lock', locked, expected='lock acquired',
                    actual='acquired' if locked else 'held by another load') == 'FAIL':
        raise LoadRejected('LOAD_LOCK_BUSY', 'Another load holds the publication lock.', retryable=True)

    cursor.execute('SELECT load_run_id FROM etl_ops.load_publications WHERE transformation_run_id = %s',
                   (batch.transformation_run_id,))
    previous = cursor.fetchone()
    if record_check(checks, 'duplicate_batch', previous is None, expected='not yet published',
                    actual=f'published by {previous[0]}' if previous else 'not yet published') == 'FAIL':
        raise LoadRejected('DUPLICATE_BATCH', 'This transformation run is already published.')

    cursor.execute('''
        SELECT max(extraction_started_at),
               coalesce(%s::timestamptz > max(extraction_started_at), true)
        FROM etl_ops.load_publications
    ''', (batch.extraction_started_at,))
    latest, fresh = cursor.fetchone()
    if record_check(checks, 'snapshot_freshness', fresh,
                    expected=f'later than {latest}' if latest else 'no publication yet',
                    actual=str(batch.extraction_started_at)) == 'FAIL':
        raise LoadRejected('STALE_BATCH', 'A newer snapshot is already published.')

    cursor.execute('SELECT pg_database_size(current_database()) + pg_total_relation_size(%s)',
                   (RECEIVED_TABLE,))
    projected = cursor.fetchone()[0]
    limit = storage_limit_bytes()
    if limit is None:
        record_check(checks, 'storage_capacity', False, warning=True,
                     expected=f'{STORAGE_LIMIT_SETTING} configured', actual='not configured')
    else:
        used = f'{projected / limit:.1%} of the plan limit'
        if projected > 0.95 * limit:
            record_check(checks, 'storage_capacity', False, expected='at most 95%', actual=used)
            raise LoadRejected('STORAGE_LIMIT', 'Database storage would exceed 95% of the plan limit.')
        record_check(checks, 'storage_capacity', projected < 0.80 * limit, warning=True,
                     expected='below 80%', actual=used)

    cursor.execute(sql.SQL('''
        SELECT t.report_date, t.branch_id, a.approval_id IS NOT NULL, {previous}
        FROM analytics.daily_branch_summary t
        LEFT JOIN etl_ops.load_review_approvals a
          ON a.applied_load_run_id IS NULL
         AND a.report_date = t.report_date AND a.branch_id = t.branch_id
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_temp.load_daily_branch_summary r
            WHERE r.report_date = t.report_date AND r.branch_id = t.branch_id)
        ORDER BY t.report_date, t.branch_id
    ''').format(previous=_measures_json('t')))
    absent = cursor.fetchall()
    held = [row for row in absent if not row[2]]
    record_check(checks, 'published_history_removal', not held, warning=True, holds_publication=True,
                 expected='every absent key approved for removal',
                 actual={'absent': len(absent), 'approved': len(absent) - len(held), 'held': len(held)})
    if held:
        result.rejections.extend({
            'report_date': report_date, 'branch_id': branch_id,
            'check_name': 'published_history_removal',
            'reason': 'Previously published key is absent from the batch and not approved for removal.',
            'offending_values': previous,
        } for report_date, branch_id, _, previous in held)
        raise LoadHeld(f'{len(held)} previously published keys are absent and not approved for removal.')


def write_target(cursor, batch, result):
    """Load.md Section 3: upsert on the match key, remove approved absent keys,
    mark approvals as applied, and record the publication."""
    differ = _measures_differ()
    cursor.execute(sql.SQL('''
        SELECT count(*) FILTER (WHERE t.report_date IS NULL),
               count(*) FILTER (WHERE t.report_date IS NOT NULL AND {differ}),
               count(*) FILTER (WHERE t.report_date IS NOT NULL AND NOT {differ})
        FROM pg_temp.load_daily_branch_summary r
        LEFT JOIN analytics.daily_branch_summary t
          ON t.report_date = r.report_date AND t.branch_id = r.branch_id
    ''').format(differ=differ))
    result.records_inserted, result.records_updated, result.records_unchanged = cursor.fetchone()

    cursor.execute(sql.SQL('''
        SELECT coalesce(json_agg(json_build_object(
                   'report_date', r.report_date, 'branch_id', r.branch_id, 'change', 'updated',
                   'previous', {previous}, 'new', {new})
               ORDER BY r.report_date, r.branch_id), '[]'::json)
        FROM pg_temp.load_daily_branch_summary r
        JOIN analytics.daily_branch_summary t
          ON t.report_date = r.report_date AND t.branch_id = r.branch_id
        WHERE {differ}
    ''').format(previous=_measures_json('t'), new=_measures_json('r'), differ=differ))
    revisions = cursor.fetchone()[0]

    cursor.execute(sql.SQL('''
        DELETE FROM analytics.daily_branch_summary t
        USING etl_ops.load_review_approvals a
        WHERE a.applied_load_run_id IS NULL
          AND a.report_date = t.report_date AND a.branch_id = t.branch_id
          AND NOT EXISTS (
              SELECT 1 FROM pg_temp.load_daily_branch_summary r
              WHERE r.report_date = t.report_date AND r.branch_id = t.branch_id)
        RETURNING json_build_object('report_date', t.report_date, 'branch_id', t.branch_id,
                                    'change', 'removed', 'previous', {previous})
    ''').format(previous=_measures_json('t')))
    removed = [row[0] for row in cursor.fetchall()]
    result.records_deleted = len(removed)
    result.revisions = revisions + removed

    updates = sql.SQL(', ').join(
        sql.SQL('{0} = EXCLUDED.{0}').format(sql.Identifier(name))
        for name in TARGET_COLUMNS if name not in KEY_COLUMNS
    )
    cursor.execute(sql.SQL('''
        INSERT INTO analytics.daily_branch_summary ({target})
        SELECT {contract}, %s::date, %s::uuid, now() FROM pg_temp.load_daily_branch_summary
        ON CONFLICT (report_date, branch_id) DO UPDATE SET {updates}
    ''').format(target=_columns(TARGET_COLUMNS), contract=_columns(CONTRACT_COLUMNS), updates=updates),
        (batch.reporting_cutoff, result.load_run_id))

    # Approvals are valid only for the next APPROVED load (Load.md Section 8).
    cursor.execute('''
        UPDATE etl_ops.load_review_approvals
        SET applied_load_run_id = %s, applied_at = now()
        WHERE applied_load_run_id IS NULL
    ''', (result.load_run_id,))

    cursor.execute('''
        INSERT INTO etl_ops.load_publications (
            load_run_id, transformation_run_id, extraction_run_id, extraction_started_at,
            reporting_cutoff, published_at, records_published)
        VALUES (%s::uuid, %s::uuid, %s::uuid, %s::timestamptz, %s::date, now(), %s)
        RETURNING published_at
    ''', (result.load_run_id, batch.transformation_run_id, batch.extraction_run_id,
          batch.extraction_started_at, batch.reporting_cutoff, batch.records_received))
    result.refreshed_at = cursor.fetchone()[0]


def run_postload_checks(cursor, batch, result):
    """Load.md Section 5: every check runs before COMMIT; any FAIL rolls back."""
    checks = result.post_load_results
    received = batch.records_received
    target = sql.SQL(TARGET_TABLE)
    received_table = sql.SQL(RECEIVED_TABLE)

    loaded = result.records_inserted + result.records_updated + result.records_unchanged
    record_check(checks, 'record_reconciliation', received == loaded + 0,
                 expected=f'received {received} = inserted + updated + unchanged + rejected',
                 actual={'inserted': result.records_inserted, 'updated': result.records_updated,
                         'unchanged': result.records_unchanged, 'rejected': 0})

    cursor.execute(f'SELECT count(*), count(*) FILTER (WHERE load_run_id = %s) FROM {TARGET_TABLE}',
                   (result.load_run_id,))
    total, stamped = cursor.fetchone()
    record_check(checks, 'target_row_count', total == stamped == received,
                 expected=received, actual={'total': total, 'this_load': stamped})

    cursor.execute(f'SELECT count(*) - count(DISTINCT (report_date, branch_id)) FROM {TARGET_TABLE}')
    duplicates = cursor.fetchone()[0]
    record_check(checks, 'key_uniqueness', duplicates == 0, expected=0, actual=duplicates)

    cursor.execute(f'''
        SELECT t.report_date, t.branch_id FROM {TARGET_TABLE} t
        WHERE NOT EXISTS (SELECT 1 FROM public.branches b WHERE b.id = t.branch_id)
        ORDER BY 1, 2
    ''')
    orphans = cursor.fetchall()
    if record_check(checks, 'referential_integrity', not orphans,
                    expected='every branch_id in public.branches', actual=len(orphans)) == 'FAIL':
        result.rejections.extend({
            'report_date': report_date, 'branch_id': branch_id,
            'check_name': 'referential_integrity',
            'reason': 'branch_id has no matching public.branches row.', 'offending_values': None,
        } for report_date, branch_id in orphans)

    nulls = sql.SQL(' + ').join(
        sql.SQL('(count(*) - count({}))').format(sql.Identifier(name)) for name in TARGET_COLUMNS
    )
    cursor.execute(sql.SQL('SELECT {} FROM {}').format(nulls, target))
    null_count = cursor.fetchone()[0]
    record_check(checks, 'required_values', null_count == 0, expected=0, actual=null_count)

    contract = _columns(CONTRACT_COLUMNS)
    cursor.execute(sql.SQL('''
        SELECT (SELECT count(*) FROM (SELECT {c} FROM {t} EXCEPT SELECT {c} FROM {r}) extra),
               (SELECT count(*) FROM (SELECT {c} FROM {r} EXCEPT SELECT {c} FROM {t}) lost)
    ''').format(c=contract, t=target, r=received_table))
    extra, lost = cursor.fetchone()
    if record_check(checks, 'exact_content_match', extra == lost == 0, expected='0 and 0',
                    actual={'target_only': extra, 'received_only': lost}) == 'FAIL':
        cursor.execute(sql.SQL('''
            SELECT report_date, branch_id FROM (SELECT {c} FROM {t} EXCEPT SELECT {c} FROM {r}) x
            ORDER BY 1, 2 LIMIT 50
        ''').format(c=contract, t=target, r=received_table))
        result.rejections.extend({
            'report_date': report_date, 'branch_id': branch_id,
            'check_name': 'exact_content_match',
            'reason': 'Published values differ from the received batch.', 'offending_values': None,
        } for report_date, branch_id in cursor.fetchall())

    published = measure_totals(cursor, TARGET_TABLE)
    differences = {name: {'received': str(batch.measure_totals[name]), 'published': str(published[name])}
                   for name in MEASURE_COLUMNS if published[name] != batch.measure_totals[name]}
    record_check(checks, 'measure_reconciliation', not differences,
                 expected='target totals = totals captured at receipt',
                 actual=differences or 'equal')

    cursor.execute(f'''
        SELECT count(DISTINCT load_run_id), count(DISTINCT refreshed_at),
               count(DISTINCT reporting_cutoff),
               coalesce(bool_and(load_run_id = %s AND reporting_cutoff = %s
                                 AND refreshed_at = now()), false)
        FROM {TARGET_TABLE}
    ''', (result.load_run_id, batch.reporting_cutoff))
    stamp = cursor.fetchone()
    record_check(checks, 'single_publication', stamp == (1, 1, 1, True),
                 expected='one load_run_id, refreshed_at, and reporting_cutoff for this load',
                 actual={'load_run_ids': stamp[0], 'refreshed_at_values': stamp[1],
                         'cutoffs': stamp[2], 'matches_this_load': stamp[3]})

    cursor.execute(f'''
        SELECT min(report_date), max(report_date),
               coalesce(bool_and(report_date < %s
                                 AND (%s::date IS NULL OR report_date >= %s::date)), false)
        FROM {TARGET_TABLE}
    ''', (batch.reporting_cutoff, batch.window_start, batch.window_start))
    first, last, inside = cursor.fetchone()
    result.published_date_range = f'{first} to {last}' if first else None
    record_check(checks, 'publication_window', inside,
                 expected=f'from {batch.window_start} and before {batch.reporting_cutoff}',
                 actual=result.published_date_range)

    record_check(checks, 'load_errors_and_interruptions', True, expected='no SQL error before COMMIT', actual='none')

    cursor.execute('''
        SELECT records_published, published_at = now()
        FROM etl_ops.load_publications WHERE load_run_id = %s
    ''', (result.load_run_id,))
    rows = cursor.fetchall()
    record_check(checks, 'publication_record', rows == [(received, True)],
                 expected=f'one row with {received} records', actual=len(rows))

    failed = [check['check'] for check in checks if check['status'] == 'FAIL']
    if failed:
        raise LoadRejected('RECONCILIATION_FAILED', 'Post-load checks failed: ' + ', '.join(failed) + '.')


def confirm_publication(connection, load_run_id) -> bool:
    """After COMMIT, confirm the publication record exists (Load.md Section 5)."""
    with connection.transaction():
        row = connection.execute(
            'SELECT 1 FROM etl_ops.load_publications WHERE load_run_id = %s', (load_run_id,)
        ).fetchone()
    return row is not None


def _database_error_code(sqlstate):
    return {
        '57014': 'STATEMENT_TIMEOUT',
        '42501': 'PERMISSION_DENIED',
        '23503': 'FOREIGN_KEY_VIOLATION',
        '23514': 'CONSTRAINT_VIOLATION',
        '23505': 'DUPLICATE_KEY',
    }.get(sqlstate, 'TRANSIENT_DATABASE_ERROR' if sqlstate in RETRYABLE_SQLSTATES else 'LOAD_FAILED')


def publish_attempt(connection, batch, load_run_id, retry_of_run_id=None,
                    before_commit=None) -> LoadResult:
    """One publication transaction (Load.md Section 1, step 3).

    before_commit(result), when given, runs after every check passes and just
    before COMMIT; raising LoadRejected from it rolls the publication back.
    """
    result = LoadResult(load_run_id=load_run_id, retry_of_run_id=retry_of_run_id,
                        load_started_at=datetime.now(timezone.utc))
    timer = perf_counter()
    committing = False
    try:
        # The session default is read-only; publication switches explicitly.
        connection.read_only = False
        connection.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '5s'")
                cursor.execute("SET LOCAL statement_timeout = '60s'")
                run_preload_checks(cursor, batch, result)
                write_target(cursor, batch, result)
                run_postload_checks(cursor, batch, result)
                if before_commit is not None:
                    before_commit(result)
            committing = True
        result.commit_verified = confirm_publication(connection, load_run_id)
        if result.commit_verified:
            result.acceptance_status = decide_acceptance(
                result.pre_load_results + result.post_load_results)
        else:
            result.error_code = 'COMMIT_UNVERIFIED'
            result.error_message = 'COMMIT returned, but no publication record was found.'
    except LoadHeld as held:
        result.acceptance_status = 'MANUAL REVIEW'
        result.error_code, result.error_message = held.code, str(held)
    except LoadRejected as rejected:
        result.acceptance_status = 'REJECTED'
        result.error_code, result.error_message = rejected.code, str(rejected)
        result.retryable = rejected.retryable
    except psycopg.Error as error:
        result.acceptance_status = 'REJECTED'
        if committing:
            # Connection lost during COMMIT: the outcome must be verified, never assumed.
            result.error_code = 'COMMIT_OUTCOME_UNKNOWN'
            result.error_message = 'The connection failed during COMMIT; verify load_publications.'
            if not connection.closed:
                try:
                    result.commit_verified = confirm_publication(connection, load_run_id)
                except psycopg.Error:
                    result.commit_verified = None
                if result.commit_verified:
                    result.acceptance_status = 'APPROVED'
                    result.error_code = result.error_message = None
        else:
            result.error_code = _database_error_code(error.sqlstate)
            result.error_message = f'SQLSTATE {error.sqlstate}: {error.diag.message_primary}'
            result.retryable = error.sqlstate in RETRYABLE_SQLSTATES and not connection.closed
            record_check(result.post_load_results if result.post_load_results else result.pre_load_results,
                         'load_errors_and_interruptions', False, expected='no SQL error before COMMIT',
                         actual=result.error_code)
    finally:
        if result.acceptance_status != 'APPROVED' and result.error_code != 'COMMIT_OUTCOME_UNKNOWN':
            # Nothing was written: the transaction rolled back.
            result.records_inserted = result.records_updated = 0
            result.records_unchanged = result.records_deleted = 0
            result.records_rejected = batch.records_received
            result.revisions = []
            result.refreshed_at = None
        elif result.acceptance_status == 'APPROVED':
            result.records_rejected = 0
        result.load_status = overall_load_status(
            result.pre_load_results + result.post_load_results, result.acceptance_status)
        result.load_ended_at = datetime.now(timezone.utc)
        result.duration_ms = int((perf_counter() - timer) * 1000)
        if not connection.closed:
            try:
                connection.read_only = True
            except psycopg.Error:
                pass
    return result


def publish(connection, consumer, before_commit=None) -> list:
    """Publish a received batch with limited retries (Load.md Sections 3 and 8).

    Runs on the run's connection after the snapshot transaction has committed.
    Only lock and serialization errors are retried: at most three attempts in
    total, waiting 2 and then 4 seconds. Each attempt has its own load_run_id,
    linked by retry_of_run_id. Returns every attempt's LoadResult.
    """
    if consumer.batch is None or consumer.failure is not None:
        return []
    results = []
    load_run_id, retry_of_run_id = consumer.load_run_id, None
    for attempt in range(1, MAX_PUBLICATION_ATTEMPTS + 1):
        result = publish_attempt(connection, consumer.batch, load_run_id, retry_of_run_id,
                                 before_commit)
        results.append(result)
        if not result.retryable or attempt == MAX_PUBLICATION_ATTEMPTS:
            break
        sleep(2 * attempt)
        load_run_id, retry_of_run_id = str(uuid4()), load_run_id
    return results


# ---------------------------------------------------------------------------
# Load metadata and pending queue (Load.md Sections 6 and 7)
# ---------------------------------------------------------------------------

USAGE = 'Usage: python -m pipeline.load [--flush-logs | setup [--dry-run] | test]'
PENDING_SUFFIX = '.load'
# An unverified entry this recent may belong to a publication still running.
UNVERIFIED_GRACE_SECONDS = 600
LOG_COLUMNS = (
    'load_run_id', 'transformation_run_id', 'extraction_run_id', 'retry_of_run_id',
    'batch_id', 'batch_window', 'published_date_range', 'reporting_cutoff',
    'extraction_started_at', 'table_name', 'load_type', 'load_started_at', 'load_ended_at',
    'duration_ms', 'input_acceptance_status', 'quarantined_input_records', 'records_received',
    'records_inserted', 'records_updated', 'records_unchanged', 'records_deleted',
    'records_rejected', 'measure_totals', 'revisions', 'pre_load_results', 'post_load_results',
    'load_status', 'acceptance_status', 'log_delivery_status', 'error_code', 'error_message',
    'load_code_version', 'schema_version',
)
JSON_LOG_COLUMNS = frozenset({'measure_totals', 'revisions', 'pre_load_results', 'post_load_results'})
LOG_DELIVERY_ERRORS = (psycopg.Error, SourceConnectionError, OSError, ValueError, KeyError,
                       subprocess.SubprocessError)


def _uuid_text(value):
    try:
        return str(UUID(str(value))) if value not in (None, '') else None
    except ValueError:
        return None


def _timestamp_text(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return None
    return None


def _text(value):
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, default=str, sort_keys=True)


def build_load_log(consumer, result=None, *, error_code=None, error_message=None,
                   provisional=False) -> dict:
    """One etl_ops.load_logs entry (Load.md Section 6) with its rejections.

    Without a result, the batch was never published: a rejected receipt or a
    snapshot that did not commit. A provisional entry is queued just before
    COMMIT and is finalized once the commit result is known.
    """
    batch = consumer.batch
    meta = consumer.handover_meta or {}
    now = datetime.now(timezone.utc)
    started = result.load_started_at if result else (consumer.receipt_started_at or now)
    ended = result.load_ended_at if result and result.load_ended_at else now
    received = batch.records_received if batch else None
    if provisional:
        counts = (result.records_inserted, result.records_updated, result.records_unchanged,
                  result.records_deleted, 0)
        acceptance = 'APPROVED'
        status = overall_load_status(result.pre_load_results + result.post_load_results, acceptance)
    elif result is not None:
        counts = (result.records_inserted, result.records_updated, result.records_unchanged,
                  result.records_deleted, result.records_rejected)
        acceptance, status = result.acceptance_status, result.load_status
        error_code, error_message = result.error_code, result.error_message
    else:
        # Nothing was written; unavailable counts stay NULL rather than zero.
        counts = (0, 0, 0, 0, received) if received is not None else (None,) * 5
        acceptance, status = 'REJECTED', 'FAIL'
    duration = (result.duration_ms if result and result.duration_ms is not None
                else max(0, int((ended - started).total_seconds() * 1000)))
    entry = {
        'load_run_id': result.load_run_id if result else consumer.load_run_id,
        'transformation_run_id': _uuid_text(batch.transformation_run_id if batch else meta.get('run_id')),
        'extraction_run_id': _uuid_text(batch.extraction_run_id if batch else meta.get('parent_run_id')),
        'retry_of_run_id': result.retry_of_run_id if result else None,
        'batch_id': _text(batch.batch_id if batch else meta.get('batch_id')),
        'batch_window': _text(batch.batch_window if batch else meta.get('batch_window')),
        'published_date_range': result.published_date_range if result else None,
        'reporting_cutoff': batch.reporting_cutoff if batch else None,
        'extraction_started_at': _timestamp_text(
            batch.extraction_started_at if batch else meta.get('extraction_started_at')),
        'table_name': TARGET_TABLE,
        'load_type': 'FULL',
        'load_started_at': started,
        'load_ended_at': ended,
        'duration_ms': duration,
        'input_acceptance_status': (batch.input_acceptance_status if batch
                                    else _text(meta.get('acceptance_status'))),
        'quarantined_input_records': batch.quarantined_input_records if batch else None,
        'records_received': received,
        'records_inserted': counts[0],
        'records_updated': counts[1],
        'records_unchanged': counts[2],
        'records_deleted': counts[3],
        'records_rejected': counts[4],
        'measure_totals': batch.measure_totals if batch and batch.measure_totals else None,
        'revisions': result.revisions if result else [],
        'pre_load_results': list(consumer.receipt_checks) + (result.pre_load_results if result else []),
        'post_load_results': result.post_load_results if result else [],
        'load_status': status,
        'acceptance_status': acceptance,
        'log_delivery_status': 'PENDING',
        'error_code': error_code,
        'error_message': error_message,
        'load_code_version': LOAD_CODE_VERSION,
        'schema_version': TARGET_SCHEMA_VERSION,
        'rejections': result.rejections if result else [],
        'outcome_verified': not provisional and not (
            result is not None and result.error_code == 'COMMIT_OUTCOME_UNKNOWN'
            and result.commit_verified is None),
    }
    # Dates, timestamps, UUIDs, and exact decimals become text for the queue.
    return json.loads(json.dumps(entry, default=str))


def queue_load_log(entry):
    """Durably write one entry to the private pending directory."""
    directory = extraction.private_pending_directory()
    target = directory / (entry['load_run_id'] + PENDING_SUFFIX)
    descriptor, temporary = tempfile.mkstemp(dir=directory, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(entry, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)


def resolve_unverified(cursor, entry):
    """Finalize an entry whose commit result was never recorded (Load.md Section 9)."""
    cursor.execute('SELECT 1 FROM etl_ops.load_publications WHERE load_run_id = %s',
                   (entry['load_run_id'],))
    if cursor.fetchone():
        entry.update(acceptance_status='APPROVED', error_code=None, error_message=None,
                     records_rejected=0)
        entry['load_status'] = overall_load_status(
            entry['pre_load_results'] + entry['post_load_results'], 'APPROVED')
    else:
        entry.update(
            acceptance_status='REJECTED', load_status='FAIL', error_code='COMMIT_OUTCOME_UNKNOWN',
            error_message='The run ended before its commit result was recorded; '
                          'no publication record exists.',
            records_inserted=0, records_updated=0, records_unchanged=0, records_deleted=0,
            records_rejected=entry.get('records_received'), revisions=[])
    entry['outcome_verified'] = True


def deliver_load_log(cursor, entry):
    """Insert one entry and its rejections; a replay never duplicates rows."""
    values = []
    for column in LOG_COLUMNS:
        value = 'SAVED' if column == 'log_delivery_status' else entry.get(column)
        values.append(Jsonb(value) if column in JSON_LOG_COLUMNS and value is not None else value)
    cursor.execute(sql.SQL(
        'INSERT INTO etl_ops.load_logs ({}) VALUES ({}) ON CONFLICT (load_run_id) DO NOTHING'
    ).format(_columns(LOG_COLUMNS), sql.SQL(', ').join([sql.Placeholder()] * len(LOG_COLUMNS))),
        values)
    for number, item in enumerate(entry.get('rejections') or []):
        offending = item.get('offending_values')
        cursor.execute('''
            INSERT INTO etl_ops.load_rejections (
                load_run_id, item_number, report_date, branch_id, check_name, reason,
                offending_values, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (load_run_id, item_number) DO NOTHING
        ''', (entry['load_run_id'], number, item.get('report_date'),
              _uuid_text(item.get('branch_id')), item['check_name'], item['reason'],
              Jsonb(offending) if offending is not None else None, entry['load_ended_at']))


def flush_load_logs() -> int:
    """Deliver queued load metadata; remove an entry only after its commit."""
    if not extraction.PENDING_LOG_DIRECTORY.exists():
        return 0
    directory = extraction.private_pending_directory()
    saved = 0
    for path in sorted(directory.glob('*' + PENDING_SUFFIX)):
        entry = json.loads(path.read_text(encoding='utf-8'))
        if not entry.get('outcome_verified', True):
            started = datetime.fromisoformat(entry['load_started_at'])
            if (datetime.now(timezone.utc) - started).total_seconds() < UNVERIFIED_GRACE_SECONDS:
                continue
        with connect_source() as connection:
            connection.read_only = False
            with connection.transaction():
                with connection.cursor() as cursor:
                    if not entry.get('outcome_verified', True):
                        resolve_unverified(cursor, entry)
                    deliver_load_log(cursor, entry)
        path.unlink(missing_ok=True)
        saved += 1
    return saved


def save_load_logs(entries) -> int:
    """Queue every entry first, then deliver the queue."""
    for entry in entries:
        queue_load_log(entry)
    return flush_load_logs()


# ---------------------------------------------------------------------------
# Manual runs: extraction, transformation, and load on one connection
# ---------------------------------------------------------------------------

def run_pipeline(flush_only=False) -> int:
    """`python -m pipeline.load` (Load.md Section 1, manual runs)."""
    try:
        recovered = flush_load_logs()
    except LOG_DELIVERY_ERRORS:
        print('Pending load logs could not be delivered. Entries were kept; the run did not start.')
        return 1
    if recovered:
        print(f'Recovered {recovered} pending load log entries.')
    if flush_only:
        print('Pending load-log delivery completed.')
        return 0
    try:
        transformation.flush_logs()
    except LOG_DELIVERY_ERRORS:
        print('Pending transformation logs could not be delivered. '
              'Entries were kept; the run did not start.')
        return 1

    loader = LoadConsumer()
    stage2 = transformation.TransformationConsumer(loader=loader)
    code = extraction.main(stage2)
    if stage2.result is not None:
        try:
            transformation.save_logs(stage2.result)
        except LOG_DELIVERY_ERRORS:
            print('Transformation logs are queued but not delivered yet.')

    if loader.load_run_id is not None and loader.results is None:
        # Received but never published: a rejected receipt, or the snapshot did not commit.
        if loader.failure is not None:
            entry = build_load_log(loader, error_code=loader.failure.code,
                                   error_message=str(loader.failure))
        else:
            entry = build_load_log(
                loader, error_code='SNAPSHOT_NOT_COMMITTED',
                error_message='The extraction snapshot did not commit; the received batch was discarded.')
        loader.log_entries = [entry]
        try:
            save_load_logs(loader.log_entries)
        except LOG_DELIVERY_ERRORS:
            loader.log_error = ('Load metadata was not fully delivered; it stays queued. '
                                'Run python -m pipeline.load --flush-logs.')
    return report_load(loader, stage2, code)


def report_load(loader, stage2, code) -> int:
    """Print the completion report (Load.md Section 8) and return the exit code."""
    if not loader.log_entries:
        print('Load did not run: no approved transformation output was offered.')
        if code:
            return code
        return 2 if (stage2.result or {}).get('status') == 'WARNING' else 1
    final = loader.log_entries[-1]
    print(f"Load decision: {final['acceptance_status']} (load_status {final['load_status']})")
    if final['error_code']:
        print(f"Reason: {final['error_code']}: {final['error_message']}")
    if len(loader.log_entries) > 1:
        print(f'Publication attempts: {len(loader.log_entries)}')
    print(f"Load run {final['load_run_id']}; transformation run {final['transformation_run_id']}; "
          f"extraction run {final['extraction_run_id']}")
    print(f"Batch window {final['batch_window']}; cutoff {final['reporting_cutoff']}; "
          f"published dates {final['published_date_range']}")
    print('Records: received {records_received}, inserted {records_inserted}, updated '
          '{records_updated}, unchanged {records_unchanged}, removed {records_deleted}, '
          'rejected {records_rejected}; quarantined input {quarantined_input_records}'.format(**final))
    if final['acceptance_status'] == 'APPROVED' and loader.results:
        print(f'Published to {TARGET_TABLE}; refreshed_at {loader.results[-1].refreshed_at}.')
    elif final['acceptance_status'] == 'MANUAL REVIEW':
        print('Publication held for review; the previous publication is unchanged. '
              'Held keys are in etl_ops.load_rejections.')
    else:
        print('Nothing was published; the previous publication is unchanged.')
    print(f"Duration {final['duration_ms']} ms. "
          + (loader.log_error or 'Load details saved in etl_ops.load_logs.'))
    return {'APPROVED': 0, 'MANUAL REVIEW': 2}.get(final['acceptance_status'], 1)


# ---------------------------------------------------------------------------
# Tests: `python -m pipeline.load test`, inside one rolled-back transaction
# ---------------------------------------------------------------------------

class _RolledBackConnection:
    """Nested transactions become savepoints of one outer transaction, and
    session mode changes are ignored, so publication never commits."""

    def __init__(self, connection):
        object.__setattr__(self, '_connection', connection)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        if name not in ('read_only', 'isolation_level'):
            setattr(self._connection, name, value)


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _load_scenarios(connection) -> int:
    cursor = connection.cursor()
    problems = verify_load_objects(cursor)
    _require(not problems, 'run python -m pipeline.load setup first: ' + '; '.join(problems))
    cursor.execute('SELECT id FROM public.branches ORDER BY id LIMIT 1')
    row = cursor.fetchone()
    _require(row is not None, 'public.branches has no branch to test with')
    branch = row[0]
    # Start from an empty target inside this transaction; real rows return on rollback.
    cursor.execute(f'DELETE FROM {TARGET_TABLE}')
    cursor.execute("SELECT coalesce(max(extraction_started_at), now()) + interval '1 day' "
                   'FROM etl_ops.load_publications')
    base = cursor.fetchone()[0]
    cursor.execute("SELECT (%s::timestamptz AT TIME ZONE 'Asia/Manila')::date", (base,))
    first_cutoff = cursor.fetchone()[0]
    day1, day2 = first_cutoff - timedelta(days=3), first_cutoff - timedelta(days=2)
    window_start = day1 - timedelta(days=1)
    definitions = sql.SQL(', ').join(
        sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
        for column, kind in CONTRACT_COLUMNS.items())
    connection.execute(sql.SQL('CREATE TEMP TABLE transformed_daily_branch_summary ({})')
                       .format(definitions))
    LoadConsumer.prepare(connection)
    target = _RolledBackConnection(connection)

    row_a = (day1, branch, 2, 1, 2, Decimal('16.00'), Decimal('400.00'), 2,
             Decimal('300.00'), Decimal('50.00'))
    row_a2 = row_a[:6] + (Decimal('450.00'),) + row_a[7:]
    row_b = (day2, branch, 1, 0, 1, Decimal('8.00'), Decimal('200.00'), 1,
             Decimal('200.00'), Decimal('0.00'))
    unknown_branch = (day2, uuid4()) + row_b[2:]
    state = {'minute': 0, 'passed': 0}

    def target_rows():
        cursor.execute(f'SELECT count(*) FROM {TARGET_TABLE}')
        return cursor.fetchone()[0]

    def case(label, rows, expected, code=None, *, minute=None, cutoff_shift=0, **handover):
        state['minute'] += 1
        started = base + timedelta(minutes=state['minute'] if minute is None else minute)
        cursor.execute(f'DELETE FROM {TRANSFORMED_TABLE}')
        cursor.executemany(sql.SQL('INSERT INTO {} VALUES ({})').format(
            sql.SQL(TRANSFORMED_TABLE), sql.SQL(', ').join([sql.Placeholder()] * 10)), rows)
        cursor.execute("SELECT (%s::timestamptz AT TIME ZONE 'Asia/Manila')::date", (started,))
        cutoff = cursor.fetchone()[0]
        offered = {
            'run_id': str(uuid4()), 'parent_run_id': str(uuid4()), 'acceptance_status': 'APPROVED',
            'extraction_started_at': started, 'batch_id': 'load-test',
            'batch_window': f'{window_start} to {cutoff + timedelta(days=cutoff_shift)}',
            'records_output': len(rows), 'validation_results': {'status': 'PASS'},
            'code_version': '1.0.0', 'rule_version': '1.0.0', 'schema_version': '1.0.0',
        }
        offered.update(handover)
        consumer, results = LoadConsumer(), []
        try:
            consumer.acknowledge(offered)
            consumer.consume(connection, offered)
        except LoadRejected as rejected:
            outcome, got = 'REJECTED', rejected.code
        else:
            results = publish(target, consumer)
            outcome, got = results[-1].acceptance_status, results[-1].error_code
        _require((outcome, got) == (expected, code),
                 f'{label}: expected {expected} {code or ""}, got {outcome} {got or ""}')
        state['passed'] += 1
        print(f'PASS: {label} -> {outcome}' + (f' ({got})' if got else ''), flush=True)
        return consumer, offered, results

    first_consumer, _, results = case('First publication', [row_a, row_b], 'APPROVED')
    first = results[-1]
    _require((first.records_inserted, first.records_rejected) == (2, 0) and target_rows() == 2,
             'First publication: expected 2 inserted rows')
    _, _, results = case('Rerun without changes', [row_a, row_b], 'APPROVED')
    _require((results[-1].records_inserted, results[-1].records_unchanged) == (0, 2)
             and target_rows() == 2, 'Rerun: expected 2 unchanged rows and no duplicates')
    _, changed_offer, results = case('Changed measure', [row_a2, row_b], 'APPROVED')
    changed = results[-1]
    _require((changed.records_updated, changed.records_unchanged, len(changed.revisions)) == (1, 1, 1),
             'Changed measure: expected 1 updated row with its revision')
    case('Duplicate transformation run', [row_a2, row_b], 'REJECTED', 'DUPLICATE_BATCH',
         run_id=changed_offer['run_id'])
    case('Older snapshot', [row_a2, row_b], 'REJECTED', 'STALE_BATCH', minute=0)
    _, _, results = case('Published key missing, not approved', [row_a2], 'MANUAL REVIEW',
                         'HISTORY_REMOVAL_HELD')
    _require(target_rows() == 2 and results[-1].rejections,
             'History removal: expected the held key recorded and the target unchanged')
    cursor.execute('INSERT INTO etl_ops.load_review_approvals (report_date, branch_id, reason) '
                   'VALUES (%s, %s, %s)', (day2, branch, 'Load test approval'))
    _, _, results = case('Published key missing, approved', [row_a2], 'APPROVED')
    cursor.execute('SELECT count(*) FROM etl_ops.load_review_approvals WHERE applied_load_run_id = %s',
                   (results[-1].load_run_id,))
    _require(results[-1].records_deleted == 1 and target_rows() == 1 and cursor.fetchone()[0] >= 1,
             'Approved removal: expected 1 removed row and the approval marked as applied')
    case('Unknown branch', [row_a2, unknown_branch], 'REJECTED', 'FOREIGN_KEY_VIOLATION')
    _require(target_rows() == 1, 'Unknown branch: expected the target unchanged')
    case('Handover not approved', [row_a2], 'REJECTED', 'HANDOVER_NOT_APPROVED',
         acceptance_status='MANUAL REVIEW')
    case('Incomplete handover', [row_a2], 'REJECTED', 'HANDOVER_INCOMPLETE', batch_id=None)
    case('Row count differs from handover', [row_a2], 'REJECTED', 'INPUT_COUNT_MISMATCH',
         records_output=5)
    case('Batch window cutoff mismatch', [row_a2], 'REJECTED', 'HANDOVER_INCOMPLETE', cutoff_shift=1)
    with connect_source() as holder:
        with holder.transaction():
            holder.execute('SELECT pg_advisory_xact_lock(%s, %s)', LOAD_LOCK_KEY)
            _, _, attempts = case('Publication lock busy', [row_a2], 'REJECTED', 'LOAD_LOCK_BUSY')
    _require(len(attempts) == MAX_PUBLICATION_ATTEMPTS
             and attempts[1].retry_of_run_id == attempts[0].load_run_id,
             'Lock busy: expected 3 attempts linked by retry_of_run_id')

    deliver_load_log(cursor, build_load_log(first_consumer, first))
    cursor.execute('SELECT acceptance_status, log_delivery_status FROM etl_ops.load_logs '
                   'WHERE load_run_id = %s', (first.load_run_id,))
    _require(cursor.fetchone() == ('APPROVED', 'SAVED'),
             'Log delivery: expected the entry in etl_ops.load_logs')
    state['passed'] += 1
    print('PASS: Load log delivery -> SAVED', flush=True)
    return state['passed']


def run_load_tests() -> int:
    """Run every load scenario in one transaction that is rolled back at the end."""
    passed = 0
    try:
        with connect_source() as connection:
            connection.read_only = False
            with connection.transaction():
                passed = _load_scenarios(connection)
                raise psycopg.Rollback()
    except (AssertionError, LoadSetupError) as error:
        print(f'Load test failed: {error}')
        return 1
    except SourceConnectionError as error:
        print(error)
        return 1
    except psycopg.Error as error:
        print(f'Load test failed (SQLSTATE: {error.sqlstate or "unavailable"}): '
              f'{error.diag.message_primary}')
        return 1
    print(f'All {passed} load tests passed. Everything was rolled back: '
          'nothing was published or logged.')
    return 0


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args[:1] == ['setup']:
        return run_setup(args)
    if args == ['test']:
        return run_load_tests()
    if args not in ([], ['--flush-logs']):
        print(USAGE)
        return 1
    return run_pipeline(flush_only=args == ['--flush-logs'])


def run_setup(args) -> int:
    if args not in (['setup'], ['setup', '--dry-run']):
        print(USAGE)
        return 1
    dry_run = '--dry-run' in args
    try:
        setup_load_objects(dry_run=dry_run)
    except LoadSetupError as error:
        print(error)
        return 1
    except SourceConnectionError as error:
        print(error)
        return 1
    except psycopg.Error as error:
        print(f'Load setup failed (SQLSTATE: {error.sqlstate or "unavailable"}).')
        if error.diag.message_primary:
            print(error.diag.message_primary)
        print('Nothing was changed. Check permissions and the public.branches table.')
        return 1
    if dry_run:
        print('Load setup is valid. Dry run only: everything was rolled back, nothing was changed.')
    else:
        print(f'Load objects are ready: {TARGET_TABLE} and the etl_ops load tables.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
