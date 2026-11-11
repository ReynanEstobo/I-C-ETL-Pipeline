"""Stage 3 load: publish the approved daily branch summary (docs/Load/Load.md).

This module provides the one-time database setup for the load:

    python -m pipeline.load setup             create the load objects
    python -m pipeline.load setup --dry-run   validate them, then roll back

Setup creates the permanent target `analytics.daily_branch_summary` and the
restricted `etl_ops` tables the load writes: publication records, load logs,
pending load logs, rejections, and review approvals. The daily load never runs
DDL; an administrator runs this setup once and again only after a versioned
schema change.
"""

import sys

import psycopg

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


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args not in (['setup'], ['setup', '--dry-run']):
        print('Usage: python -m pipeline.load setup [--dry-run]')
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
