"""Read the five source datasets specified in extraction.md (issue #2)."""

from contextlib import contextmanager
from datetime import datetime, timezone
from time import perf_counter, sleep
from uuid import uuid4
import sys
import csv
import json
import os
from pathlib import Path
import subprocess
import tempfile

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.connection import (
    SourceConnectionError, connect_source, is_temporary_database_error,
)


# Shared lock identity for every manual and future scheduled extraction.
# Both values fit PostgreSQL's two-integer advisory-lock signature.
EXTRACTION_LOCK_KEY = (18755, 1163152460)
PENDING_LOG_DIRECTORY = Path(__file__).resolve().parents[1] / '.pending-extraction-logs'


# PostgreSQL's internal type names, in the output column order.
SOURCE_COLUMNS = {
    "branches": {
        "id": "uuid", "branch_name": "text", "name": "text", "code": "text",
    },
    "customers": {"id": "uuid"},
    "orders": {
        "id": "uuid", "order_number": "text", "branch_id": "uuid",
        "customer_id": "uuid", "created_at": "timestamptz", "status": "text",
        "weight_kg": "numeric", "total_price": "numeric",
    },
    "payments": {
        "id": "uuid", "order_id": "uuid", "branch_id": "uuid",
        "amount": "numeric", "paid_at": "timestamptz", "source": "text",
    },
    "expenses": {
        "id": "uuid", "branch_id": "uuid", "amount": "numeric",
        "expense_date": "date", "deleted_at": "timestamptz",
    },
}


class ExtractionError(RuntimeError):
    """Extraction could not produce all five datasets."""

    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


class AcceptanceBlocked(ExtractionError):
    """Stop the staging transaction when extraction is not approved."""


class ExtractionAlreadyRunning(ExtractionError):
    """Another extraction owns the transaction-scoped advisory lock."""


def record_run_check(checks, name, status):
    """Store one outcome per run-level check without recording credentials."""
    checks[:] = [check for check in checks if check['check'] != name]
    checks.append({'table': '*', 'check': name, 'status': status,
                   'affected_rows': 1 if status == 'FAIL' else 0})


def check_source_schema(connection: psycopg.Connection) -> None:
    """Check required columns/types and SELECT access without reading rows."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name, column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            """,
            (list(SOURCE_COLUMNS),),
        )
        actual = {(table, column): kind for table, column, kind in cursor.fetchall()}
        problems = []
        for table, columns in SOURCE_COLUMNS.items():
            for column, expected in columns.items():
                found = actual.get((table, column))
                if found is None:
                    problems.append(f"public.{table}.{column} is missing or inaccessible")
                elif found != expected:
                    problems.append(
                        f"public.{table}.{column} requires {expected}, found {found}"
                    )
        if problems:
            raise ExtractionError("Source schema check failed: " + "; ".join(problems))

        for table, columns in SOURCE_COLUMNS.items():
            try:
                cursor.execute(
                    sql.SQL("SELECT {} FROM {} LIMIT 0").format(
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.Identifier("public", table),
                    )
                )
            except psycopg.Error as error:
                raise ExtractionError(
                    f"Cannot read the required columns from public.{table}. "
                    "Check source permissions and schema.",
                    retryable=is_temporary_database_error(error),
                ) from None


@contextmanager
def extract_source(source_counts=None, staged_counts=None, run_checks=None):
    """Stage source rows in SQL and yield the connection and row counts.

    Stages remain available inside this context only. Future SQL validation
    must use the yielded connection before leaving this transaction.
    """
    run_checks = run_checks if run_checks is not None else []
    phase = 'source_accessibility'
    try:
        with connect_source() as connection:
            record_run_check(run_checks, phase, 'PASS')
            phase = 'completion'
            # PostgreSQL forbids CREATE TABLE in read-only transactions.
            # Create empty temporary structures before taking the source snapshot.
            connection.read_only = False
            with connection.transaction():
                with connection.cursor() as cursor:
                    for table, columns in SOURCE_COLUMNS.items():
                        definitions = sql.SQL(', ').join(
                            sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
                            for column, kind in columns.items()
                        )
                        cursor.execute(
                            sql.SQL('CREATE TEMP TABLE {} ({}) ON COMMIT DELETE ROWS').format(
                                sql.Identifier('stage_' + table), definitions
                            )
                        )

            connection.read_only = True
            connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            with connection.transaction():
                phase = 'overlap_lock'
                with connection.cursor() as cursor:
                    cursor.execute(
                        'SELECT pg_try_advisory_xact_lock(%s, %s)',
                        EXTRACTION_LOCK_KEY,
                    )
                    if cursor.fetchone() != (True,):
                        raise ExtractionAlreadyRunning(
                            'Another extraction is running. This run stopped before reading source records.'
                        )
                record_run_check(run_checks, phase, 'PASS')
                phase = 'snapshot_consistency'
                with connection.cursor() as cursor:
                    cursor.execute('SHOW transaction_read_only')
                    if cursor.fetchone() != ('on',):
                        raise ExtractionError('Extraction requires a read-only transaction.')
                    cursor.execute('SHOW transaction_isolation')
                    if cursor.fetchone() != ('repeatable read',):
                        raise ExtractionError('Extraction requires a consistent snapshot.')

                record_run_check(run_checks, phase, 'PASS')
                phase = 'schema_compatibility'
                check_source_schema(connection)
                record_run_check(run_checks, phase, 'PASS')
                phase = 'completion'
                counts = {}
                for table, columns in SOURCE_COLUMNS.items():
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(sql.SQL('SELECT count(*) FROM {}').format(
                                sql.Identifier('public', table)
                            ))
                            if source_counts is not None:
                                source_counts[table] = cursor.fetchone()[0]
                            else:
                                cursor.fetchone()
                            selected = sql.SQL(', ').join(map(sql.Identifier, columns))
                            cursor.execute(
                                sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}').format(
                                    sql.Identifier('pg_temp', 'stage_' + table),
                                    selected, selected, sql.Identifier('public', table),
                                )
                            )
                            cursor.execute(sql.SQL('SELECT count(*) FROM {}').format(
                                sql.Identifier('pg_temp', 'stage_' + table)
                            ))
                            counts[table] = cursor.fetchone()[0]
                            if staged_counts is not None:
                                staged_counts[table] = counts[table]
                    except psycopg.Error as error:
                        raise ExtractionError(
                            f'Could not stage public.{table}. '
                            'Extraction was rolled back; source records were unchanged.',
                            retryable=is_temporary_database_error(error),
                        ) from None
                yield connection, counts
    except AcceptanceBlocked:
        raise
    except (SourceConnectionError, ExtractionError):
        record_run_check(run_checks, phase, 'FAIL')
        raise
    except psycopg.Error as error:
        record_run_check(run_checks, phase, 'FAIL')
        raise ExtractionError(
            'Extraction failed. Check database access, temporary-table permissions, '
            'and network connectivity. Source records were unchanged.',
            retryable=is_temporary_database_error(error),
        ) from None


def validation_queries(source_schema='public', source_prefix=''):
    """Shared SQL checks for both Python execution and database scheduling."""
    queries = []

    def record(table, name, query, warning=False):
        queries.append((table, name, query, warning))

    for table in SOURCE_COLUMNS:
        stage = sql.Identifier('pg_temp', 'stage_' + table)
        record(table, 'row_count', sql.SQL(
            'SELECT abs((SELECT count(*) FROM {}) - (SELECT count(*) FROM {}))'
        ).format(sql.Identifier(source_schema, source_prefix + table), stage))
        columns = ['id', 'order_number'] if table == 'orders' else ['id']
        for column in columns:
            field = sql.Identifier(column)
            record(table, column + '_present', sql.SQL(
                'SELECT count(*) FROM {} WHERE {} IS NULL'
            ).format(stage, field))
            record(table, column + '_unique', sql.SQL(
                'SELECT count({}) - count(DISTINCT {}) FROM {}'
            ).format(field, field, stage))

    for table, column, parent, nullable in [
        ('orders', 'branch_id', 'branches', True),
        ('orders', 'customer_id', 'customers', True),
        ('payments', 'branch_id', 'branches', True),
        ('payments', 'order_id', 'orders', False),
        ('expenses', 'branch_id', 'branches', True),
    ]:
        stage = sql.Identifier('pg_temp', 'stage_' + table)
        field = sql.Identifier(column)
        record(table, column + '_present', sql.SQL(
            'SELECT count(*) FROM {} WHERE {} IS NULL'
        ).format(stage, field), warning=nullable)
        record(table, column + '_reference', sql.SQL(
            'SELECT count(*) FROM {} AS child '
            'WHERE child.{} IS NOT NULL AND NOT EXISTS '
            '(SELECT 1 FROM {} AS parent WHERE parent.id = child.{})'
        ).format(stage, field, sql.Identifier('pg_temp', 'stage_' + parent), field))

    for table, column, nullable in [
        ('orders', 'created_at', True),
        ('payments', 'paid_at', False),
        ('expenses', 'expense_date', False),
    ]:
        stage = sql.Identifier('pg_temp', 'stage_' + table)
        field = sql.Identifier(column)
        record(table, column + '_present', sql.SQL(
            'SELECT count(*) FROM {} WHERE {} IS NULL'
        ).format(stage, field), warning=nullable)
        # PostgreSQL types already reject malformed dates. Infinity is
        # valid PostgreSQL syntax but cannot identify a reporting date.
        record(table, column + '_valid', sql.SQL(
            'SELECT count(*) FROM {} WHERE {} IS NOT NULL AND NOT isfinite({})'
        ).format(stage, field, field))

    return queries


def validate_staged_data(
    connection: psycopg.Connection,
    source_schema: str = 'public',
    source_prefix: str = '',
) -> dict:
    """Run shared SQL checks inside the extraction snapshot."""
    checks = []
    with connection.cursor() as cursor:
        for table, name, query, warning in validation_queries(source_schema, source_prefix):
            cursor.execute(query)
            affected = cursor.fetchone()[0]
            status = ('WARNING' if warning else 'FAIL') if affected else 'PASS'
            checks.append({'table': table, 'check': name, 'status': status,
                           'affected_rows': affected})

    def summarize(statuses):
        return 'FAIL' if 'FAIL' in statuses else 'WARNING' if 'WARNING' in statuses else 'PASS'

    tables = {
        table: summarize({check['status'] for check in checks if check['table'] == table})
        for table in SOURCE_COLUMNS
    }
    overall = summarize(set(tables.values()))
    return {'status': overall, 'tables': tables, 'checks': checks}


def setup_extraction_logs():
    """One-time administrator setup; no access is granted to browser roles."""
    with connect_source() as connection:
        connection.read_only = False
        with connection.cursor() as cursor:
            cursor.execute('''
                CREATE SCHEMA IF NOT EXISTS etl_ops;
                REVOKE ALL ON SCHEMA etl_ops FROM PUBLIC;
                CREATE TABLE IF NOT EXISTS etl_ops.extraction_logs (
                    pipeline_run_id UUID NOT NULL,
                    table_name TEXT NOT NULL,
                    extraction_started_at TIMESTAMPTZ NOT NULL,
                    extraction_ended_at TIMESTAMPTZ NOT NULL,
                    extraction_status TEXT NOT NULL
                        CHECK (extraction_status IN ('PASS', 'WARNING', 'FAIL')),
                    records_read BIGINT CHECK (records_read >= 0),
                    records_extracted BIGINT CHECK (records_extracted >= 0),
                    records_rejected BIGINT NOT NULL DEFAULT 0,
                    extraction_window TEXT NOT NULL,
                    validation_results JSONB NOT NULL,
                    error_code TEXT,
                    error_message TEXT,
                    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
                    acceptance_status TEXT,
                    PRIMARY KEY (pipeline_run_id, table_name)
                );
                REVOKE ALL ON etl_ops.extraction_logs FROM PUBLIC;
                ALTER TABLE etl_ops.extraction_logs ENABLE ROW LEVEL SECURITY;
                CREATE OR REPLACE FUNCTION etl_ops.protect_extraction_logs()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF TG_OP <> 'DELETE' THEN
                        RAISE EXCEPTION 'Extraction logs cannot be updated or truncated';
                    END IF;
                    IF OLD.extraction_ended_at > now() - interval '1 year' THEN
                        RAISE EXCEPTION 'Extraction logs must be retained for at least one year';
                    END IF;
                    RETURN OLD;
                END;
                $$;
                REVOKE ALL ON FUNCTION etl_ops.protect_extraction_logs() FROM PUBLIC;
                DROP TRIGGER IF EXISTS protect_extraction_logs ON etl_ops.extraction_logs;
                CREATE TRIGGER protect_extraction_logs
                    BEFORE UPDATE OR DELETE ON etl_ops.extraction_logs
                    FOR EACH ROW EXECUTE FUNCTION etl_ops.protect_extraction_logs();
                DROP TRIGGER IF EXISTS prevent_log_truncate ON etl_ops.extraction_logs;
                CREATE TRIGGER prevent_log_truncate
                    BEFORE TRUNCATE ON etl_ops.extraction_logs
                    FOR EACH STATEMENT EXECUTE FUNCTION etl_ops.protect_extraction_logs();
            ''')


def acceptance_decision(results, failed=False):
    """Keep validation outcomes separate from acceptance decisions."""
    if failed:
        return 'REJECTED'
    return {
        'PASS': 'APPROVED', 'WARNING': 'MANUAL REVIEW', 'FAIL': 'REJECTED',
    }.get(results.get('status'), 'REJECTED')


def handover_extraction(connection, run_id, started, counts, results, consumer,
                        *, source_counts, ended, duration):
    """Offer approved SQL stages to a consumer before the transaction ends.

    consumer.acknowledge(metadata) must confirm the run before
    consumer.consume(connection, payload) receives the staging connection.
    The consumer must not commit or close the connection.
    No consumer is invoked for held or rejected runs.
    """
    decision = acceptance_decision(results)
    if decision != 'APPROVED':
        raise AcceptanceBlocked(f'Handover blocked: {decision}.')
    if connection.closed or connection.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
        raise ExtractionError('Handover requires the active extraction transaction.')
    payload = {
        'pipeline_run_id': run_id,
        'extraction_started_at': started,
        'extraction_ended_at': ended,
        'duration_ms': duration,
        'extraction_status': results['status'],
        'extraction_window': 'All available history',
        'acceptance_status': decision,
        'validation_results': results,
        'records_read': dict(source_counts),
        'records_extracted': dict(counts),
        'records_rejected': {table: 0 for table in SOURCE_COLUMNS},
        'stages': {table: 'pg_temp.stage_' + table for table in SOURCE_COLUMNS},
        'error_code': None,
        'error_message': None,
    }
    try:
        acknowledgment = consumer.acknowledge({
            'pipeline_run_id': run_id, 'acceptance_status': decision,
        })
    except Exception:
        # Do not leak consumer errors that might contain connection secrets.
        raise ExtractionError('The next stage could not acknowledge extraction.') from None
    if connection.closed or connection.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
        raise ExtractionError('The handover consumer ended the extraction transaction.')
    if not isinstance(acknowledgment, dict) or (
        acknowledgment.get('pipeline_run_id') != run_id
        or acknowledgment.get('acceptance_status') != decision
    ):
        raise ExtractionError('The next stage did not acknowledge the approved run.')
    try:
        consumer.consume(connection, payload)
    except Exception:
        raise ExtractionError('The next stage could not consume approved staging.') from None
    if connection.closed or connection.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
        raise ExtractionError('The handover consumer ended the extraction transaction.')
    return acknowledgment


def write_database_logs(run_id, started, ended, duration, source_counts,
                         staged_counts, results, error_code=None, error_message=None):
    """Commit only metadata on a separate connection after extraction ends."""
    with connect_source() as connection:
        connection.read_only = False
        with connection.cursor() as cursor:
            for table in SOURCE_COLUMNS:
                status = (results['tables'][table] if results.get('tables')
                          else 'FAIL')
                details = {
                    'overall_status': 'FAIL' if error_code else results['status'],
                    'checks': [check for check in results.get('checks', [])
                               if check['table'] in (table, '*')],
                }
                cursor.execute('''
                    INSERT INTO etl_ops.extraction_logs (
                        pipeline_run_id, table_name, extraction_started_at,
                        extraction_ended_at, extraction_status, records_read,
                        records_extracted, records_rejected, extraction_window,
                        validation_results, error_code, error_message, duration_ms,
                        acceptance_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_run_id, table_name) DO NOTHING
                ''', (run_id, 'public.' + table, started, ended, status,
                      source_counts.get(table), staged_counts.get(table),
                      'All available history', Jsonb(details), error_code,
                      error_message, duration,
                      acceptance_decision(results, failed=bool(error_code))))


def private_pending_directory():
    """Restrict pending metadata to the operating-system user running Python."""
    directory = PENDING_LOG_DIRECTORY
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.name == 'nt':
        identity = subprocess.run(
            ['whoami', '/user', '/fo', 'csv', '/nh'],
            capture_output=True, text=True, check=True,
        )
        sid = next(csv.reader([identity.stdout.strip()]))[1]
        subprocess.run(
            ['icacls', str(directory), '/inheritance:r', '/grant:r',
             f'*{sid}:(OI)(CI)F'],
            capture_output=True, check=True,
        )
    else:
        directory.chmod(0o700)
    return directory


def flush_pending_logs():
    """Replay durable metadata; only delete entries after the DB commit returns."""
    if not PENDING_LOG_DIRECTORY.exists():
        return 0
    directory = private_pending_directory()
    saved = 0
    for path in sorted(directory.glob('*.json')):
        with path.open(encoding='utf-8') as handle:
            entry = json.load(handle)
        write_database_logs(
            uuid4_from_text(entry['run_id']),
            datetime.fromisoformat(entry['started']),
            datetime.fromisoformat(entry['ended']), entry['duration'],
            entry['source_counts'], entry['staged_counts'], entry['results'],
            entry['error_code'], entry['error_message'],
        )
        path.unlink(missing_ok=True)
        saved += 1
    return saved


def uuid4_from_text(value):
    """Parse an existing run ID without assigning a new identity during replay."""
    from uuid import UUID
    return UUID(value)


def save_extraction_logs(run_id, started, ended, duration, source_counts,
                         staged_counts, results, error_code=None, error_message=None):
    """Durably queue metadata before attempting database delivery."""
    directory = private_pending_directory()
    entry = {
        'run_id': str(run_id), 'started': started.isoformat(),
        'ended': ended.isoformat(), 'duration': duration,
        'source_counts': source_counts, 'staged_counts': staged_counts,
        'results': results, 'error_code': error_code, 'error_message': error_message,
    }
    # Only counts/check results and fixed safe error messages enter this queue.
    target = directory / (str(run_id) + '.json')
    descriptor, temporary = tempfile.mkstemp(dir=directory, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(entry, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)
    flush_pending_logs()


def run_extraction_attempt(consumer=None, retry_state=None) -> int:
    retry_state = retry_state if retry_state is not None else {}
    retry_state['retryable'] = False
    if sys.argv[1:] == ['--setup-logs']:
        try:
            setup_extraction_logs()
        except (psycopg.Error, SourceConnectionError):
            print('Log setup failed. Check database access and administrator permissions.')
            return 1
        print('Restricted extraction log table created: etl_ops.extraction_logs.')
        print('Only its owner has access by default. Retention is at least one year.')
        return 0
    if sys.argv[1:]:
        print('Usage: python -m pipeline.extraction [--setup-logs]')
        return 1
    run_id = uuid4()
    started = datetime.now(timezone.utc)
    timer = perf_counter()
    source_counts, staged_counts = {}, {}
    results = {}
    run_checks = []
    error_code = error_message = None
    print(f'Run ID: {run_id}')
    try:
        with extract_source(source_counts, staged_counts, run_checks) as (connection, counts):
            print('Extraction completed from one read-only snapshot.')
            for table, count in counts.items():
                print(f'stage_{table}: {count} rows')
            results = validate_staged_data(connection)
            record_run_check(run_checks, 'completion', 'PASS')
            for check in results['checks']:
                print(f"{check['status']}: {check['table']}.{check['check']} "
                      f"(affected rows: {check['affected_rows']})")
            for table, status in results['tables'].items():
                print(f"Table validation: {table}: {status}")
            print(f"Overall validation: {results['status']}")
            decision = acceptance_decision(results)
            print(f'Acceptance: {decision}')
            if decision != 'APPROVED':
                raise AcceptanceBlocked(f'Handover blocked: {decision}. Staging will be rolled back.')
            if consumer is not None:
                handover_extraction(
                    connection, run_id, started, counts, results, consumer,
                    source_counts=source_counts, ended=datetime.now(timezone.utc),
                    duration=int((perf_counter() - timer) * 1000),
                )
                print('Approved extraction acknowledged by the next stage.')
            else:
                print('Standalone extraction check only; no next stage was invoked.')
            print('Records stayed in PostgreSQL; Python read only counts and check results.')
        print('Connection closed. Temporary staging tables were removed.')
    except AcceptanceBlocked as error:
        print(error)
    except (ExtractionError, SourceConnectionError) as error:
        # A consumer may have performed work; do not replay handover implicitly.
        retry_state['retryable'] = error.retryable and consumer is None
        print(error)
        error_code = ('EXTRACTION_ALREADY_RUNNING' if isinstance(error, ExtractionAlreadyRunning)
                      else 'SOURCE_CONNECTION_FAILED' if isinstance(error, SourceConnectionError)
                      else 'EXTRACTION_FAILED')
        # Store fixed messages rather than raw database errors or credentials.
        error_message = ('Another extraction holds the advisory lock. No source records were read.'
                         if isinstance(error, ExtractionAlreadyRunning)
                         else 'Source connection failed.' if isinstance(error, SourceConnectionError)
                         else 'Extraction or SQL validation could not complete.')
        record_run_check(run_checks, 'completion', 'FAIL')
    # Early failures retain the checks completed before the failing operation.
    results.setdefault('checks', []).extend(run_checks)
    tables = {}
    for table in SOURCE_COLUMNS:
        statuses = {check['status'] for check in results['checks']
                    if check['table'] in (table, '*')}
        tables[table] = ('FAIL' if 'FAIL' in statuses else
                         'WARNING' if 'WARNING' in statuses else 'PASS')
    results['tables'] = tables
    results['status'] = ('FAIL' if 'FAIL' in tables.values() else
                         'WARNING' if 'WARNING' in tables.values() else 'PASS')
    for check in run_checks:
        print(f"{check['status']}: run.{check['check']}")
    ended = datetime.now(timezone.utc)
    duration = int((perf_counter() - timer) * 1000)
    if not error_code and results.get('status') == 'FAIL':
        error_code = 'VALIDATION_FAILED'
        error_message = 'One or more extraction validation checks failed.'
    try:
        save_extraction_logs(run_id, started, ended, duration, source_counts,
                             staged_counts, results, error_code, error_message)
    except (psycopg.Error, SourceConnectionError, OSError, ValueError, subprocess.SubprocessError):
        # Never start another extraction while this attempt's log is unsaved.
        retry_state['retryable'] = False
        print('Could not save extraction logs. The run is not fully logged.')
        print('Check the private pending-log queue; queued entries remain until delivery succeeds.')
        print('Check log setup, permissions, and database connectivity before retrying.')
        print(f'Run ID: {run_id}; start: {started.isoformat()}; end: {ended.isoformat()}')
        return 1
    print(f'Run details saved in etl_ops.extraction_logs ({duration} ms).')
    return 1 if error_code else 2 if results.get('status') == 'WARNING' else 0


def main(consumer=None) -> int:
    """Try at most three times, with a fresh UUID and snapshot per attempt."""
    if sys.argv[1:] not in ([], ['--setup-logs'], ['--flush-logs']):
        print('Usage: python -m pipeline.extraction [--setup-logs | --flush-logs]')
        return 1
    if sys.argv[1:] != ['--setup-logs']:
        try:
            saved = flush_pending_logs()
        except (psycopg.Error, SourceConnectionError, OSError, ValueError,
                KeyError, subprocess.SubprocessError):
            print('Pending logs could not be delivered. Entries were retained; extraction did not start.')
            return 1
        if saved:
            print(f'Recovered {saved} pending extraction log entries.')
        if sys.argv[1:] == ['--flush-logs']:
            print('Pending-log delivery completed.')
            return 0
    for attempt in range(1, 4):
        retry_state = {}
        code = run_extraction_attempt(consumer, retry_state)
        if code != 1 or not retry_state['retryable']:
            return code
        if attempt == 3:
            print('Temporary failure persisted. Stopped after 3 extraction attempts.')
            return 1
        delay = 2 * attempt
        print(f'Temporary failure. Retrying in {delay} seconds '
              f'(attempt {attempt + 1}/3, with a new run ID).')
        sleep(delay)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
