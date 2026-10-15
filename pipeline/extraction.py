"""Read the five source datasets specified in extraction.md (issue #2)."""

from contextlib import contextmanager
from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4
import sys

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.connection import SourceConnectionError, connect_source


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
            except psycopg.Error:
                raise ExtractionError(
                    f"Cannot read the required columns from public.{table}. "
                    "Check source permissions and schema."
                ) from None


@contextmanager
def extract_source(source_counts=None, staged_counts=None):
    """Stage source rows in SQL and yield the connection and row counts.

    Stages remain available inside this context only. Future SQL validation
    must use the yielded connection before leaving this transaction.
    """
    try:
        with connect_source() as connection:
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
                with connection.cursor() as cursor:
                    cursor.execute('SHOW transaction_read_only')
                    if cursor.fetchone() != ('on',):
                        raise ExtractionError('Extraction requires a read-only transaction.')
                    cursor.execute('SHOW transaction_isolation')
                    if cursor.fetchone() != ('repeatable read',):
                        raise ExtractionError('Extraction requires a consistent snapshot.')

                check_source_schema(connection)
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
                    except psycopg.Error:
                        raise ExtractionError(
                            f'Could not stage public.{table}. '
                            'Extraction was rolled back; source records were unchanged.'
                        ) from None
                yield connection, counts
    except psycopg.Error:
        raise ExtractionError(
            'Extraction failed. Check database access, temporary-table permissions, '
            'and network connectivity. Source records were unchanged.'
        ) from None


def validate_staged_data(
    connection: psycopg.Connection,
    source_schema: str = 'public',
    source_prefix: str = '',
) -> dict:
    """Run issue #3 checks in SQL; return counts and statuses, never records.

    Call inside extract_source() so source counts and stages share a snapshot.
    Nullable dates/references are warnings, not repaired or removed here.
    """
    checks = []
    with connection.cursor() as cursor:
        def record(table, name, query, warning=False):
            cursor.execute(query)
            affected = cursor.fetchone()[0]
            status = ('WARNING' if warning else 'FAIL') if affected else 'PASS'
            checks.append({
                'table': table, 'check': name, 'status': status,
                'affected_rows': affected,
            })

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


def save_extraction_logs(run_id, started, ended, duration, source_counts,
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
                               if check['table'] == table],
                }
                cursor.execute('''
                    INSERT INTO etl_ops.extraction_logs (
                        pipeline_run_id, table_name, extraction_started_at,
                        extraction_ended_at, extraction_status, records_read,
                        records_extracted, records_rejected, extraction_window,
                        validation_results, error_code, error_message, duration_ms
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s)
                ''', (run_id, 'public.' + table, started, ended, status,
                      source_counts.get(table), staged_counts.get(table),
                      'All available history', Jsonb(details), error_code,
                      error_message, duration))


def main() -> int:
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
    error_code = error_message = None
    print(f'Run ID: {run_id}')
    try:
        with extract_source(source_counts, staged_counts) as (connection, counts):
            print('Extraction completed from one read-only snapshot.')
            for table, count in counts.items():
                print(f'stage_{table}: {count} rows')
            results = validate_staged_data(connection)
            for check in results['checks']:
                print(f"{check['status']}: {check['table']}.{check['check']} "
                      f"(affected rows: {check['affected_rows']})")
            for table, status in results['tables'].items():
                print(f"Table validation: {table}: {status}")
            print(f"Overall validation: {results['status']}")
            print('Records stayed in PostgreSQL; Python read only counts and check results.')
        print('Connection closed. Temporary staging tables were removed.')
    except (ExtractionError, SourceConnectionError) as error:
        print(error)
        error_code = ('SOURCE_CONNECTION_FAILED' if isinstance(error, SourceConnectionError)
                      else 'EXTRACTION_FAILED')
        # Store fixed messages rather than raw database errors or credentials.
        error_message = ('Source connection failed.' if isinstance(error, SourceConnectionError)
                         else 'Extraction or SQL validation could not complete.')
    ended = datetime.now(timezone.utc)
    duration = int((perf_counter() - timer) * 1000)
    if not error_code and results.get('status') == 'FAIL':
        error_code = 'VALIDATION_FAILED'
        error_message = 'One or more extraction validation checks failed.'
    try:
        save_extraction_logs(run_id, started, ended, duration, source_counts,
                             staged_counts, results, error_code, error_message)
    except (psycopg.Error, SourceConnectionError):
        print('Could not save extraction logs. The run is not fully logged.')
        print('Check log setup, permissions, and database connectivity before retrying.')
        print(f'Run ID: {run_id}; start: {started.isoformat()}; end: {ended.isoformat()}')
        return 1
    print(f'Run details saved in etl_ops.extraction_logs ({duration} ms).')
    return 1 if error_code else 0


if __name__ == '__main__':
    raise SystemExit(main())
