"""Read the five source datasets specified in extraction.md (issue #2)."""

from contextlib import contextmanager

import psycopg
from psycopg import sql

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
def extract_source():
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

    statuses = {check['status'] for check in checks}
    overall = 'FAIL' if 'FAIL' in statuses else 'WARNING' if 'WARNING' in statuses else 'PASS'
    return {'status': overall, 'checks': checks}


def main() -> int:
    try:
        with extract_source() as (connection, counts):
            print('Extraction completed from one read-only snapshot.')
            for table, count in counts.items():
                print(f'stage_{table}: {count} rows')
            results = validate_staged_data(connection)
            for check in results['checks']:
                print(f"{check['status']}: {check['table']}.{check['check']} "
                      f"(affected rows: {check['affected_rows']})")
            print(f"Overall validation: {results['status']}")
            print('Records stayed in PostgreSQL; Python read only counts and check results.')
        print('Connection closed. Temporary staging tables were removed.')
    except (ExtractionError, SourceConnectionError) as error:
        print(error)
        return 1
    return 1 if results['status'] == 'FAIL' else 0


if __name__ == '__main__':
    raise SystemExit(main())
