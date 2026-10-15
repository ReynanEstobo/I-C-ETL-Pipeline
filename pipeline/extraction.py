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


def main() -> int:
    try:
        with extract_source() as (_, counts):
            print('Extraction completed from one read-only snapshot.')
            for table, count in counts.items():
                print(f'stage_{table}: {count} rows')
            print('Records stayed in PostgreSQL temporary tables; Python read only counts.')
        print('Connection closed. Temporary staging tables were removed.')
    except (ExtractionError, SourceConnectionError) as error:
        print(error)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
