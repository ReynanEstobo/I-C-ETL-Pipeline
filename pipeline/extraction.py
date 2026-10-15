"""Read the five source datasets specified in extraction.md (issue #2)."""

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

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


def extract_source() -> dict[str, list[dict]]:
    """Return separate, unchanged datasets only after every read succeeds.

    Native UUID, Decimal, date, datetime and None values are retained. The
    caller receives in-memory data; this function does not save output.
    """
    try:
        with connect_source() as connection:
            connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SHOW transaction_read_only")
                    if cursor.fetchone() != ("on",):
                        raise ExtractionError("Extraction requires a read-only transaction.")
                    cursor.execute("SHOW transaction_isolation")
                    if cursor.fetchone() != ("repeatable read",):
                        raise ExtractionError("Extraction requires a consistent snapshot.")

                check_source_schema(connection)
                datasets = {}
                for table, columns in SOURCE_COLUMNS.items():
                    try:
                        with connection.cursor(row_factory=dict_row) as cursor:
                            cursor.execute(
                                sql.SQL("SELECT {} FROM {}").format(
                                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                                    sql.Identifier("public", table),
                                )
                            )
                            datasets[table] = cursor.fetchall()
                    except psycopg.Error:
                        raise ExtractionError(
                            f"Could not finish reading public.{table}. "
                            "No datasets were handed over."
                        ) from None
        return datasets
    except psycopg.Error:
        raise ExtractionError(
            "Extraction failed. Check database access and retry. "
            "No datasets were handed over."
        ) from None


def main() -> int:
    try:
        datasets = extract_source()
    except (ExtractionError, SourceConnectionError) as error:
        print(error)
        return 1
    print("Extraction completed from one read-only snapshot.")
    for table, rows in datasets.items():
        print(f"{table}: {len(rows)} rows")
    print("Data was read into memory only; no output files were saved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
