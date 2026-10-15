"""Read-only source connection used by extraction."""

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


class SourceConnectionError(RuntimeError):
    """A safe connection error that does not disclose credentials."""


def connect_source() -> psycopg.Connection:
    """Open an encrypted connection whose transactions default to read-only.

    The caller owns the returned connection and must close it, preferably with
    a ``with connect_source() as connection:`` block.
    """
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    connection_url = os.environ.get("SOURCE_DATABASE_URL", "").strip()
    if not connection_url:
        raise SourceConnectionError(
            "Set SOURCE_DATABASE_URL in your local .env file before connecting."
        )

    connection = None
    try:
        connection = psycopg.connect(
            connection_url,
            sslmode="require",
            connect_timeout=10,
            options="-c default_transaction_read_only=on",
        )
        connection.read_only = True
        return connection
    except (psycopg.Error, ValueError):
        if connection is not None:
            connection.close()
        raise SourceConnectionError(
            "Could not connect to the source database. Check your connection "
            "details, password, and network access."
        ) from None


def main() -> int:
    """Check connectivity and the server's read-only setting without writes."""
    try:
        with connect_source() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_read_only")
                if cursor.fetchone() != ("on",):
                    raise SourceConnectionError("The connection is not read-only.")
    except (SourceConnectionError, psycopg.Error) as error:
        message = str(error) if isinstance(error, SourceConnectionError) else (
            "The database connection check failed. Check your settings and retry."
        )
        print(message)
        return 1
    print("Connected successfully. The source connection is read-only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
