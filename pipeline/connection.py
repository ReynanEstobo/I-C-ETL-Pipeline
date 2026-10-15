"""Read-only source connection used by extraction."""

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


class SourceConnectionError(RuntimeError):
    """A safe connection error that does not disclose credentials."""

    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def is_temporary_database_error(error):
    """Classify failures without exposing the original database message."""
    state = getattr(error, 'sqlstate', None)
    if state:
        return state.startswith('08') or state in {
            '40001', '40P01', '53300', '57P01', '57P02', '57P03',
        }
    # libpq may omit SQLSTATE for authentication and network failures.
    message = str(error).lower()
    if any(text in message for text in (
        'password authentication failed', 'no password supplied',
        'invalid connection option', 'invalid integer value',
        'certificate verify failed', 'role does not exist',
        'database does not exist', 'no pg_hba.conf entry',
    )):
        return False
    return isinstance(error, psycopg.OperationalError)


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
    except (psycopg.Error, ValueError) as error:
        if connection is not None:
            connection.close()
        raise SourceConnectionError(
            "Could not connect to the source database. Check your connection "
            "details, password, and network access.",
            retryable=is_temporary_database_error(error),
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
