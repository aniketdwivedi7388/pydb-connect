"""PostgreSQL backend, on ``psycopg`` (v3) or ``psycopg2``.

Install with ``pip install "pydb-connect[postgres]"``, which pulls
``psycopg[binary]``. If ``psycopg2`` is already present this backend uses it
instead - the API differences are handled here, not by the caller.

Configuration::

    connections:
      warehouse:
        backend: postgres
        host: db.internal
        port: 5432
        database: analytics
        schema: reporting          # applied as SET search_path
        user: etl_writer
        secret: env:PGPASSWORD
        options:
          sslmode: require
          application_name: nightly-load     # shows up in pg_stat_activity
          connect_timeout: 10

Two things worth knowing:

* **Streaming is genuinely server-side.** ``Connection.stream`` asks for a named
  cursor, so PostgreSQL keeps the result set on the server and sends it in
  batches. Without it, libpq buffers the entire result in client memory before
  your first row arrives - which is why "the query is fast but Python OOMs" is
  such a common PostgreSQL story.
* **``COPY`` is the fast path and it is not close.** ``COPY ... FROM STDIN``
  bypasses the SQL parser and planner per row. Expect 5-20x ``executemany``.
"""

from __future__ import annotations

import csv
import io
import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence

from ..exceptions import ConnectionFailure, DriverNotInstalledError
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["PostgresBackend"]

log = logging.getLogger("pydbconnect.backends.postgres")

#: SQLSTATE values that mean "try again": connection exceptions, serialisation
#: failures, deadlocks, resource exhaustion, and admin shutdowns.
_RETRYABLE_SQLSTATES = frozenset({
    "08000", "08003", "08006", "08001", "08004", "08007", "08P01",
    "40001", "40P01",
    "53000", "53100", "53200", "53300", "55P03",
    "57P01", "57P02", "57P03", "58030",
})

#: SQLSTATE values that will never succeed on a retry.
_PERMANENT_SQLSTATES = frozenset({
    "23505", "23503", "23502", "23514", "23P01",   # integrity
    "42601", "42703", "42P01", "42P07", "42883", "42501", "42804",  # programming
    "28000", "28P01",                               # authentication
    "22001", "22003", "22007", "22012", "22P02",    # data
})


class PostgresBackend(Backend):
    """PostgreSQL via psycopg 3, falling back to psycopg2."""

    name = "postgres"
    driver_module = "psycopg"
    install_extra = "postgres"
    default_port = 5432
    required_fields = ("host", "database")

    placeholder_style = "format"   # %s, for both psycopg and psycopg2
    quote_char = '"'
    supports_copy = True
    supports_upsert = True
    supports_streaming = True
    supports_transactions = True

    def import_driver(self, module: Optional[str] = None, hint: str = "") -> Any:
        """Import ``psycopg``, or ``psycopg2`` when only that is installed."""
        if module:
            return super().import_driver(module, hint)
        try:
            import psycopg

            return psycopg
        except ImportError:
            pass
        try:
            import psycopg2

            return psycopg2
        except ImportError as exc:
            raise DriverNotInstalledError(
                self.name, "psycopg", self.install_extra,
                "psycopg2 is also accepted if you already depend on it",
            ) from exc

    @staticmethod
    def _is_psycopg3(driver: Any) -> bool:
        return getattr(driver, "__name__", "") == "psycopg"

    def connect(self, config: "ConnectionConfig") -> Any:
        """Open a connection.

        ``options`` are passed straight through as libpq connection parameters,
        so ``sslmode``, ``application_name``, ``connect_timeout``,
        ``target_session_attrs`` and the rest work without this backend needing
        to know about them.
        """
        driver = self.import_driver()
        secret = config.resolve_password()
        kwargs: dict = {
            "host": config.host,
            "port": config.effective_port(self.default_port),
            "dbname": config.database,
            "user": config.user,
        }
        if secret is not None:
            kwargs["password"] = secret.reveal()
        kwargs.update(config.options or {})
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        try:
            conn = driver.connect(**kwargs)
        except Exception as exc:
            raise ConnectionFailure(
                f"cannot connect to postgres at {config.host}:{kwargs.get('port')} "
                f"database {config.database!r} as {config.user!r}: "
                f"{type(exc).__name__}: {exc}",
                connection=config.name,
            ) from exc
        conn.autocommit = False
        return conn

    def cursor(self, conn: Any, *, server_side: bool = False, name: str = "") -> Any:
        """Return a cursor; a named (server-side) one when asked.

        A named cursor keeps the result set on the server, which is the only way
        to iterate a large table without libpq buffering all of it client-side.
        If the driver refuses - some pooling proxies do - this falls back to a
        normal cursor and logs it, rather than failing the query.
        """
        if not server_side:
            return conn.cursor()
        try:
            return conn.cursor(name=name or "pydb_stream")
        except Exception as exc:  # noqa: BLE001 - degrade to a client cursor, do not fail the query
            log.debug("server-side cursor unavailable (%s); using a client cursor", exc)
            return conn.cursor()

    def ping(self, conn: Any) -> bool:
        """Return True if the connection answers ``SELECT 1``."""
        try:
            if getattr(conn, "closed", 0):
                return False
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                cur.fetchone()
            finally:
                cur.close()
        except Exception:  # noqa: BLE001 - ping must never raise; the pool relies on it
            return False
        return True

    def set_schema_sql(self, schema: str) -> Optional[str]:
        """Return ``SET search_path TO <schema>``."""
        return f"SET search_path TO {self.quote_identifier(schema)}"

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return ``INSERT ... ON CONFLICT (keys) DO UPDATE SET ...``.

        ``key_columns`` must be covered by a unique index or primary key -
        PostgreSQL resolves the conflict target against an index, and reports
        ``there is no unique or exclusion constraint matching the ON CONFLICT
        specification`` when there is not.
        """
        updates = self.merge_update_columns(columns, key_columns, update_columns)
        insert = self.insert_sql(table, columns)
        conflict = ", ".join(self.quote_identifier(k) for k in key_columns)
        if not updates:
            return f"{insert} ON CONFLICT ({conflict}) DO NOTHING"
        assignments = ", ".join(
            f"{self.quote_identifier(c)} = EXCLUDED.{self.quote_identifier(c)}"
            for c in updates
        )
        return f"{insert} ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"

    def copy_from(
        self,
        conn: Any,
        table: str,
        rows: Iterable[Sequence[Any]],
        columns: Sequence[str],
        **options: Any,
    ) -> int:
        """Load rows with ``COPY ... FROM STDIN``.

        Rows are serialised as CSV and streamed to the server. Both psycopg 3
        (``cursor.copy``) and psycopg2 (``cursor.copy_expert``) are supported.

        Args:
            conn: The raw driver connection.
            table: Target table.
            rows: Tuples in ``columns`` order.
            columns: Column names.
            **options: ``null_marker`` (default ``\\N``) sets the ``NULL``
                token used in the CSV stream.

        Returns:
            The number of rows sent.
        """
        driver = self.import_driver()
        null_marker = options.get("null_marker", "\\N")
        cols = ", ".join(self.quote_identifier(c) for c in columns)
        statement = (
            f"COPY {self.quote_identifier(table)} ({cols}) FROM STDIN "
            f"WITH (FORMAT csv, NULL '{null_marker}')"
        )
        materialised = list(rows)
        if not materialised:
            return 0

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        for row in materialised:
            writer.writerow([null_marker if v is None else v for v in row])
        buffer.seek(0)

        cur = conn.cursor()
        try:
            if self._is_psycopg3(driver):
                with cur.copy(statement) as copy:
                    copy.write(buffer.getvalue())
            else:
                cur.copy_expert(statement, buffer)
        finally:
            cur.close()
        return len(materialised)

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify by SQLSTATE, which PostgreSQL always supplies.

        This is why SQLSTATE exists: ``40001`` is a serialisation failure and
        retrying it is the documented remedy, while ``23505`` is a duplicate key
        and retrying it will produce the same duplicate key forever.
        """
        state = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
        if state is None:
            diag = getattr(exc, "diag", None)
            state = getattr(diag, "sqlstate", None) if diag is not None else None
        if state:
            state = str(state).upper()
            if state in _PERMANENT_SQLSTATES:
                return False
            if state in _RETRYABLE_SQLSTATES or state.startswith(("08", "40", "53")):
                return True
            if state.startswith(("22", "23", "42", "28")):
                return False
        name = type(exc).__name__
        if name in ("OperationalError", "InterfaceError", "AdminShutdown"):
            return True
        if name in ("ProgrammingError", "IntegrityError", "DataError", "NotSupportedError"):
            return False
        return None
