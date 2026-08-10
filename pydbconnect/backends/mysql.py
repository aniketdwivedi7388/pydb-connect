"""MySQL and MariaDB backend, on ``mysql-connector-python``.

Install with ``pip install "pydb-connect[mysql]"``.

Configuration::

    connections:
      orders:
        backend: mysql
        host: mysql.internal
        port: 3306
        database: orders
        user: etl
        secret: env:MYSQL_PASSWORD
        options:
          charset: utf8mb4          # use this, not 'utf8', which is 3-byte
          connection_timeout: 10
          ssl_disabled: false
          autocommit: false

Notes that save an afternoon:

* ``executemany`` in this driver rewrites a batch of single-row inserts into one
  multi-row ``INSERT``, so :func:`~pydbconnect.bulk.bulk_insert` really is one
  round trip per chunk. That rewrite only happens for statements matching
  ``INSERT ... VALUES (...)``, which is exactly what this library generates.
* ``LOAD DATA LOCAL INFILE`` needs ``local_infile=1`` on the server *and*
  ``allow_local_infile=True`` on the client. Many managed MySQL services
  disable it, so :func:`~pydbconnect.bulk.copy_from` may fall back.
* ``ON DUPLICATE KEY UPDATE`` fires on *any* unique key, not only the one you
  had in mind. With two unique indexes it can update a row you did not intend
  to touch.
"""

from __future__ import annotations

import contextlib
import csv
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence

from ..exceptions import ConnectionFailure
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["MySQLBackend"]

log = logging.getLogger("pydbconnect.backends.mysql")

#: Server error codes worth retrying.
#: 1040 too many connections, 1053 shutdown in progress, 1205 lock wait timeout,
#: 1213 deadlock, 1290 read-only, 2002-2013 client/network failures.
_RETRYABLE_ERRNOS = frozenset({
    1040, 1053, 1152, 1158, 1159, 1160, 1161, 1205, 1213, 1290,
    2002, 2003, 2005, 2006, 2013, 2055,
})

#: Codes that will never succeed on a retry: syntax, missing objects,
#: constraint violations, authentication.
_PERMANENT_ERRNOS = frozenset({
    1045, 1046, 1049, 1051, 1054, 1062, 1064, 1136, 1146, 1216, 1217,
    1264, 1292, 1364, 1406, 1451, 1452, 1698,
})


class MySQLBackend(Backend):
    """MySQL / MariaDB via ``mysql.connector``."""

    name = "mysql"
    driver_module = "mysql.connector"
    install_extra = "mysql"
    default_port = 3306
    required_fields = ("host", "database")

    placeholder_style = "format"   # %s
    quote_char = "`"
    supports_copy = True           # LOAD DATA LOCAL INFILE, when permitted
    supports_upsert = True
    supports_streaming = True
    supports_transactions = True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Open a connection.

        ``options`` pass through to ``mysql.connector.connect``. ``autocommit``
        is forced off so that :meth:`~pydbconnect.connection.Connection.transaction`
        actually controls the transaction boundary.
        """
        driver = self.import_driver()
        secret = config.resolve_password()
        kwargs: dict = {
            "host": config.host,
            "port": config.effective_port(self.default_port),
            "database": config.database,
            "user": config.user,
        }
        if secret is not None:
            kwargs["password"] = secret.reveal()
        kwargs.update(config.options or {})
        kwargs.setdefault("charset", "utf8mb4")
        kwargs["autocommit"] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        try:
            conn = driver.connect(**kwargs)
        except Exception as exc:
            raise ConnectionFailure(
                f"cannot connect to mysql at {config.host}:{kwargs.get('port')} "
                f"database {config.database!r} as {config.user!r}: "
                f"{type(exc).__name__}: {exc}",
                connection=config.name,
            ) from exc
        return conn

    def cursor(self, conn: Any, *, server_side: bool = False, name: str = "") -> Any:
        """Return a cursor; an unbuffered one for streaming.

        ``mysql.connector`` buffers the whole result set by default. An
        unbuffered cursor reads rows from the socket as you consume them, which
        is what :meth:`~pydbconnect.connection.Connection.stream` wants. The
        trade-off: you must finish reading before running another statement on
        the same connection.
        """
        if server_side:
            try:
                return conn.cursor(buffered=False)
            except TypeError:  # pragma: no cover - older driver builds
                return conn.cursor()
        return conn.cursor()

    def ping(self, conn: Any) -> bool:
        """Return True if the server responds, reconnecting is left to the pool."""
        try:
            if hasattr(conn, "is_connected") and not conn.is_connected():
                return False
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                cur.fetchall()
            finally:
                cur.close()
        except Exception:  # noqa: BLE001 - ping must never raise; the pool relies on it
            return False
        return True

    def set_schema_sql(self, schema: str) -> Optional[str]:
        """MySQL's schema is its database; ``USE`` switches it."""
        return f"USE {self.quote_identifier(schema)}"

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return ``INSERT ... ON DUPLICATE KEY UPDATE ...``.

        ``VALUES(col)`` is used rather than the newer ``AS new`` row alias,
        because the alias form needs MySQL 8.0.19+ and this statement has to
        work on 5.7 and MariaDB too. ``VALUES()`` is deprecated in 8.0.20 but
        still functional everywhere.

        Note that MySQL matches on *any* unique key, not on ``key_columns``
        specifically - the argument documents intent and picks the update list,
        but the server decides what counts as a duplicate.
        """
        updates = self.merge_update_columns(columns, key_columns, update_columns)
        insert = self.insert_sql(table, columns)
        if not updates:
            # No-op assignment: the idiomatic MySQL way to say DO NOTHING.
            first = self.quote_identifier(key_columns[0])
            return f"{insert} ON DUPLICATE KEY UPDATE {first} = {first}"
        assignments = ", ".join(
            f"{self.quote_identifier(c)} = VALUES({self.quote_identifier(c)})"
            for c in updates
        )
        return f"{insert} ON DUPLICATE KEY UPDATE {assignments}"

    def copy_from(
        self,
        conn: Any,
        table: str,
        rows: Iterable[Sequence[Any]],
        columns: Sequence[str],
        **options: Any,
    ) -> int:
        """Load rows with ``LOAD DATA LOCAL INFILE``.

        Rows are written to a temporary CSV and handed to the server in one
        statement. The temporary file is removed on every path.

        Requires ``local_infile=ON`` on the server and ``allow_local_infile=True``
        in ``options``. Without both, the driver raises and
        :func:`~pydbconnect.bulk.copy_from` falls back to chunked
        ``executemany``.

        Args:
            conn: Raw driver connection.
            table: Target table.
            rows: Tuples in ``columns`` order.
            columns: Column names.
            **options: ``null_marker`` (default ``\\N``), ``encoding``
                (default ``utf-8``).

        Returns:
            Rows loaded.
        """
        null_marker = options.get("null_marker", "\\N")
        encoding = options.get("encoding", "utf-8")
        materialised = list(rows)
        if not materialised:
            return 0

        # Not a context manager: the file must survive being closed so the
        # server can read it, and it is removed in the finally block below.
        handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
            mode="w", suffix=".csv", delete=False, encoding=encoding, newline=""
        )
        path = handle.name
        try:
            writer = csv.writer(handle, lineterminator="\n")
            for row in materialised:
                writer.writerow([null_marker if v is None else v for v in row])
            handle.close()

            cols = ", ".join(self.quote_identifier(c) for c in columns)
            statement = (
                f"LOAD DATA LOCAL INFILE %s INTO TABLE {self.quote_identifier(table)} "
                f"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' ({cols})"
            )
            cur = conn.cursor()
            try:
                cur.execute(statement, (path,))
            finally:
                cur.close()
        finally:
            with contextlib.suppress(Exception):
                handle.close()          # already closed on the happy path
            try:
                os.unlink(path)
            except OSError:  # pragma: no cover
                log.debug("could not remove temporary file %s", path)
        return len(materialised)

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify by MySQL error number.

        ``2006 MySQL server has gone away`` and ``1213 deadlock found`` are the
        two you will actually meet: the first because a proxy dropped an idle
        connection, the second because two jobs touched the same rows in
        different orders. Both are worth retrying. ``1062 duplicate entry`` is
        not.
        """
        errno = getattr(exc, "errno", None)
        if errno is None:
            code = self._error_code(exc)
            errno = int(code) if code and str(code).isdigit() else None
        if errno is not None:
            if errno in _RETRYABLE_ERRNOS:
                return True
            if errno in _PERMANENT_ERRNOS:
                return False
            if 2000 <= errno < 2100:      # client-side / network errors
                return True
        name = type(exc).__name__
        if name in ("InterfaceError", "OperationalError", "PoolError"):
            return True
        if name in ("ProgrammingError", "IntegrityError", "DataError", "NotSupportedError"):
            return False
        return None
