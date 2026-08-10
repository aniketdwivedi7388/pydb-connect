"""Oracle backend, on ``python-oracledb``.

Install with ``pip install "pydb-connect[oracle]"``. ``oracledb`` runs in *thin*
mode by default, which needs no Oracle Instant Client - a large improvement over
``cx_Oracle``. Thick mode is available by setting ``options.thick: true``, which
you need for Kerberos, some proxy authentication and a few legacy character
sets.

Configuration::

    connections:
      erp:
        backend: oracle
        host: ora.internal
        port: 1521
        database: ORCLPDB1          # service name
        schema: STAGING             # applied as ALTER SESSION SET CURRENT_SCHEMA
        user: ETL_USER
        secret: env:ORACLE_PASSWORD
        options:
          service_name: ORCLPDB1    # explicit alternative to 'database'
          dsn: myhost:1521/ORCLPDB1 # or supply a full DSN / TNS alias
          thick: false
          arraysize: 5000

On bulk loading: Oracle has no ``COPY``, so :attr:`supports_copy` is False. That
is not a gap. ``oracledb``'s ``executemany`` is *array DML*: the whole batch
goes to the server in one call and is applied in a single context switch, which
is Oracle's real bulk path. :func:`~pydbconnect.bulk.bulk_insert` is the fast
option here, not a consolation prize.

Placeholders are ``:1``, ``:2`` - Oracle's numeric style. The shared bulk layer
binds positionally, so this is invisible to callers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence

from ..exceptions import ConnectionFailure
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["OracleBackend"]

log = logging.getLogger("pydbconnect.backends.oracle")

#: ORA codes worth retrying: dead links, listener problems, resource waits.
#: 3113/3114 end-of-file on communication channel, 12170 TNS timeout,
#: 12541 no listener, 12514 service not known yet, 12571 packet failure,
#: 1033 in startup, 1089 shutdown, 60 deadlock, 54 resource busy, 4021 lock.
_RETRYABLE_ORA = frozenset({
    12, 54, 60, 1033, 1034, 1089, 1092, 3113, 3114, 3135, 4021, 4068,
    12152, 12170, 12500, 12514, 12518, 12520, 12528, 12537, 12541, 12571,
    25408, 25409,
})

#: ORA codes that will never succeed on a retry.
#: 1 unique constraint, 904 invalid identifier, 942 table does not exist,
#: 1017 invalid credentials, 1400 NULL into NOT NULL, 2291 FK violation.
_PERMANENT_ORA = frozenset({
    1, 900, 902, 903, 904, 905, 906, 907, 911, 913, 917, 918, 920, 923,
    933, 936, 942, 947, 957, 972, 979, 1017, 1031, 1400, 1722, 1747,
    1858, 2289, 2290, 2291, 2292, 12899,
})


class OracleBackend(Backend):
    """Oracle Database via ``python-oracledb``."""

    name = "oracle"
    driver_module = "oracledb"
    install_extra = "oracle"
    default_port = 1521
    required_fields = ("user",)

    placeholder_style = "numeric"   # :1, :2, ...
    quote_char = '"'
    supports_copy = False           # executemany *is* array DML; see module docstring
    supports_upsert = True          # MERGE
    supports_streaming = True
    supports_transactions = True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Open a connection.

        The DSN is taken from ``options.dsn`` when present, otherwise built as
        ``host:port/service_name`` from ``host``, ``port`` and
        ``options.service_name`` or ``database``.
        """
        driver = self.import_driver(
            hint="oracledb runs in thin mode with no Instant Client required"
        )
        options = dict(config.options or {})
        secret = config.resolve_password()

        if options.pop("thick", False):
            lib_dir = options.pop("lib_dir", None)
            try:
                driver.init_oracle_client(lib_dir=lib_dir) if lib_dir else driver.init_oracle_client()
            except Exception as exc:
                raise ConnectionFailure(
                    f"could not initialise the Oracle thick client: "
                    f"{type(exc).__name__}: {exc}. Remove options.thick to use thin mode",
                    connection=config.name,
                ) from exc

        dsn = options.pop("dsn", None)
        if not dsn:
            service = options.pop("service_name", None) or config.database
            if not service:
                raise ConnectionFailure(
                    "oracle needs a service name: set 'database', "
                    "'options.service_name' or 'options.dsn'",
                    connection=config.name,
                )
            port = config.effective_port(self.default_port)
            dsn = f"{config.host}:{port}/{service}" if config.host else str(service)

        kwargs: dict = {"user": config.user, "dsn": dsn}
        if secret is not None:
            kwargs["password"] = secret.reveal()
        options.pop("arraysize", None)  # applied per cursor, not per connection
        kwargs.update({k: v for k, v in options.items() if v is not None})

        try:
            conn = driver.connect(**kwargs)
        except Exception as exc:
            raise ConnectionFailure(
                f"cannot connect to oracle dsn {dsn!r} as {config.user!r}: "
                f"{type(exc).__name__}: {exc}",
                connection=config.name,
            ) from exc
        return conn

    def cursor(self, conn: Any, *, server_side: bool = False, name: str = "") -> Any:
        """Return a cursor.

        Oracle cursors are server-side already; the ``arraysize`` and
        ``prefetchrows`` settings control how many rows come back per round
        trip. They are raised well above the default of 100 for streaming,
        which is the single biggest win available when reading a large table
        from Oracle.
        """
        cur = conn.cursor()
        if server_side:
            try:
                cur.arraysize = 5000
                cur.prefetchrows = 5001
            except Exception:
                log.debug("could not tune cursor arraysize", exc_info=True)
        return cur

    def ping(self, conn: Any) -> bool:
        """Use the driver's own ``ping``, falling back to a trivial query."""
        try:
            if hasattr(conn, "ping"):
                conn.ping()
                return True
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1 FROM dual")
                cur.fetchone()
            finally:
                cur.close()
        except Exception:  # noqa: BLE001 - ping must never raise; the pool relies on it
            return False
        return True

    def set_schema_sql(self, schema: str) -> Optional[str]:
        """Return ``ALTER SESSION SET CURRENT_SCHEMA``."""
        return f"ALTER SESSION SET CURRENT_SCHEMA = {self.quote_identifier(schema)}"

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return a single-row ``MERGE`` binding each value exactly once.

        The values are bound into a ``SELECT ... FROM dual`` source, then
        referenced by alias in both branches. That keeps the parameter count
        equal to ``len(columns)``, so the statement works with ``executemany``
        and the caller does not have to send every value twice.

        Oracle forbids updating a column that appears in the ``ON`` clause, so
        key columns are excluded from the update list - which is the default
        behaviour of :meth:`~pydbconnect.backends.base.Backend.merge_update_columns`
        anyway.

        Raises:
            ValueError: A key column is not among ``columns``, or every column
                is a key (Oracle has no ``WHEN MATCHED THEN DO NOTHING``).
        """
        updates = [c for c in self.merge_update_columns(columns, key_columns, update_columns)
                   if c not in key_columns]
        target = self.quote_identifier(table)
        source_cols = ", ".join(
            f"{self.placeholder(i + 1)} AS {self.quote_identifier(c)}"
            for i, c in enumerate(columns)
        )
        on_clause = " AND ".join(
            f"tgt.{self.quote_identifier(k)} = src.{self.quote_identifier(k)}"
            for k in key_columns
        )
        insert_cols = ", ".join(self.quote_identifier(c) for c in columns)
        insert_vals = ", ".join(f"src.{self.quote_identifier(c)}" for c in columns)

        statement = (
            f"MERGE INTO {target} tgt "
            f"USING (SELECT {source_cols} FROM dual) src "
            f"ON ({on_clause}) "
        )
        if updates:
            assignments = ", ".join(
                f"tgt.{self.quote_identifier(c)} = src.{self.quote_identifier(c)}"
                for c in updates
            )
            statement += f"WHEN MATCHED THEN UPDATE SET {assignments} "
        elif not updates and len(key_columns) == len(columns):
            raise ValueError(
                "every column is a key column, so there is nothing to update; "
                "use bulk_insert with an ON-CONFLICT-tolerant table or add a "
                "non-key column"
            )
        statement += (
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        return statement

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify by ORA number.

        ``ORA-03113: end-of-file on communication channel`` after an idle period
        is the classic Oracle transient - a firewall dropped the TCP session and
        nobody told the client. ``ORA-00001: unique constraint violated`` is not
        transient no matter how many times you try it.
        """
        code = self._ora_code(exc)
        if code is not None:
            if code in _RETRYABLE_ORA:
                return True
            if code in _PERMANENT_ORA:
                return False
        text = str(exc).upper()
        for number in _RETRYABLE_ORA:
            if f"ORA-{number:05d}" in text:
                return True
        for number in _PERMANENT_ORA:
            if f"ORA-{number:05d}" in text:
                return False
        if "DPY-4011" in text or "DPY-6005" in text:   # oracledb network errors
            return True
        return None

    @staticmethod
    def _ora_code(exc: BaseException) -> Optional[int]:
        """Pull the numeric ORA code out of an ``oracledb`` error object."""
        args = getattr(exc, "args", ())
        if args:
            code = getattr(args[0], "code", None)
            if isinstance(code, int):
                return code
        code = getattr(exc, "code", None)
        if isinstance(code, int):
            return code
        return None
