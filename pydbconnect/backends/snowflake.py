"""Snowflake backend, on ``snowflake-connector-python``.

Install with ``pip install "pydb-connect[snowflake]"``.

Configuration::

    connections:
      analytics:
        backend: snowflake
        database: ANALYTICS
        schema: PUBLIC
        user: ETL_SVC
        secret: env:SNOWFLAKE_PASSWORD
        options:
          account: ab12345.eu-west-1     # required
          warehouse: LOAD_WH
          role: TRANSFORMER
          client_session_keep_alive: true
          private_key_file: /run/secrets/rsa_key.p8   # key-pair auth

Key-pair authentication is preferred over passwords for service accounts, and
Snowflake is progressively requiring it. Set ``options.private_key_file`` and
put the passphrase in ``secret`` - the passphrase is then handled like every
other credential here rather than sitting in the file next to the key.

Cost note, since Snowflake bills by the second: an idle connection does not keep
a warehouse running, but an open transaction does hold locks. Keep
``pool.recycle`` well under the warehouse auto-suspend interval and do not hold
transactions open across long Python work.

On bulk loading: :attr:`supports_copy` is True and it means what it says. Rows
are written to a local gzipped CSV, ``PUT`` to the table's internal stage, and
loaded with ``COPY INTO``. That is Snowflake's designed bulk path and it is
dramatically faster than ``INSERT`` above roughly 100k rows. Below that, the
staging overhead dominates - use :func:`~pydbconnect.bulk.bulk_insert` instead.
"""

from __future__ import annotations

import csv
import gzip
import logging
import os
import tempfile
import uuid
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence

from ..exceptions import ConnectionFailure
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["SnowflakeBackend"]

log = logging.getLogger("pydbconnect.backends.snowflake")

#: Connector error codes worth retrying.
#: 250001 connection closed, 250002/250003 network, 250006 request timeout,
#: 390114 authentication token expired, 604 statement cancelled by a restart.
_RETRYABLE_CODES = frozenset({
    "250001", "250002", "250003", "250006", "250007",
    "390114", "390195", "604", "000629", "000630",
})

#: Codes that will never succeed on a retry: SQL compilation, object not found,
#: authentication failures, insufficient privileges.
_PERMANENT_CODES = frozenset({
    "001003", "002003", "002043", "090105", "100038",
    "390100", "390101", "390102", "003001", "003011",
})


class SnowflakeBackend(Backend):
    """Snowflake via ``snowflake.connector``."""

    name = "snowflake"
    driver_module = "snowflake.connector"
    install_extra = "snowflake"
    default_port = None
    required_fields = ("user", "database")

    placeholder_style = "format"   # pyformat paramstyle binds %s positionally
    quote_char = '"'
    supports_copy = True           # PUT + COPY INTO
    supports_upsert = True         # MERGE
    supports_streaming = True
    supports_transactions = True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Open a connection.

        ``options.account`` is required; ``host`` is accepted as an alias for
        it, since that is where people habitually put it.
        """
        driver = self.import_driver()
        options = dict(config.options or {})
        secret = config.resolve_password()

        account = options.pop("account", None) or config.host
        if not account:
            raise ConnectionFailure(
                "snowflake needs an account identifier: set options.account "
                "(for example ab12345.eu-west-1)",
                connection=config.name,
            )

        kwargs: dict = {
            "account": account,
            "user": config.user,
            "database": config.database,
            "schema": config.schema,
        }
        private_key_file = options.pop("private_key_file", None)
        if private_key_file:
            kwargs["private_key_file"] = private_key_file
            if secret is not None:
                # Snowflake calls the key passphrase this. Reuse 'secret' so the
                # passphrase goes through the same resolver as everything else.
                kwargs["private_key_file_pwd"] = secret.reveal().encode("utf-8")
        elif secret is not None:
            kwargs["password"] = secret.reveal()

        kwargs.update(options)
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        try:
            conn = driver.connect(**kwargs)
        except Exception as exc:
            raise ConnectionFailure(
                f"cannot connect to snowflake account {account!r} "
                f"database {config.database!r} as {config.user!r}: "
                f"{type(exc).__name__}: {exc}",
                connection=config.name,
            ) from exc
        try:
            conn.autocommit(False)
        except Exception:
            log.debug("could not disable autocommit", exc_info=True)
        return conn

    def ping(self, conn: Any) -> bool:
        """Return True if the session is still valid."""
        try:
            if hasattr(conn, "is_closed") and conn.is_closed():
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
        """Return ``USE SCHEMA <schema>``."""
        return f"USE SCHEMA {self.quote_identifier(schema)}"

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return a single-row ``MERGE`` binding each value exactly once.

        Snowflake allows ``SELECT`` without ``FROM``, so the source is a bare
        ``SELECT %s AS col, ...``. As on Oracle, values are bound once and
        referenced by alias in both branches, keeping the statement compatible
        with ``executemany``.
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
            f"MERGE INTO {target} tgt USING (SELECT {source_cols}) src "
            f"ON ({on_clause}) "
        )
        if updates:
            assignments = ", ".join(
                f"tgt.{self.quote_identifier(c)} = src.{self.quote_identifier(c)}"
                for c in updates
            )
            statement += f"WHEN MATCHED THEN UPDATE SET {assignments} "
        statement += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        return statement

    def copy_from(
        self,
        conn: Any,
        table: str,
        rows: Iterable[Sequence[Any]],
        columns: Sequence[str],
        **options: Any,
    ) -> int:
        """Load rows via a gzipped CSV, ``PUT`` to the table stage, ``COPY INTO``.

        Steps, in order:

        1. Write the batch to a local ``.csv.gz`` in a temporary directory.
        2. ``PUT file://... @%<table>`` - upload to the table's internal stage.
           ``AUTO_COMPRESS=FALSE`` because the file is already gzipped.
        3. ``COPY INTO <table> (cols) FROM @%<table>/<name> ... PURGE=TRUE`` -
           load and delete the staged file.

        The temporary file is removed on every path, including failures.

        Args:
            conn: Raw driver connection.
            table: Target table. Must be a plain table - the internal table
                stage ``@%table`` does not exist for views or transient
                external tables.
            rows: Tuples in ``columns`` order.
            columns: Column names.
            **options: ``on_error`` (default ``ABORT_STATEMENT``),
                ``null_marker`` (default empty string).

        Returns:
            Rows loaded, taken from the ``COPY`` result when available.
        """
        materialised = list(rows)
        if not materialised:
            return 0

        on_error = options.get("on_error", "ABORT_STATEMENT")
        null_marker = options.get("null_marker", "")
        stage_name = f"pydb_{uuid.uuid4().hex}.csv.gz"
        tmp_dir = tempfile.mkdtemp(prefix="pydbconnect_")
        local_path = os.path.join(tmp_dir, stage_name)

        try:
            with gzip.open(local_path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                for row in materialised:
                    writer.writerow([null_marker if v is None else v for v in row])

            quoted_table = self.quote_identifier(table)
            stage = f"@%{quoted_table}"
            cols = ", ".join(self.quote_identifier(c) for c in columns)
            posix_path = local_path.replace("\\", "/")

            cur = conn.cursor()
            try:
                cur.execute(
                    f"PUT 'file://{posix_path}' {stage} "
                    f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                cur.execute(
                    f"COPY INTO {quoted_table} ({cols}) FROM {stage}/{stage_name} "
                    f"FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                    f"COMPRESSION = GZIP NULL_IF = ('{null_marker}')) "
                    f"ON_ERROR = {on_error} PURGE = TRUE"
                )
                loaded = self._rows_loaded(cur)
            finally:
                cur.close()
        finally:
            for cleanup in (lambda: os.unlink(local_path), lambda: os.rmdir(tmp_dir)):
                try:
                    cleanup()
                except OSError:  # pragma: no cover
                    log.debug("could not clean up %s", tmp_dir)
        return loaded if loaded is not None else len(materialised)

    @staticmethod
    def _rows_loaded(cur: Any) -> Optional[int]:
        """Read ``rows_loaded`` out of a ``COPY INTO`` result set."""
        try:
            rows = cur.fetchall()
            names = [d[0].lower() for d in (cur.description or [])]
            if "rows_loaded" in names:
                index = names.index("rows_loaded")
                return sum(int(r[index] or 0) for r in rows)
        except Exception:
            log.debug("could not read rows_loaded from COPY result", exc_info=True)
        return None

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify by the connector's ``errno``/``sfqid`` error codes.

        ``390114 Authentication token has expired`` is the one that surprises
        people: a long-running job's session expires mid-flight, and a retry -
        which re-authenticates - fixes it. Compilation errors do not improve
        with repetition.
        """
        code = getattr(exc, "errno", None)
        code = str(code) if code is not None else self._error_code(exc)
        if code:
            code = str(code).zfill(6) if code.isdigit() and len(code) < 6 else str(code)
            if code in _RETRYABLE_CODES or code.lstrip("0") in {
                c.lstrip("0") for c in _RETRYABLE_CODES
            }:
                return True
            if code in _PERMANENT_CODES:
                return False
        name = type(exc).__name__
        if name in ("OperationalError", "InterfaceError", "RequestExceedMaxRetryError"):
            return True
        if name in ("ProgrammingError", "IntegrityError", "DataError", "DatabaseError"):
            message = str(exc).lower()
            # A long job whose session expired mid-flight: a retry re-authenticates.
            return "token" in message and "expired" in message
        return None
