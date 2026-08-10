"""The backend contract.

A *backend* is a thin adapter between this library and one driver. It owns the
five things that genuinely differ between databases and nothing else:

1. **How you connect** - :meth:`Backend.connect`.
2. **What a parameter placeholder looks like** - :attr:`Backend.placeholder_style`.
3. **How you quote an identifier** - :meth:`Backend.quote_identifier`.
4. **How you write an upsert** - :meth:`Backend.upsert_sql`.
5. **Which errors are worth retrying** - :meth:`Backend.classify_error`.

Everything else - pooling, transactions, chunking, redaction, retry - lives in
the shared layer and is written once. That is the whole design: a backend is
roughly 150 lines, and adding one does not require understanding the rest.

Writing a backend
-----------------

Subclass :class:`Backend`, set the class attributes, implement
:meth:`connect` and :meth:`ping`, and register it::

    from pydbconnect.backends.base import Backend
    from pydbconnect.registry import register_backend

    class DuckDBBackend(Backend):
        name = "duckdb"
        driver_module = "duckdb"
        install_extra = "duckdb"
        placeholder_style = "qmark"
        required_fields = ("database",)
        supports_copy = False

        def connect(self, config):
            duckdb = self.import_driver()
            return duckdb.connect(config.database)

        def ping(self, conn):
            conn.execute("select 1").fetchone()
            return True

    register_backend(DuckDBBackend)

Rules the shared layer relies on:

* :meth:`connect` returns a **DB-API 2.0 connection**. If your driver is not
  DB-API, return an adapter object that provides ``cursor()``, ``commit()``,
  ``rollback()`` and ``close()``, or override the SQL methods to raise
  :class:`~pydbconnect.exceptions.NotSupportedError` the way
  :mod:`pydbconnect.backends.adls` does.
* **Import your driver inside the method that needs it**, via
  :meth:`import_driver`. A top-level ``import psycopg`` would make this whole
  library uninstallable for someone who only uses MySQL.
* :meth:`classify_error` returns ``True`` (retryable), ``False`` (not) or
  ``None`` (no opinion - fall through to the generic rules). Prefer driver error
  *codes* over string matching; string matching is the fallback the generic
  classifier already does.
* Never put a credential in a log line, an exception message or a ``__repr__``.
  Pass ``SecretStr.reveal()`` straight into the driver call and nowhere else.

Placeholder styles
------------------

===============  =============  =====================================
``placeholder_style``  Renders  Backends
===============  =============  =====================================
``qmark``        ``?``          sqlite
``format``       ``%s``         mysql, postgres, snowflake
``numeric``      ``:1``         oracle
===============  =============  =====================================

All three bind positionally, so the shared bulk layer always passes tuples and
never has to care which one a backend uses.
"""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..exceptions import DriverNotInstalledError, NotSupportedError

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["Backend", "PLACEHOLDER_STYLES"]

#: Supported placeholder styles and the token each one renders.
PLACEHOLDER_STYLES: Dict[str, str] = {
    "qmark": "?",
    "format": "%s",
    "numeric": ":n",
}


class Backend(ABC):
    """Adapter for one database driver.

    Backends are stateless and shared between connections; the registry keeps
    one instance per name. Do not store per-connection state on ``self``.
    """

    # -- identity ----------------------------------------------------------- #

    #: Registry name, as it appears in ``backend:`` in the config file.
    name: str = ""

    #: Import path of the driver package, e.g. ``"psycopg"``. Used by
    #: :meth:`import_driver` and by the error message when it is missing.
    driver_module: str = ""

    #: The pip extra that installs the driver, e.g. ``"postgres"`` for
    #: ``pip install "pydb-connect[postgres]"``.
    install_extra: str = ""

    #: Default TCP port, or ``None`` for file-based and object stores.
    default_port: Optional[int] = None

    #: Config fields that must be set for this backend, checked by
    #: :meth:`~pydbconnect.config.ConnectionConfig.validate`.
    required_fields: Tuple[str, ...] = ()

    # -- capabilities ------------------------------------------------------- #

    #: Placeholder style; one of :data:`PLACEHOLDER_STYLES`.
    placeholder_style: str = "qmark"

    #: True when :meth:`copy_from` is implemented with a genuine bulk path.
    supports_copy: bool = False

    #: True when :meth:`upsert_sql` produces real merge semantics.
    supports_upsert: bool = True

    #: True when results can be streamed without buffering the whole set.
    supports_streaming: bool = True

    #: True when ``commit``/``rollback`` mean something.
    supports_transactions: bool = True

    #: Character used to quote identifiers.
    quote_char: str = '"'

    # -- driver loading ----------------------------------------------------- #

    def import_driver(self, module: Optional[str] = None, hint: str = "") -> Any:
        """Import the driver, or raise a message that says what to install.

        Args:
            module: Override the module to import; defaults to
                :attr:`driver_module`.
            hint: Extra guidance appended to the error, e.g. a note about
                system libraries the driver needs.

        Raises:
            DriverNotInstalledError: The module is not importable. The message
                names the pip extra, because ``ModuleNotFoundError: no module
                named 'psycopg'`` does not tell anyone which extra provides it.
        """
        target = module or self.driver_module
        if not target:  # pragma: no cover - programming error in a backend
            raise NotSupportedError(f"backend {self.name!r} declares no driver_module")
        try:
            return importlib.import_module(target)
        except ImportError as exc:
            raise DriverNotInstalledError(
                self.name, target, self.install_extra or self.name, hint
            ) from exc

    def driver_available(self) -> bool:
        """Return whether the driver can be imported, without raising.

        Used by ``pydb config list`` to show which backends are usable on this
        machine.
        """
        if not self.driver_module:
            return True
        try:
            importlib.import_module(self.driver_module)
        except ImportError:
            return False
        return True

    # -- connection lifecycle ----------------------------------------------- #

    @abstractmethod
    def connect(self, config: "ConnectionConfig") -> Any:
        """Open and return a raw DB-API connection.

        Args:
            config: The resolved configuration. Call
                ``config.resolve_password()`` to obtain a
                :class:`~pydbconnect.secrets.SecretStr` and pass
                ``.reveal()`` straight to the driver.

        Returns:
            A DB-API 2.0 connection, or an adapter that behaves like one.

        Raises:
            DriverNotInstalledError: The driver package is missing.
            ConnectionFailure: The server refused or the credentials failed.
        """

    @abstractmethod
    def ping(self, conn: Any) -> bool:
        """Return ``True`` if ``conn`` is still usable.

        Must be cheap - it runs on every pool checkout when ``pool.pre_ping``
        is on - and must never raise. Return ``False`` instead.
        """

    def close(self, conn: Any) -> None:
        """Close a raw connection. Overridable for non-DB-API adapters."""
        conn.close()

    def cursor(self, conn: Any, *, server_side: bool = False, name: str = "") -> Any:
        """Return a cursor.

        Args:
            server_side: Request a server-side (named) cursor so that
                :meth:`~pydbconnect.connection.Connection.stream` does not
                buffer the whole result set client-side. Backends that cannot
                do this ignore the flag; the shared layer still chunks with
                ``fetchmany`` so memory stays bounded either way.
            name: Suggested cursor name for backends that need one.
        """
        return conn.cursor()

    def on_connect(self, conn: Any, config: "ConnectionConfig") -> None:
        """Hook run once, immediately after a connection is opened.

        The default sets the session schema when :attr:`ConnectionConfig.schema`
        is configured and the backend defines :meth:`set_schema_sql`.
        """
        statement = self.set_schema_sql(config.schema) if config.schema else None
        if statement:
            cur = conn.cursor()
            try:
                cur.execute(statement)
            finally:
                cur.close()

    def set_schema_sql(self, schema: str) -> Optional[str]:
        """Return SQL that sets the session schema, or ``None`` if unsupported."""
        return None

    # -- SQL dialect -------------------------------------------------------- #

    def quote_identifier(self, identifier: str) -> str:
        """Quote a table or column name so it survives reserved words and case.

        Dotted names are quoted part by part, so ``analytics.fact_sales``
        becomes ``"analytics"."fact_sales"``. The quote character is doubled
        inside the identifier, which is the escaping rule in every SQL dialect
        this library targets.

        Raises:
            ValueError: The identifier is empty or contains a NUL byte.
        """
        if identifier is None or identifier == "":
            raise ValueError("identifier cannot be empty")
        if "\x00" in identifier:
            raise ValueError("identifier contains a NUL byte")
        q = self.quote_char
        parts = str(identifier).split(".")
        return ".".join(f"{q}{p.replace(q, q * 2)}{q}" for p in parts if p != "")

    def placeholder(self, index: int) -> str:
        """Return the placeholder for the 1-based parameter ``index``."""
        style = self.placeholder_style
        if style == "qmark":
            return "?"
        if style == "format":
            return "%s"
        if style == "numeric":
            return f":{index}"
        raise NotSupportedError(  # pragma: no cover - guarded by tests
            f"backend {self.name!r} declares unknown placeholder_style {style!r}"
        )

    def placeholders(self, count: int, offset: int = 0) -> str:
        """Return ``count`` comma-separated placeholders starting at ``offset``."""
        return ", ".join(self.placeholder(offset + i + 1) for i in range(count))

    def insert_sql(self, table: str, columns: Sequence[str]) -> str:
        """Return a parameterised ``INSERT`` for ``columns`` into ``table``."""
        cols = ", ".join(self.quote_identifier(c) for c in columns)
        return (
            f"INSERT INTO {self.quote_identifier(table)} ({cols}) "
            f"VALUES ({self.placeholders(len(columns))})"
        )

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return a parameterised insert-or-update statement.

        Args:
            table: Target table.
            columns: Every column being written, in the order values are bound.
            key_columns: Columns that identify an existing row.
            update_columns: Columns to overwrite on conflict; defaults to
                ``columns`` minus ``key_columns``.

        Returns:
            SQL binding exactly ``len(columns)`` parameters positionally, so
            the shared bulk layer can hand it straight to ``executemany``.
            Backends whose merge syntax needs the values twice (Oracle,
            Snowflake) must still bind them once and reference them by alias.

        Raises:
            NotSupportedError: The backend has no upsert.
        """
        raise NotSupportedError(
            f"backend {self.name!r} does not support upsert; "
            f"load into a staging table and merge with your own SQL"
        )

    def merge_update_columns(
        self, columns: Sequence[str], key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]],
    ) -> List[str]:
        """Resolve which columns an upsert should overwrite.

        Raises:
            ValueError: ``key_columns`` is empty, or names a column that is not
                being written - both of which would silently produce a plain
                insert or a broken statement.
        """
        if not key_columns:
            raise ValueError("upsert requires at least one key column")
        missing = [k for k in key_columns if k not in columns]
        if missing:
            raise ValueError(
                f"key column(s) {', '.join(missing)} are not among the inserted "
                f"columns {', '.join(columns)}"
            )
        if update_columns is None:
            update_columns = [c for c in columns if c not in key_columns]
        return list(update_columns)

    # -- bulk --------------------------------------------------------------- #

    def copy_from(
        self,
        conn: Any,
        table: str,
        rows: Iterable[Sequence[Any]],
        columns: Sequence[str],
        **options: Any,
    ) -> int:
        """Load ``rows`` using the backend's native bulk path.

        Only called when :attr:`supports_copy` is true.
        :func:`pydbconnect.bulk.copy_from` falls back to chunked
        ``executemany`` otherwise.

        Returns:
            Number of rows loaded.

        Raises:
            NotSupportedError: The backend has no native bulk path.
        """
        raise NotSupportedError(
            f"backend {self.name!r} has no native bulk copy path; "
            f"use bulk_insert, which batches with executemany"
        )

    # -- errors ------------------------------------------------------------- #

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Say whether ``exc`` is transient.

        Returns:
            ``True`` retryable, ``False`` permanent, ``None`` no opinion (the
            generic classifier in :mod:`pydbconnect.retry` then decides).

        Base implementation has no opinion. Override it with real driver error
        codes: string matching on messages is a last resort, not a design.
        """
        return None

    @staticmethod
    def _error_code(exc: BaseException) -> Optional[str]:
        """Extract a driver error code from the usual attribute names."""
        for attr in ("errno", "sqlstate", "pgcode", "code", "errorcode"):
            value = getattr(exc, attr, None)
            if value not in (None, ""):
                return str(value)
        args = getattr(exc, "args", ())
        if args and isinstance(args[0], int):
            return str(args[0])
        return None

    # -- introspection ------------------------------------------------------ #

    def describe(self) -> Dict[str, Any]:
        """Return a capability summary, used by ``pydb config list`` and docs."""
        return {
            "name": self.name,
            "driver": self.driver_module or "(stdlib)",
            "extra": self.install_extra or "(none)",
            "installed": self.driver_available(),
            "placeholder": PLACEHOLDER_STYLES.get(self.placeholder_style, "?"),
            "default_port": self.default_port,
            "copy": self.supports_copy,
            "upsert": self.supports_upsert,
            "streaming": self.supports_streaming,
            "transactions": self.supports_transactions,
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"
