"""SQLite backend - the reference implementation.

This is the backend to read first. It uses nothing but the standard library, so
the test suite, all four examples and the CLI run on a clean checkout with no
database, no container and no credentials. Everything the other backends do,
this one does in the smallest possible amount of code.

It is not a toy. SQLite is a genuinely good choice for local development, for
unit tests that need real SQL semantics rather than a mock, and for
single-writer datasets up to a few hundred gigabytes. It has ``ON CONFLICT ...
DO UPDATE`` (3.24+), real transactions, and a working ``executemany``.

Configuration::

    connections:
      local:
        backend: sqlite
        database: ./warehouse.db     # or ':memory:'
        options:
          timeout: 10.0              # seconds to wait on a locked database
          journal_mode: WAL          # strongly recommended for concurrency
          foreign_keys: true         # off by default in SQLite, which surprises people
          busy_timeout: 5000         # milliseconds
          uri: false                 # set true to use file: URIs
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence

from ..exceptions import ConnectionFailure
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["SQLiteBackend"]

#: Pragmas accepted in ``options`` and applied on connect, in this order.
_PRAGMAS = ("journal_mode", "synchronous", "foreign_keys", "busy_timeout", "cache_size", "temp_store")


class SQLiteBackend(Backend):
    """SQLite via the standard library ``sqlite3`` module."""

    name = "sqlite"
    driver_module = "sqlite3"
    install_extra = ""  # standard library
    default_port = None
    required_fields = ("database",)

    placeholder_style = "qmark"
    quote_char = '"'
    supports_copy = False       # no bulk loader; executemany is the fast path
    supports_upsert = True      # ON CONFLICT ... DO UPDATE, SQLite 3.24+
    supports_streaming = True
    supports_transactions = True

    def connect(self, config: "ConnectionConfig") -> sqlite3.Connection:
        """Open a SQLite connection.

        ``database`` is a filesystem path or ``:memory:``. The connection is
        opened with ``check_same_thread=False`` so it can be handed between
        threads by the pool; the pool guarantees only one thread holds a given
        connection at a time, which is the condition SQLite actually requires.
        """
        database = config.database or ":memory:"
        options = dict(config.options or {})

        if database not in (":memory:", "") and not str(database).startswith("file:"):
            parent = Path(database).expanduser().parent
            if str(parent) and not parent.exists():
                raise ConnectionFailure(
                    f"directory {str(parent)!r} does not exist for sqlite database "
                    f"{database!r}; create it or fix the path",
                    connection=config.name,
                )
            database = str(Path(database).expanduser())

        connect_kwargs: dict = {
            "timeout": float(options.pop("timeout", 5.0)),
            "check_same_thread": False,
            "isolation_level": options.pop("isolation_level", ""),
        }
        if _as_bool(options.pop("uri", False)):
            connect_kwargs["uri"] = True

        try:
            conn = sqlite3.connect(database, **connect_kwargs)
        except sqlite3.Error as exc:
            raise ConnectionFailure(
                f"cannot open sqlite database {database!r}: {exc}",
                connection=config.name,
            ) from exc

        self._apply_pragmas(conn, options)
        return conn

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection, options: dict) -> None:
        """Apply recognised pragmas from ``options``.

        Unknown option keys are ignored rather than passed to ``sqlite3.connect``,
        which would raise a confusing ``TypeError`` from deep inside the driver.
        """
        cur = conn.cursor()
        try:
            for pragma in _PRAGMAS:
                if pragma not in options:
                    continue
                value = options[pragma]
                if isinstance(value, bool):
                    value = "ON" if value else "OFF"
                elif pragma == "foreign_keys":
                    value = "ON" if _as_bool(value) else "OFF"
                # Pragma names come from the fixed _PRAGMAS tuple, never user input.
                cur.execute(f"PRAGMA {pragma} = {value}")
            conn.commit()
        finally:
            cur.close()

    def ping(self, conn: sqlite3.Connection) -> bool:
        """Return True if the connection still answers a trivial query."""
        try:
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
        """SQLite has no schemas; attached databases are a different concept."""
        return None

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return ``INSERT ... ON CONFLICT (keys) DO UPDATE SET ...``.

        Requires SQLite 3.24 (2018) or newer. When ``update_columns`` resolves
        to empty - every column is a key - this degenerates to ``DO NOTHING``,
        which is the correct semantic rather than an error.
        """
        updates = self.merge_update_columns(columns, key_columns, update_columns)
        insert = self.insert_sql(table, columns)
        conflict = ", ".join(self.quote_identifier(k) for k in key_columns)
        if not updates:
            return f"{insert} ON CONFLICT ({conflict}) DO NOTHING"
        assignments = ", ".join(
            f"{self.quote_identifier(c)} = excluded.{self.quote_identifier(c)}"
            for c in updates
        )
        return f"{insert} ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify SQLite errors.

        ``database is locked`` and ``database table is locked`` are the only
        genuinely transient SQLite failures - another writer holds the lock and
        backing off is exactly right. Integrity and programming errors are
        permanent by definition.
        """
        if isinstance(exc, (sqlite3.IntegrityError, sqlite3.ProgrammingError)):
            return False
        if isinstance(exc, sqlite3.NotSupportedError):
            return False
        if isinstance(exc, sqlite3.OperationalError):
            message = str(exc).lower()
            return "locked" in message or "busy" in message
        return None


def _as_bool(value: Any) -> bool:
    """Interpret YAML-ish truthiness without importing the config module."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "t"}
