"""Exception hierarchy for pydb-connect.

Every error raised by this library derives from :class:`PyDBError`, so callers
can wrap a whole block in ``except PyDBError`` without swallowing genuine bugs
such as ``TypeError`` or ``KeyError``.

Two properties matter more than the class names:

1. **Errors never leak secrets.** :meth:`PyDBError.__str__` pushes the message
   through :func:`pydbconnect.secrets.redact` on every formatting, so a driver
   that helpfully embeds the DSN (password included) in its error text still
   renders as ``***`` by the time the message reaches a log file or a stack
   trace.
2. **Errors carry structured context.** Anything passed as a keyword argument
   is stored on ``.context`` and appended to the message, which means you get
   ``connection='warehouse' key='port'`` instead of having to guess which of
   forty connections failed.

The split between *configuration* errors and *operational* errors is
load-bearing: the CLI exits ``2`` for the former and ``1`` for the latter, and
the retry layer refuses to retry the former no matter what a classifier says.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

__all__ = [
    "PyDBError",
    "ConfigurationError",
    "SecretError",
    "BackendError",
    "BackendNotFoundError",
    "DriverNotInstalledError",
    "NotSupportedError",
    "ConnectionFailure",
    "QueryError",
    "TransactionError",
    "PoolTimeout",
    "PoolClosedError",
    "UnsafeSQLError",
    "BulkLoadError",
]


class PyDBError(Exception):
    """Base class for every pydb-connect error.

    Args:
        message: Human-readable description. Written for the person reading a
            log at 03:00, not for the person who wrote the library.
        **context: Structured key/value pairs appended to the rendered message
            and available on ``.context`` for programmatic handling.
    """

    #: Suggested process exit code when this error reaches ``main()``.
    exit_code: int = 1

    def __init__(self, message: str = "", **context: Any) -> None:
        self.message = message
        self.context: Dict[str, Any] = {k: v for k, v in context.items() if v is not None}
        super().__init__(message)

    def _render(self) -> str:
        base = self.message or self.__class__.__name__
        if self.context:
            extras = " ".join(f"{k}={v!r}" for k, v in sorted(self.context.items()))
            return f"{base} ({extras})"
        return base

    def __str__(self) -> str:
        # Imported lazily: ``secrets`` imports this module at load time.
        from .secrets import redact

        return redact(self._render())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"


class ConfigurationError(PyDBError):
    """The configuration is wrong: a missing key, a bad type, an unknown name.

    Configuration errors are never retried and exit the CLI with code ``2``.
    They should always name the offending key and the connection it belongs to.
    """

    exit_code: int = 2


class SecretError(PyDBError):
    """A secret reference could not be resolved.

    Raised for unknown schemes, missing environment variables, unreadable
    secret files and Key Vault failures. The *reference* is included in the
    message; the *value* never is.
    """

    exit_code: int = 2


class BackendError(PyDBError):
    """Base class for backend-level problems."""


class BackendNotFoundError(BackendError, ConfigurationError):
    """The configured backend name is not registered.

    Inherits from :class:`ConfigurationError` because a typo in ``backend:`` is
    a configuration problem, and should exit ``2`` rather than ``1``.
    """

    exit_code: int = 2

    def __init__(self, name: str, available: Optional[Iterable[str]] = None) -> None:
        names = ", ".join(sorted(available or ()))
        message = f"unknown backend {name!r}"
        if names:
            message += f"; registered backends: {names}"
        super().__init__(message, backend=name)
        self.name = name


class DriverNotInstalledError(BackendError, ImportError):
    """The driver package for a backend is not installed.

    The message always names the exact pip command, because "no module named
    psycopg" tells the reader nothing about which extra provides it.
    """

    exit_code: int = 2

    def __init__(self, backend: str, module: str, extra: str, hint: str = "") -> None:
        message = (
            f"backend {backend!r} needs the {module!r} package, which is not installed. "
            f'Install it with: pip install "pydb-connect[{extra}]"'
        )
        if hint:
            message += f" ({hint})"
        super().__init__(message, backend=backend, module=module)
        self.backend = backend
        self.module = module
        self.extra = extra


class NotSupportedError(BackendError):
    """The backend cannot do what was asked.

    Object stores have no transactions; SQLite has no ``COPY``. Rather than
    silently degrading, the offending call raises this and the caller decides.
    """


class ConnectionFailure(PyDBError):
    """Opening, checking or closing a connection failed."""


class QueryError(PyDBError):
    """Executing a statement failed.

    The original driver exception is kept on ``.__cause__`` so backend-specific
    error codes stay reachable.
    """


class TransactionError(PyDBError):
    """A commit or rollback failed, or transaction state was misused."""


class PoolTimeout(ConnectionFailure):
    """No pooled connection became available within the checkout timeout.

    This nearly always means "you have more concurrent work than pool slots",
    not "the database is down". The message reports both numbers.
    """


class PoolClosedError(ConnectionFailure):
    """A connection was requested from a pool that has already been closed."""


class UnsafeSQLError(PyDBError):
    """A statement looks like it was built by string interpolation.

    Raised only when the SQL guard is set to ``error``; the default is to log a
    warning. See :func:`pydbconnect.connection.check_sql_safety`.
    """

    exit_code: int = 2


class BulkLoadError(PyDBError):
    """A bulk insert, upsert or copy failed.

    Carries ``rows_written`` so the caller knows how far the load got before it
    died, which is the difference between "resume" and "start over".
    """

    def __init__(self, message: str, rows_written: int = 0, **context: Any) -> None:
        super().__init__(message, rows_written=rows_written, **context)
        self.rows_written = rows_written
