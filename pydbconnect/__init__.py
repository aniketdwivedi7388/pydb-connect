"""pydb-connect: config-driven database and object-store connectivity.

One interface, many backends. Credentials come from the environment or a secret
store, connections close on every path, bulk loads batch instead of looping, and
retries know the difference between a dropped TCP session and a typo in your
SQL.

Quickstart::

    from pydbconnect import connect, bulk_insert, chunked_read

    with connect("warehouse") as conn:                 # reads connections.yaml
        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER, name TEXT)")
        bulk_insert(conn, "t", [{"id": 1, "name": "ada"}], chunk_size=5000)
        for row in conn.query("SELECT * FROM t WHERE id = ?", (1,)):
            print(row["name"])

No configuration file? Configure entirely from the environment::

    export PYDB_WAREHOUSE_BACKEND=postgres
    export PYDB_WAREHOUSE_HOST=db.internal
    export PYDB_WAREHOUSE_DATABASE=analytics
    export PYDB_WAREHOUSE_USER=etl
    export PYDB_WAREHOUSE_PASSWORD=...        # becomes secret: env:PYDB_WAREHOUSE_PASSWORD

See ``docs/configuration.md`` for the precedence rules, ``docs/secrets.md`` for
credential handling, and ``docs/backends.md`` for the support matrix.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from . import exceptions
from .bulk import (
    BulkResult,
    bulk_insert,
    bulk_insert_dataframe,
    chunked,
    chunked_read,
    chunked_read_dataframes,
    copy_from,
    rows_from_csv,
    upsert,
)
from .config import (
    ConfigFile,
    ConnectionConfig,
    PoolSettings,
    RetrySettings,
    default_config_path,
    load_config,
)
from .connection import Connection, ConnectionPool, check_sql_safety
from .exceptions import (
    BackendNotFoundError,
    BulkLoadError,
    ConfigurationError,
    ConnectionFailure,
    DriverNotInstalledError,
    NotSupportedError,
    PoolTimeout,
    PyDBError,
    QueryError,
    SecretError,
    TransactionError,
    UnsafeSQLError,
)
from .registry import available_backends, describe_backends, get_backend, register_backend
from .retry import RetryPolicy, classify, retry, retry_call
from .secrets import (
    RedactingFilter,
    SecretStr,
    install_log_redaction,
    redact,
    register_resolver,
    resolve_secret,
)

__version__ = "1.0.0"
__author__ = "Aniket Dwivedi"
__license__ = "MIT"

__all__ = [
    "__version__",
    # entry points
    "connect",
    "pool",
    "Connection",
    "ConnectionPool",
    # configuration
    "ConfigFile",
    "ConnectionConfig",
    "PoolSettings",
    "RetrySettings",
    "load_config",
    "default_config_path",
    # secrets
    "SecretStr",
    "resolve_secret",
    "register_resolver",
    "redact",
    "RedactingFilter",
    "install_log_redaction",
    # retry
    "RetryPolicy",
    "retry",
    "retry_call",
    "classify",
    # bulk
    "BulkResult",
    "bulk_insert",
    "upsert",
    "copy_from",
    "chunked_read",
    "chunked",
    "rows_from_csv",
    "bulk_insert_dataframe",
    "chunked_read_dataframes",
    # backends
    "get_backend",
    "register_backend",
    "available_backends",
    "describe_backends",
    # utilities
    "check_sql_safety",
    # exceptions
    "exceptions",
    "PyDBError",
    "ConfigurationError",
    "SecretError",
    "BackendNotFoundError",
    "DriverNotInstalledError",
    "NotSupportedError",
    "ConnectionFailure",
    "QueryError",
    "TransactionError",
    "PoolTimeout",
    "UnsafeSQLError",
    "BulkLoadError",
]


def connect(
    name: Union[str, ConnectionConfig] = "default",
    *,
    config_path: Optional[Any] = None,
    profile: Optional[str] = None,
    **overrides: Any,
) -> Connection:
    """Open a connection by name.

    Args:
        name: A connection name to resolve from configuration, or a ready-made
            :class:`~pydbconnect.config.ConnectionConfig` to use directly.
        config_path: Explicit configuration file. Defaults to
            ``$PYDB_CONFIG_FILE`` and then the standard search path.
        profile: Profile to activate, e.g. ``prod``. Defaults to
            ``$PYDB_PROFILE``.
        **overrides: Values that beat every configuration layer, e.g.
            ``database="/tmp/scratch.db"``.

    Returns:
        An open :class:`~pydbconnect.connection.Connection`. Use it as a context
        manager so it closes on every path::

            with connect("warehouse") as conn:
                ...

    Raises:
        ConfigurationError: The name is unknown or the configuration is invalid.
        ConnectionFailure: The database refused the connection after retries.

    Example::

        with connect("warehouse", profile="prod") as conn:
            rows = conn.query("SELECT count(*) AS n FROM orders")
    """
    if isinstance(name, ConnectionConfig):
        return Connection.open(name)
    config = load_config(config_path, profile=profile).get(name, **overrides)
    return Connection.open(config)


def pool(
    name: Union[str, ConnectionConfig] = "default",
    *,
    config_path: Optional[Any] = None,
    profile: Optional[str] = None,
    size: Optional[int] = None,
    **overrides: Any,
) -> ConnectionPool:
    """Create a connection pool for a named connection.

    Args:
        name: Connection name or a ready-made configuration.
        config_path: Explicit configuration file.
        profile: Profile to activate.
        size: Maximum connections; defaults to ``pool.max_size`` from config.
        **overrides: Values that beat every configuration layer.

    Returns:
        A :class:`~pydbconnect.connection.ConnectionPool`. Close it when the
        process is done with it::

            with pool("warehouse", size=8) as p:
                with p.acquire() as conn:
                    conn.query("SELECT 1")
    """
    config = (
        name if isinstance(name, ConnectionConfig)
        else load_config(config_path, profile=profile).get(name, **overrides)
    )
    return ConnectionPool(config, size=size)
