"""Layered configuration resolution.

Precedence, highest wins:

1. **Explicit keyword arguments** passed to :func:`pydbconnect.connect` or
   :meth:`ConfigFile.get`. Code beats everything, because when you are
   debugging at 2am you need an override that no file can contradict.
2. **Environment variables** named ``PYDB_<CONNECTION>_<FIELD>``. This is how
   containers, CI and Kubernetes inject values, and how per-developer
   differences stay out of the shared file.
3. **The YAML file**, optionally split into profiles (``dev``/``test``/``prod``).
   This is the shared, committed, secret-free description of *shape*.
4. **Defaults** - the ``defaults:`` block in the file, then the library's own.

Nothing in this chain ever contains a password. Field ``secret`` holds a
*reference* (``env:PGPASSWORD``), resolved on demand by
:mod:`pydbconnect.secrets`.

Placeholders
------------
Any string value in the YAML may contain ``${env:VAR}``, ``${env:VAR:-default}``,
``${file:/path}`` or ``${vault:name/secret}``. They are resolved when the file
is loaded, so the rest of the library only ever sees final values. Results of
``vault:`` and ``file:`` placeholders are registered with the redaction table;
``env:`` results are not, because ``${env:REGION}`` is not a secret.

Example file::

    version: 1
    defaults:
      retry: {max_attempts: 5}
      pool:  {max_size: 8}
    profiles:
      dev:
        connections:
          warehouse: {backend: sqlite, database: ./dev.db}
      prod:
        connections:
          warehouse:
            backend: postgres
            host: ${env:PGHOST}
            database: analytics
            user: etl_writer
            secret: env:PGPASSWORD
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .exceptions import ConfigurationError
from .secrets import SecretStr, redact, register_secret_value, resolve_secret

__all__ = [
    "ConnectionConfig",
    "PoolSettings",
    "RetrySettings",
    "ConfigFile",
    "load_config",
    "default_config_path",
    "CONFIG_SEARCH_PATH",
    "ENV_PREFIX",
]

ENV_PREFIX = "PYDB"

#: Where :func:`load_config` looks when no path is given, in order.
CONFIG_SEARCH_PATH: Tuple[str, ...] = (
    "./connections.yaml",
    "./connections.yml",
    "./config/connections.yaml",
    "./config/connections.yml",
    "~/.config/pydbconnect/connections.yaml",
    "/etc/pydbconnect/connections.yaml",
)

_PLACEHOLDER = re.compile(r"\$\{(?P<scheme>env|file|vault|keyvault):(?P<body>[^}]*)\}")
_ENV_TOKEN = re.compile(r"[^A-Za-z0-9]+")

_TRUE = {"1", "true", "yes", "on", "y", "t"}
_FALSE = {"0", "false", "no", "off", "n", "f"}


# --------------------------------------------------------------------------- #
# Settings blocks
# --------------------------------------------------------------------------- #


@dataclass
class PoolSettings:
    """Connection pool tuning.

    Attributes:
        min_size: Connections opened eagerly when the pool is created.
        max_size: Hard ceiling on open connections. A pool that can grow without
            bound is not a pool, it is a denial-of-service tool aimed at your
            own database.
        timeout: Seconds to wait for a free connection before raising
            :class:`~pydbconnect.exceptions.PoolTimeout`.
        recycle: Close and reopen connections older than this many seconds.
            ``0`` disables recycling. Set it below any proxy or firewall idle
            timeout sitting between you and the database.
        pre_ping: Check liveness on checkout. Costs one round trip, saves one
            mysterious failure per deployment.
    """

    min_size: int = 0
    max_size: int = 5
    timeout: float = 30.0
    recycle: float = 0.0
    pre_ping: bool = True

    def validate(self, connection: str = "") -> None:
        """Raise :class:`ConfigurationError` if any value is nonsensical."""
        if self.max_size < 1:
            raise ConfigurationError(
                "pool.max_size must be at least 1", connection=connection, key="pool.max_size",
                value=self.max_size,
            )
        if self.min_size < 0:
            raise ConfigurationError(
                "pool.min_size cannot be negative", connection=connection, key="pool.min_size",
                value=self.min_size,
            )
        if self.min_size > self.max_size:
            raise ConfigurationError(
                f"pool.min_size ({self.min_size}) exceeds pool.max_size ({self.max_size})",
                connection=connection, key="pool.min_size",
            )
        if self.timeout <= 0:
            raise ConfigurationError(
                "pool.timeout must be greater than 0", connection=connection, key="pool.timeout",
                value=self.timeout,
            )
        if self.recycle < 0:
            raise ConfigurationError(
                "pool.recycle cannot be negative", connection=connection, key="pool.recycle",
                value=self.recycle,
            )


@dataclass
class RetrySettings:
    """Retry tuning, converted to a
    :class:`~pydbconnect.retry.RetryPolicy` when a connection is opened.

    Attributes:
        max_attempts: Total attempts including the first. ``1`` disables retry.
        initial_backoff: Seconds for the first wait.
        max_backoff: Ceiling on any single wait.
        multiplier: Growth factor between attempts.
        max_elapsed: Give up after this many seconds regardless of attempts.
            ``0`` means no wall-clock limit.
        jitter: ``full`` (recommended) or ``none``. Full jitter is what keeps a
            thundering herd from re-forming on every retry round.
    """

    max_attempts: int = 3
    initial_backoff: float = 0.2
    max_backoff: float = 10.0
    multiplier: float = 2.0
    max_elapsed: float = 60.0
    jitter: str = "full"

    def validate(self, connection: str = "") -> None:
        """Raise :class:`ConfigurationError` if any value is nonsensical."""
        if self.max_attempts < 1:
            raise ConfigurationError(
                "retry.max_attempts must be at least 1", connection=connection,
                key="retry.max_attempts", value=self.max_attempts,
            )
        if self.initial_backoff <= 0:
            raise ConfigurationError(
                "retry.initial_backoff must be greater than 0", connection=connection,
                key="retry.initial_backoff", value=self.initial_backoff,
            )
        if self.max_backoff < self.initial_backoff:
            raise ConfigurationError(
                f"retry.max_backoff ({self.max_backoff}) is below "
                f"retry.initial_backoff ({self.initial_backoff})",
                connection=connection, key="retry.max_backoff",
            )
        if self.multiplier < 1:
            raise ConfigurationError(
                "retry.multiplier must be at least 1", connection=connection,
                key="retry.multiplier", value=self.multiplier,
            )
        if self.max_elapsed < 0:
            raise ConfigurationError(
                "retry.max_elapsed cannot be negative", connection=connection,
                key="retry.max_elapsed", value=self.max_elapsed,
            )
        if self.jitter not in ("full", "none"):
            raise ConfigurationError(
                f"retry.jitter must be 'full' or 'none', got {self.jitter!r}",
                connection=connection, key="retry.jitter",
            )


# --------------------------------------------------------------------------- #
# Connection configuration
# --------------------------------------------------------------------------- #


@dataclass
class ConnectionConfig:
    """Everything needed to open one connection, and nothing secret.

    Attributes:
        name: Logical connection name, e.g. ``warehouse``. Used in env var
            lookups and in every log line and error message.
        backend: Registered backend name, e.g. ``sqlite``, ``postgres``.
        host: Server host. Unused by ``sqlite``.
        port: Server port. Falls back to the backend default when ``None``.
        database: Database name, or file path for ``sqlite``, or container name
            for ``adls``.
        schema: Default schema, where the backend has the concept.
        user: Login name.
        secret: A *reference* to the password, e.g. ``env:PGPASSWORD``. Never
            the password itself.
        options: Backend-specific extras passed to the driver, e.g.
            ``{"sslmode": "require"}`` or ``{"warehouse": "LOAD_WH"}``.
        pool: :class:`PoolSettings`.
        retry: :class:`RetrySettings`.
        profile: Profile this configuration was resolved from, for diagnostics.
        sql_guard: ``warn`` (default), ``error`` or ``off`` - what to do when a
            statement looks string-interpolated.
    """

    name: str
    backend: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    secret: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    pool: PoolSettings = field(default_factory=PoolSettings)
    retry: RetrySettings = field(default_factory=RetrySettings)
    profile: Optional[str] = None
    sql_guard: str = "warn"

    # -- construction ------------------------------------------------------- #

    @classmethod
    def from_dict(cls, name: str, data: Mapping[str, Any], *, profile: Optional[str] = None) -> "ConnectionConfig":
        """Build a config from a plain mapping, validating unknown keys.

        Raises:
            ConfigurationError: An unknown top-level key is present, or a value
                has the wrong type. The offending key is always named.
        """
        data = dict(data or {})
        known = {f.name for f in fields(cls)}
        # 'password' is rejected explicitly: it is the mistake this library exists to stop.
        if "password" in data:
            raise ConfigurationError(
                "'password' is not a valid key - put a reference in 'secret' instead, "
                "e.g. secret: env:PGPASSWORD. Storing a password in the config file is "
                "the exact failure mode this library is designed to prevent",
                connection=name, key="password",
            )
        unknown = sorted(set(data) - known)
        if unknown:
            raise ConfigurationError(
                f"unknown configuration key(s): {', '.join(unknown)}; "
                f"valid keys are: {', '.join(sorted(known - {'name', 'profile'}))}",
                connection=name, key=unknown[0],
            )

        pool_data = data.pop("pool", {}) or {}
        retry_data = data.pop("retry", {}) or {}
        options = data.pop("options", {}) or {}
        if not isinstance(options, Mapping):
            raise ConfigurationError(
                f"'options' must be a mapping, got {type(options).__name__}",
                connection=name, key="options",
            )

        backend = data.pop("backend", None)
        if not backend:
            raise ConfigurationError(
                "'backend' is required; set it to one of the registered backends",
                connection=name, key="backend",
            )

        cfg = cls(
            name=name,
            backend=str(backend),
            host=_opt_str(data.pop("host", None), name, "host"),
            port=_opt_int(data.pop("port", None), name, "port"),
            database=_opt_str(data.pop("database", None), name, "database"),
            schema=_opt_str(data.pop("schema", None), name, "schema"),
            user=_opt_str(data.pop("user", None), name, "user"),
            secret=_opt_str(data.pop("secret", None), name, "secret"),
            options=dict(options),
            pool=_build_block(PoolSettings, pool_data, name, "pool"),
            retry=_build_block(RetrySettings, retry_data, name, "retry"),
            profile=profile if profile is not None else data.pop("profile", None),
            sql_guard=str(data.pop("sql_guard", "warn")),
        )
        data.pop("profile", None)
        return cfg

    # -- validation --------------------------------------------------------- #

    def validate(self, *, check_backend: bool = True) -> "ConnectionConfig":
        """Validate this configuration, naming any offending key.

        Args:
            check_backend: Also confirm the backend is registered and that the
                backend's own required fields are present. Turn this off to
                validate shape without touching the registry.

        Returns:
            ``self``, so calls can be chained.

        Raises:
            ConfigurationError: With ``connection`` and ``key`` in the context.
        """
        if not self.name or not str(self.name).strip():
            raise ConfigurationError("connection name cannot be empty", key="name")
        if not self.backend or not str(self.backend).strip():
            raise ConfigurationError(
                "'backend' is required", connection=self.name, key="backend"
            )
        if self.port is not None and not (0 < int(self.port) < 65536):
            raise ConfigurationError(
                f"port must be between 1 and 65535, got {self.port}",
                connection=self.name, key="port",
            )
        if self.sql_guard not in ("warn", "error", "off"):
            raise ConfigurationError(
                f"sql_guard must be 'warn', 'error' or 'off', got {self.sql_guard!r}",
                connection=self.name, key="sql_guard",
            )
        if not isinstance(self.options, dict):
            raise ConfigurationError(
                "'options' must be a mapping", connection=self.name, key="options"
            )
        self.pool.validate(self.name)
        self.retry.validate(self.name)

        if self.secret:
            from .secrets import available_schemes

            scheme, sep, _ = str(self.secret).partition(":")
            if sep and scheme.lower() not in available_schemes():
                raise ConfigurationError(
                    f"secret reference uses unknown scheme {scheme!r}; "
                    f"available: {', '.join(available_schemes())}",
                    connection=self.name, key="secret",
                )

        if check_backend:
            from .registry import get_backend

            backend = get_backend(self.backend)  # raises BackendNotFoundError
            for required in backend.required_fields:
                if not getattr(self, required, None):
                    raise ConfigurationError(
                        f"backend {self.backend!r} requires '{required}' to be set",
                        connection=self.name, key=required,
                    )
        return self

    # -- derived values ----------------------------------------------------- #

    def resolve_password(self) -> Optional[SecretStr]:
        """Resolve :attr:`secret` into a :class:`SecretStr`, or ``None``.

        Resolution happens here and nowhere else, so a process that never opens
        a connection never touches the secret store.
        """
        return resolve_secret(self.secret)

    def effective_port(self, default: Optional[int] = None) -> Optional[int]:
        """Return :attr:`port` if set, otherwise ``default``."""
        return self.port if self.port is not None else default

    def to_dict(self, *, redacted: bool = True) -> Dict[str, Any]:
        """Return a plain dict of this configuration.

        Args:
            redacted: When true (the default), option values whose keys look
                sensitive are replaced with ``***`` and every string is passed
                through :func:`~pydbconnect.secrets.redact`.
        """
        data = asdict(self)
        if redacted:
            from .secrets import redact_mapping

            data["options"] = redact_mapping(data.get("options", {}))
            # The reference itself is safe to print; a literal: value is not.
            if data.get("secret") and str(data["secret"]).lower().startswith("literal:"):
                data["secret"] = "literal:***"
        return data

    def __repr__(self) -> str:
        from .secrets import redact_mapping

        opts = redact_mapping(self.options) if self.options else {}
        secret = self.secret
        if secret and str(secret).lower().startswith("literal:"):
            secret = "literal:***"
        body = (
            f"name={self.name!r} backend={self.backend!r} host={self.host!r} "
            f"port={self.port!r} database={self.database!r} schema={self.schema!r} "
            f"user={self.user!r} secret={secret!r} options={opts!r} "
            f"profile={self.profile!r}"
        )
        return redact(f"ConnectionConfig({body})")


def _opt_str(value: Any, connection: str, key: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        raise ConfigurationError(
            f"'{key}' must be a scalar, got {type(value).__name__}",
            connection=connection, key=key,
        )
    return str(value)


def _opt_int(value: Any, connection: str, key: str) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigurationError(
            f"'{key}' must be an integer, got {value!r}",
            connection=connection, key=key,
        ) from None


def _build_block(cls: type, data: Any, connection: str, key: str) -> Any:
    """Build a ``PoolSettings``/``RetrySettings`` from a mapping, checking keys."""
    if isinstance(data, cls):
        return data
    if not isinstance(data, Mapping):
        raise ConfigurationError(
            f"'{key}' must be a mapping, got {type(data).__name__}",
            connection=connection, key=key,
        )
    known = {f.name: f for f in fields(cls)}
    unknown = sorted(set(data) - set(known))
    if unknown:
        raise ConfigurationError(
            f"unknown key(s) under '{key}': {', '.join(unknown)}; "
            f"valid keys are: {', '.join(sorted(known))}",
            connection=connection, key=f"{key}.{unknown[0]}",
        )
    kwargs: Dict[str, Any] = {}
    for name, value in data.items():
        target = known[name].type
        kwargs[name] = _coerce(value, target, connection, f"{key}.{name}")
    return cls(**kwargs)


def _coerce(value: Any, target: Any, connection: str, key: str) -> Any:
    """Coerce a scalar to the annotated type of a settings field."""
    target = str(target)
    try:
        if "bool" in target:
            return _to_bool(value)
        if "int" in target:
            return int(value)
        if "float" in target:
            return float(value)
    except (TypeError, ValueError):
        raise ConfigurationError(
            f"'{key}' expects {target}, got {value!r}", connection=connection, key=key
        ) from None
    return str(value) if value is not None else value


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"cannot interpret {value!r} as a boolean")


# --------------------------------------------------------------------------- #
# Placeholders
# --------------------------------------------------------------------------- #


def expand_placeholders(value: Any, *, strict: bool = True) -> Any:
    """Recursively expand ``${scheme:body}`` placeholders in loaded YAML.

    Supported schemes are ``env``, ``file``, ``vault`` and ``keyvault``. An
    ``env`` placeholder may carry a default with ``${env:VAR:-fallback}``.

    Args:
        value: A string, mapping or sequence taken from the parsed YAML.
        strict: Raise when a placeholder cannot be resolved. When false the
            placeholder is left in place, which is useful for
            ``pydb config list`` on a machine that lacks production credentials.

    Raises:
        ConfigurationError: ``strict`` is set and a placeholder is unresolvable.
    """
    if isinstance(value, str):
        return _expand_string(value, strict=strict)
    if isinstance(value, Mapping):
        return {k: expand_placeholders(v, strict=strict) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [expand_placeholders(v, strict=strict) for v in value]
    return value


def _expand_string(text: str, *, strict: bool) -> str:
    def replace(match: "re.Match[str]") -> str:
        scheme = match.group("scheme")
        body = match.group("body")
        if scheme == "env":
            name, sep, fallback = body.partition(":-")
            found = os.environ.get(name.strip())
            if found is not None:
                return found
            if sep:
                return fallback
            if strict:
                raise ConfigurationError(
                    f"placeholder ${{env:{name}}} could not be resolved: "
                    f"environment variable {name.strip()!r} is not set. "
                    f"Use ${{env:{name.strip()}:-default}} if it is optional",
                    key=name.strip(),
                )
            return match.group(0)
        # file:/vault:/keyvault: go through the secret resolvers and are
        # registered for redaction, because their results are credentials.
        from .exceptions import SecretError

        try:
            secret = resolve_secret(f"{scheme}:{body}")
        except SecretError as exc:
            if strict:
                raise ConfigurationError(
                    f"placeholder ${{{scheme}:{body}}} could not be resolved: {exc}",
                    key=body,
                ) from exc
            return match.group(0)
        if secret is None:
            return match.group(0)
        revealed = secret.reveal()
        register_secret_value(revealed)
        return revealed

    return _PLACEHOLDER.sub(replace, text)


# --------------------------------------------------------------------------- #
# The file
# --------------------------------------------------------------------------- #


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge ``override`` onto ``base``, recursing into nested mappings."""
    out: Dict[str, Any] = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def default_config_path() -> Optional[Path]:
    """Return the first existing path in :data:`CONFIG_SEARCH_PATH`, or ``None``."""
    env_path = os.environ.get(f"{ENV_PREFIX}_CONFIG_FILE") or os.environ.get(f"{ENV_PREFIX}_CONFIG")
    if env_path:
        return Path(env_path).expanduser()
    for candidate in CONFIG_SEARCH_PATH:
        path = Path(candidate).expanduser()
        if path.is_file():
            return path
    return None


class ConfigFile:
    """A parsed connections file, plus the environment layered on top.

    Construct with :func:`load_config` rather than directly.

    Attributes:
        path: Where the file came from, or ``None`` for env-only configuration.
        profile: Active profile name.
        raw: The merged mapping for the active profile.
    """

    def __init__(
        self,
        data: Optional[Mapping[str, Any]] = None,
        *,
        path: Optional[Path] = None,
        profile: Optional[str] = None,
    ) -> None:
        data = dict(data or {})
        self.path = path
        self._document = data
        # Precedence, as documented: explicit argument, then the environment,
        # then the file's own default. The environment must beat the file, or
        # PYDB_PROFILE=prod would silently keep using the dev profile.
        self.profile = (
            profile
            or os.environ.get(f"{ENV_PREFIX}_PROFILE")
            or data.get("default_profile")
        )

        profiles = data.get("profiles") or {}
        if self.profile and profiles and self.profile not in profiles:
            raise ConfigurationError(
                f"profile {self.profile!r} is not defined in {path or '<no file>'}; "
                f"available profiles: {', '.join(sorted(profiles)) or '(none)'}",
                key="profiles",
            )

        shared = data.get("connections") or {}
        profile_block = (profiles.get(self.profile) or {}) if self.profile else {}
        profile_conns = profile_block.get("connections") or {}
        self._defaults = _deep_merge(
            data.get("defaults") or {}, profile_block.get("defaults") or {}
        )
        merged: Dict[str, Any] = {}
        for conn_name in list(shared) + [n for n in profile_conns if n not in shared]:
            merged[conn_name] = _deep_merge(
                shared.get(conn_name) or {}, profile_conns.get(conn_name) or {}
            )
        self.raw = merged

    # -- introspection ------------------------------------------------------ #

    def names(self) -> List[str]:
        """Return connection names defined in the file, sorted."""
        return sorted(self.raw)

    def profiles(self) -> List[str]:
        """Return profile names defined in the file, sorted."""
        return sorted(self._document.get("profiles") or {})

    def __contains__(self, name: str) -> bool:
        return name in self.raw

    def __repr__(self) -> str:
        return (
            f"ConfigFile(path={str(self.path)!r} profile={self.profile!r} "
            f"connections={len(self.raw)})"
        )

    # -- resolution --------------------------------------------------------- #

    def get(self, name: str, *, validate: bool = True, **overrides: Any) -> ConnectionConfig:
        """Resolve one connection through the full precedence chain.

        Args:
            name: Connection name.
            validate: Run :meth:`ConnectionConfig.validate` before returning.
            **overrides: Explicit values that beat every other layer. Nested
                blocks accept either a mapping (``pool={"max_size": 2}``) or
                dotted keys are *not* supported - use the mapping form.

        Raises:
            ConfigurationError: The name is unknown in every layer, or the
                resulting configuration is invalid.
        """
        env_layer = _env_layer(name)
        file_layer = self.raw.get(name)

        if file_layer is None and not env_layer and not overrides:
            known = ", ".join(self.names()) or "(none)"
            where = f" in {self.path}" if self.path else ""
            raise ConfigurationError(
                f"no connection named {name!r}{where}; defined connections: {known}. "
                f"Set {_env_key(name, 'BACKEND')} to configure it from the environment instead",
                connection=name,
            )

        layered = _deep_merge(self._defaults, file_layer or {})
        layered = _deep_merge(layered, env_layer)
        layered = _deep_merge(layered, _normalise_overrides(overrides))

        config = ConnectionConfig.from_dict(name, layered, profile=self.profile)
        if validate:
            config.validate()
        return config

    def all(self, *, validate: bool = True) -> List[ConnectionConfig]:
        """Resolve every connection in the file.

        Used by ``pydb config validate``, which is the cheapest possible way to
        find out that production is misconfigured before a job does.
        """
        return [self.get(name, validate=validate) for name in self.names()]


def _env_key(name: str, suffix: str) -> str:
    token = _ENV_TOKEN.sub("_", str(name)).strip("_").upper()
    return f"{ENV_PREFIX}_{token}_{suffix}"


#: Environment variable suffix -> path into the configuration mapping.
_ENV_FIELDS: Dict[str, Tuple[str, ...]] = {
    "BACKEND": ("backend",),
    "HOST": ("host",),
    "PORT": ("port",),
    "DATABASE": ("database",),
    "SCHEMA": ("schema",),
    "USER": ("user",),
    "SECRET": ("secret",),
    "SQL_GUARD": ("sql_guard",),
    "POOL_MIN_SIZE": ("pool", "min_size"),
    "POOL_MAX_SIZE": ("pool", "max_size"),
    "POOL_TIMEOUT": ("pool", "timeout"),
    "POOL_RECYCLE": ("pool", "recycle"),
    "POOL_PRE_PING": ("pool", "pre_ping"),
    "RETRY_MAX_ATTEMPTS": ("retry", "max_attempts"),
    "RETRY_INITIAL_BACKOFF": ("retry", "initial_backoff"),
    "RETRY_MAX_BACKOFF": ("retry", "max_backoff"),
    "RETRY_MULTIPLIER": ("retry", "multiplier"),
    "RETRY_MAX_ELAPSED": ("retry", "max_elapsed"),
    "RETRY_JITTER": ("retry", "jitter"),
}


def _env_layer(name: str) -> Dict[str, Any]:
    """Build the environment layer for one connection.

    Recognised variables:

    * ``PYDB_<CONN>_<FIELD>`` for every field in :data:`_ENV_FIELDS`.
    * ``PYDB_<CONN>_PASSWORD`` - sets ``secret`` to a *reference* to that same
      variable, so the value is read through the normal resolver and never
      copied into the configuration object.
    * ``PYDB_<CONN>_OPTIONS`` - a JSON object merged into ``options``.
    * ``PYDB_<CONN>_OPTION_<KEY>`` - a single option, lower-cased key.
    """
    layer: Dict[str, Any] = {}
    prefix = _env_key(name, "")

    for suffix, path in _ENV_FIELDS.items():
        raw = os.environ.get(prefix + suffix)
        if raw is None:
            continue
        cursor = layer
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = raw

    password_key = prefix + "PASSWORD"
    if password_key in os.environ and "secret" not in layer:
        register_secret_value(os.environ[password_key])
        layer["secret"] = f"env:{password_key}"

    options: Dict[str, Any] = {}
    raw_options = os.environ.get(prefix + "OPTIONS")
    if raw_options:
        try:
            parsed = json.loads(raw_options)
        except ValueError as exc:
            raise ConfigurationError(
                f"{prefix}OPTIONS must be a JSON object: {exc}",
                connection=name, key="options",
            ) from None
        if not isinstance(parsed, dict):
            raise ConfigurationError(
                f"{prefix}OPTIONS must be a JSON object, got {type(parsed).__name__}",
                connection=name, key="options",
            )
        options.update(parsed)

    option_prefix = prefix + "OPTION_"
    for key, value in os.environ.items():
        if key.startswith(option_prefix) and len(key) > len(option_prefix):
            options[key[len(option_prefix):].lower()] = value
    if options:
        layer["options"] = options
    return layer


def _normalise_overrides(overrides: Mapping[str, Any]) -> Dict[str, Any]:
    """Drop ``None`` values so an unset keyword does not erase a file value."""
    return {k: v for k, v in overrides.items() if v is not None}


def load_config(
    path: Optional[Any] = None,
    *,
    profile: Optional[str] = None,
    strict_placeholders: bool = True,
    required: bool = False,
) -> ConfigFile:
    """Load a connections file.

    Args:
        path: Explicit path. When ``None``, ``PYDB_CONFIG_FILE`` is consulted,
            then :data:`CONFIG_SEARCH_PATH`. When nothing is found an empty
            :class:`ConfigFile` is returned so that pure-environment
            configuration keeps working.
        profile: Profile to activate; falls back to ``PYDB_PROFILE`` and then to
            ``default_profile`` in the file.
        strict_placeholders: Raise when a ``${...}`` placeholder cannot be
            resolved.
        required: Raise if no file is found, instead of returning an empty one.

    Raises:
        ConfigurationError: The file is missing (when ``required``), unreadable,
            not valid YAML, not a mapping, or contains an unresolvable
            placeholder.
    """
    resolved = Path(path).expanduser() if path else default_config_path()

    if resolved is None:
        if required:
            raise ConfigurationError(
                "no configuration file found. Looked at $PYDB_CONFIG_FILE and: "
                + ", ".join(CONFIG_SEARCH_PATH)
            )
        return ConfigFile({}, path=None, profile=profile)

    if not resolved.is_file():
        raise ConfigurationError(f"configuration file not found: {resolved}", key="path")

    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigurationError(
            f"cannot read configuration file {resolved}: {exc.strerror}", key="path"
        ) from exc

    data = _parse_yaml(text, resolved)
    if data is None:
        data = {}
    if not isinstance(data, Mapping):
        raise ConfigurationError(
            f"{resolved} must contain a mapping at the top level, "
            f"got {type(data).__name__}"
        )

    data = expand_placeholders(dict(data), strict=strict_placeholders)
    return ConfigFile(data, path=resolved, profile=profile)


def _parse_yaml(text: str, source: Path) -> Any:
    """Parse YAML, or JSON when PyYAML is not installed.

    PyYAML is an optional dependency. Without it, ``.json`` files still work and
    the error message says exactly what to install.
    """
    try:
        import yaml
    except ImportError:
        try:
            return json.loads(text)
        except ValueError as exc:
            raise ConfigurationError(
                f"cannot parse {source}: PyYAML is not installed and the file is not "
                f"valid JSON ({exc}). Install it with: pip install pyyaml"
            ) from None
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"invalid YAML in {source}: {exc}") from None
