"""Backend registry.

Backends are looked up by the name in ``backend:``. Built-ins are registered as
*module paths* and imported on first use, so importing :mod:`pydbconnect` does
not drag in six backend modules, and an unused backend can never break startup.

Third-party backends register themselves::

    from pydbconnect.registry import register_backend
    register_backend(MyBackend)          # uses MyBackend.name

The registry holds one instance per backend name. Backends are stateless, so
sharing them is safe and avoids re-running import machinery on every connect.
"""

from __future__ import annotations

import importlib
import threading
from typing import Any, Callable, Dict, List, Optional, Type, Union

from .backends.base import Backend
from .exceptions import BackendNotFoundError, ConfigurationError

__all__ = [
    "register_backend",
    "get_backend",
    "available_backends",
    "is_registered",
    "unregister_backend",
    "describe_backends",
    "BUILTIN_BACKENDS",
]

#: Built-in backends, mapped to ``module:ClassName`` and imported on demand.
BUILTIN_BACKENDS: Dict[str, str] = {
    "sqlite": "pydbconnect.backends.sqlite:SQLiteBackend",
    "mysql": "pydbconnect.backends.mysql:MySQLBackend",
    "mariadb": "pydbconnect.backends.mysql:MySQLBackend",
    "postgres": "pydbconnect.backends.postgres:PostgresBackend",
    "postgresql": "pydbconnect.backends.postgres:PostgresBackend",
    "oracle": "pydbconnect.backends.oracle:OracleBackend",
    "snowflake": "pydbconnect.backends.snowflake:SnowflakeBackend",
    "adls": "pydbconnect.backends.adls:ADLSBackend",
    "azure_blob": "pydbconnect.backends.adls:ADLSBackend",
}

_lock = threading.RLock()
_factories: Dict[str, Union[str, Type[Backend], Backend, Callable[[], Backend]]] = dict(
    BUILTIN_BACKENDS
)
_instances: Dict[str, Backend] = {}


def register_backend(
    backend: Union[Type[Backend], Backend, Callable[[], Backend], str],
    name: Optional[str] = None,
) -> str:
    """Register a backend under ``name``.

    Args:
        backend: A :class:`~pydbconnect.backends.base.Backend` subclass, an
            instance, a zero-argument factory, or a ``"module:Class"`` string
            for lazy import.
        name: Registry name. Defaults to the backend's ``name`` attribute.

    Returns:
        The name it was registered under.

    Raises:
        ConfigurationError: No name could be determined.
    """
    if name is None:
        candidate = getattr(backend, "name", None)
        if isinstance(candidate, str) and candidate:
            name = candidate
    if not name:
        raise ConfigurationError(
            "cannot register a backend without a name: pass name= or set a "
            "'name' class attribute on the backend"
        )
    key = name.lower()
    with _lock:
        _factories[key] = backend
        _instances.pop(key, None)
    return key


def unregister_backend(name: str) -> None:
    """Remove a backend from the registry. Mostly useful in tests."""
    key = name.lower()
    with _lock:
        _factories.pop(key, None)
        _instances.pop(key, None)


def is_registered(name: str) -> bool:
    """Return whether ``name`` is registered, without importing anything."""
    return bool(name) and name.lower() in _factories


def available_backends() -> List[str]:
    """Return every registered backend name, sorted."""
    with _lock:
        return sorted(_factories)


def get_backend(name: str) -> Backend:
    """Return the shared instance for ``name``, importing it on first use.

    Raises:
        BackendNotFoundError: ``name`` is not registered. The message lists the
            names that are, which turns a typo into a five-second fix.
        ConfigurationError: The backend is registered but its module or class
            cannot be loaded - a packaging problem, not a user error.
    """
    if not name:
        raise BackendNotFoundError("", available_backends())
    key = str(name).lower()
    with _lock:
        cached = _instances.get(key)
        if cached is not None:
            return cached
        factory = _factories.get(key)
    if factory is None:
        raise BackendNotFoundError(name, available_backends())

    instance = _instantiate(factory, key)
    if not isinstance(instance, Backend):
        raise ConfigurationError(
            f"backend {name!r} resolved to {type(instance).__name__}, "
            f"which is not a pydbconnect Backend"
        )
    if not instance.name:
        instance.name = key
    with _lock:
        _instances[key] = instance
    return instance


def _instantiate(factory: Any, key: str) -> Backend:
    """Turn a registry entry into a Backend instance."""
    if isinstance(factory, str):
        module_path, _, class_name = factory.partition(":")
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ConfigurationError(
                f"backend {key!r} maps to {factory!r} but {module_path} could not "
                f"be imported: {exc}"
            ) from exc
        try:
            factory = getattr(module, class_name)
        except AttributeError as exc:
            raise ConfigurationError(
                f"backend {key!r} maps to {factory!r} but {module_path} has no "
                f"attribute {class_name!r}"
            ) from exc
    if isinstance(factory, Backend):
        return factory
    if isinstance(factory, type):
        return factory()
    if callable(factory):
        return factory()
    raise ConfigurationError(  # pragma: no cover - defensive
        f"backend {key!r} is registered as {factory!r}, which is not constructible"
    )


def describe_backends() -> List[Dict[str, Any]]:
    """Return a capability row per backend, for ``pydb config list`` and docs.

    Backends whose module cannot be imported at all are reported with an
    ``error`` key rather than raising, so one broken backend does not hide the
    other eight.
    """
    rows: List[Dict[str, Any]] = []
    for name in available_backends():
        try:
            backend = get_backend(name)
        except Exception as exc:  # noqa: BLE001 - one bad backend must not hide the rest
            rows.append({"name": name, "error": f"{type(exc).__name__}: {exc}"})
            continue
        row = backend.describe()
        # Report the registry key, so aliases such as 'postgresql' and
        # 'azure_blob' are visibly distinct rows rather than duplicates.
        row["name"] = name
        if backend.name != name:
            row["alias_of"] = backend.name
        rows.append(row)
    return rows
