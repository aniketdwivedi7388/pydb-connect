"""Secret resolution and redaction.

The rule this module enforces is simple: **a credential enters the process from
exactly one place, is wrapped in a type that will not print itself, and is
scrubbed from anything that gets logged.**

A secret is referenced in configuration by a scheme-prefixed string:

===========================  ==================================================
Reference                    Meaning
===========================  ==================================================
``env:PGPASSWORD``           Read the ``PGPASSWORD`` environment variable.
``file:/run/secrets/db``     Read a mounted file (Kubernetes, Docker secrets).
``keyvault:vault/name``      Read from Azure Key Vault via ``DefaultAzureCredential``.
``literal:hunter2``          Use the value inline. **Warns loudly.**
===========================  ==================================================

A bare string with no recognised scheme is treated as ``env:``, because the
environment is the correct default and a typo should not silently become a
literal password.

Resolvers are pluggable::

    from pydbconnect.secrets import register_resolver, SecretStr

    def _from_ssm(ref: str) -> SecretStr:
        return SecretStr(boto3.client("ssm").get_parameter(
            Name=ref, WithDecryption=True)["Parameter"]["Value"], source="ssm")

    register_resolver("ssm", _from_ssm)

Redaction is a safety net, not the primary defence. Every :class:`SecretStr`
registers its value in a process-wide table; :func:`redact` scrubs those values
out of arbitrary text, and :class:`RedactingFilter` does the same for the
``logging`` module. Values shorter than
:data:`MIN_REDACTABLE_LENGTH` are not registered, because scrubbing the string
``"a"`` out of every log line would destroy the logs.
"""

from __future__ import annotations

import hmac
import logging
import os
import threading
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Set

from .exceptions import SecretError

__all__ = [
    "SecretStr",
    "resolve_secret",
    "register_resolver",
    "available_schemes",
    "redact",
    "redact_mapping",
    "register_secret_value",
    "RedactingFilter",
    "install_log_redaction",
    "MIN_REDACTABLE_LENGTH",
    "REDACTED",
]

#: What every redacted value renders as.
REDACTED = "***"

#: Values shorter than this are not added to the redaction table. Scrubbing very
#: short strings would mangle unrelated text far more than it would protect.
MIN_REDACTABLE_LENGTH = 4

_SENSITIVE_NAME_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "private_key",
    "credential",
    "sas",
    "connection_string",
    "conn_str",
    "authorization",
)

_registry_lock = threading.Lock()
_known_secret_values: Set[str] = set()


def register_secret_value(value: Optional[str]) -> None:
    """Add ``value`` to the process-wide redaction table.

    Called automatically by :class:`SecretStr`. Call it directly when a secret
    reaches you from somewhere this library does not control, e.g. a driver
    that hands back a refreshed token.

    Values that are empty, non-string or shorter than
    :data:`MIN_REDACTABLE_LENGTH` are ignored.
    """
    if not isinstance(value, str) or len(value) < MIN_REDACTABLE_LENGTH:
        return
    with _registry_lock:
        _known_secret_values.add(value)


def _known_values() -> Iterable[str]:
    with _registry_lock:
        # Longest first so that overlapping values redact completely.
        return sorted(_known_secret_values, key=len, reverse=True)


class SecretStr:
    """A string that refuses to render itself.

    ``str()``, ``repr()``, f-strings and ``%``-formatting all produce ``***``.
    The only way to obtain the underlying value is :meth:`reveal`, which is
    deliberately ugly to type and trivial to grep for in code review::

        >>> s = SecretStr("hunter2")
        >>> str(s), repr(s), f"{s}", "%s" % s
        ('***', 'SecretStr(***)', '***', '***')
        >>> s.reveal()
        'hunter2'

    Equality works against other :class:`SecretStr` objects and against plain
    strings, using a constant-time comparison so it cannot be used as a timing
    oracle.
    """

    __slots__ = ("_value", "source")

    def __init__(self, value: str, *, source: str = "unknown") -> None:
        if value is None:  # pragma: no cover - defensive
            raise SecretError("secret value cannot be None", source=source)
        self._value = str(value)
        #: Where the value came from, e.g. ``"env:PGPASSWORD"``. Safe to log.
        self.source = source
        register_secret_value(self._value)

    def reveal(self) -> str:
        """Return the underlying value.

        Call this as late as possible and never assign the result to a
        long-lived variable: hand it straight to the driver.
        """
        return self._value

    def __str__(self) -> str:
        return REDACTED

    def __repr__(self) -> str:
        return f"SecretStr({REDACTED})"

    def __format__(self, format_spec: str) -> str:
        # Without this, ``f"{secret:>20}"`` would fall through to object.__format__
        # for non-empty specs. It does not, but relying on that is unwise.
        return format(REDACTED, format_spec)

    def __bool__(self) -> bool:
        return bool(self._value)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SecretStr):
            return hmac.compare_digest(self._value, other._value)
        if isinstance(other, str):
            return hmac.compare_digest(self._value, other)
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        # Hash the redaction marker, not the value: a secret must not be
        # recoverable from a hash bucket in a heap dump.
        return hash(("pydbconnect.SecretStr", REDACTED))

    def __reduce__(self) -> Any:
        raise SecretError(
            "SecretStr refuses to be pickled; resolve the secret in the child "
            "process instead of shipping it across a process boundary"
        )


# --------------------------------------------------------------------------- #
# Resolvers
# --------------------------------------------------------------------------- #

Resolver = Callable[[str], SecretStr]
_resolvers: Dict[str, Resolver] = {}


def register_resolver(scheme: str, resolver: Resolver) -> None:
    """Register a resolver for ``scheme:`` references.

    Args:
        scheme: Prefix without the colon, e.g. ``"ssm"``.
        resolver: Callable taking the part after the colon and returning a
            :class:`SecretStr`.
    """
    if not scheme or ":" in scheme:
        raise ValueError(f"invalid resolver scheme: {scheme!r}")
    _resolvers[scheme.lower()] = resolver


def available_schemes() -> list:
    """Return the registered scheme names, sorted."""
    return sorted(_resolvers)


def _resolve_env(ref: str) -> SecretStr:
    """``env:VAR`` - read an environment variable."""
    name = ref.strip()
    if not name:
        raise SecretError("env: secret reference is missing a variable name")
    try:
        value = os.environ[name]
    except KeyError:
        raise SecretError(
            f"environment variable {name!r} is not set; "
            f"export it or point the secret at a different resolver",
            reference=f"env:{name}",
        ) from None
    return SecretStr(value, source=f"env:{name}")


def _resolve_file(ref: str) -> SecretStr:
    """``file:/path`` - read a mounted secret file.

    Trailing newlines are stripped, because every tool that writes a secret
    file adds one and no password ends in ``\\n``.
    """
    path = Path(ref.strip()).expanduser()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise SecretError(
            f"secret file {str(path)!r} does not exist; "
            f"check the volume mount or the path",
            reference=f"file:{path}",
        ) from None
    except OSError as exc:
        raise SecretError(
            f"cannot read secret file {str(path)!r}: {exc.strerror}",
            reference=f"file:{path}",
        ) from exc
    value = raw.rstrip("\r\n")
    if not value:
        raise SecretError(f"secret file {str(path)!r} is empty", reference=f"file:{path}")
    return SecretStr(value, source=f"file:{path}")


def _resolve_keyvault(ref: str) -> SecretStr:
    """``keyvault:<vault>/<secret>`` - read from Azure Key Vault.

    ``<vault>`` may be a bare vault name or a full ``https://...`` URL. The
    ``azure-identity`` and ``azure-keyvault-secrets`` packages are imported
    here, not at module import time, so this library stays installable without
    the Azure SDK.
    """
    ref = ref.strip()
    if "/" not in ref:
        raise SecretError(
            "keyvault: reference must be '<vault>/<secret-name>', "
            f"got {ref!r}",
            reference=f"keyvault:{ref}",
        )
    vault, _, secret_name = ref.partition("/")
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:  # pragma: no cover - requires azure absent+present
        raise SecretError(
            "the keyvault: resolver needs the Azure SDK. Install it with: "
            'pip install "pydb-connect[azure]"',
            reference=f"keyvault:{ref}",
        ) from exc

    url = vault if vault.startswith("http") else f"https://{vault}.vault.azure.net"
    try:
        client = SecretClient(vault_url=url, credential=DefaultAzureCredential())
        value = client.get_secret(secret_name).value
    except Exception as exc:
        raise SecretError(
            f"could not read secret {secret_name!r} from {url}: {type(exc).__name__}",
            reference=f"keyvault:{ref}",
        ) from exc
    if value is None:
        raise SecretError(f"key vault secret {secret_name!r} has no value")
    return SecretStr(value, source=f"keyvault:{vault}/{secret_name}")


def _resolve_literal(ref: str) -> SecretStr:
    """``literal:<value>`` - use the value inline. Emits a warning, always.

    This resolver exists so that a five-minute experiment does not require
    standing up a secret store, and for nothing else. A literal in a file that
    lives in version control is a credential in version control.
    """
    warnings.warn(
        "literal: secret used. A literal credential belongs nowhere near a "
        "repository, a container image or a CI log. Move it to env:, file: or "
        "keyvault: before this reaches anything shared.",
        UserWarning,
        stacklevel=3,
    )
    return SecretStr(ref, source="literal")


register_resolver("env", _resolve_env)
register_resolver("file", _resolve_file)
register_resolver("keyvault", _resolve_keyvault)
register_resolver("vault", _resolve_keyvault)  # alias used by ${vault:...}
register_resolver("literal", _resolve_literal)


def resolve_secret(reference: Optional[str], *, default_scheme: str = "env") -> Optional[SecretStr]:
    """Resolve a secret reference into a :class:`SecretStr`.

    Args:
        reference: Something like ``"env:PGPASSWORD"``. ``None`` or an empty
            string returns ``None`` - a connection without a password is a
            legitimate configuration, not an error.
        default_scheme: Scheme applied when the reference has no ``scheme:``
            prefix. Defaults to ``env``.

    Raises:
        SecretError: The scheme is unknown, or the underlying store could not
            produce a value.
    """
    if reference is None:
        return None
    reference = reference.strip()
    if not reference:
        return None

    scheme, sep, rest = reference.partition(":")
    scheme = scheme.lower()
    if not sep or scheme not in _resolvers:
        # A Windows path or a bare variable name: fall back rather than guess.
        if sep and scheme not in _resolvers and len(scheme) > 1:
            raise SecretError(
                f"unknown secret scheme {scheme!r} in {reference!r}; "
                f"available schemes: {', '.join(available_schemes())}",
                reference=reference,
            )
        scheme, rest = default_scheme, reference
    return _resolvers[scheme](rest)


# --------------------------------------------------------------------------- #
# Redaction
# --------------------------------------------------------------------------- #


def redact(text: Any, extra: Iterable[str] = ()) -> str:
    """Scrub every known secret value out of ``text``.

    Args:
        text: Anything; non-strings are passed through ``str()`` first.
        extra: Additional values to scrub for this call only.

    Returns:
        ``text`` with each known secret replaced by ``***``.

    This is a defence in depth measure. It cannot scrub a secret it has never
    seen, and it cannot un-send a log line. Do not treat it as permission to be
    careless with what you log.
    """
    if not isinstance(text, str):
        text = str(text)
    if not text:
        return text
    for value in list(extra) + list(_known_values()):
        if value and len(value) >= MIN_REDACTABLE_LENGTH and value in text:
            text = text.replace(value, REDACTED)
    return text


def redact_mapping(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``data`` safe to log.

    Values are redacted when the key looks sensitive (``password``, ``token``,
    ``secret``, ``sas``, ``connection_string``, ...) or when the value itself is
    a known secret. Nested dictionaries are handled recursively.
    """
    out: Dict[str, Any] = {}
    for key, value in data.items():
        lowered = str(key).lower().replace("-", "_")
        if isinstance(value, dict):
            out[key] = redact_mapping(value)
        elif isinstance(value, SecretStr):
            out[key] = REDACTED
        elif any(part in lowered for part in _SENSITIVE_NAME_PARTS):
            out[key] = REDACTED if value not in (None, "") else value
        elif isinstance(value, str):
            out[key] = redact(value)
        else:
            out[key] = value
    return out


class RedactingFilter(logging.Filter):
    """A ``logging`` filter that scrubs secrets from records.

    Attach it to a handler so that *everything* passing through that handler is
    scrubbed, including records emitted by driver libraries you do not control::

        handler.addFilter(RedactingFilter())

    The filter rewrites ``record.msg`` and ``record.args`` before formatting,
    which covers both eager (``log.info(f"...{pw}")``) and lazy
    (``log.info("%s", pw)``) styles.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = redact(record.msg)
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {k: _redact_arg(v) for k, v in record.args.items()}
                elif isinstance(record.args, tuple):
                    record.args = tuple(_redact_arg(a) for a in record.args)
        except Exception:  # noqa: BLE001  # pragma: no cover
            # A logging filter that raises takes the application down with it.
            return True
        return True


def _redact_arg(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, str):
        return redact(value)
    return value


def install_log_redaction(logger: Optional[logging.Logger] = None) -> RedactingFilter:
    """Attach a :class:`RedactingFilter` to ``logger`` and all of its handlers.

    Args:
        logger: Target logger; defaults to the root logger so that third-party
            driver logging is covered too.

    Returns:
        The filter that was installed, so it can be removed again in tests.
    """
    target = logger if logger is not None else logging.getLogger()
    filt = RedactingFilter()
    target.addFilter(filt)
    for handler in target.handlers:
        handler.addFilter(filt)
    return filt
