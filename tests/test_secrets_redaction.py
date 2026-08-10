"""Secrets: resolution, the SecretStr type, and redaction.

The central assertion of this file is negative: a password must not appear in a
``repr``, in a log record, in an exception message or in anything the CLI
prints. Those are the four places credentials actually leak from in practice.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest

from pydbconnect.config import ConnectionConfig
from pydbconnect.exceptions import ConfigurationError, SecretError
from pydbconnect.secrets import (
    RedactingFilter,
    SecretStr,
    available_schemes,
    redact,
    redact_mapping,
    register_resolver,
    resolve_secret,
)

PASSWORD = "correct-horse-battery-staple"


# --------------------------------------------------------------------------- #
# SecretStr
# --------------------------------------------------------------------------- #


def test_secretstr_never_renders_its_value() -> None:
    """str, repr, f-string and %-formatting all produce the mask."""
    secret = SecretStr(PASSWORD)
    assert str(secret) == "***"
    assert PASSWORD not in repr(secret)
    assert PASSWORD not in f"{secret}"
    assert PASSWORD not in f"{secret!r}"
    assert PASSWORD not in f"{secret:>40}"     # __format__ with a spec
    assert PASSWORD not in "%s" % (secret,)  # noqa: UP031 - %-formatting is under test
    assert PASSWORD not in "{}".format(secret)  # noqa: UP032 - .format() is under test
    assert PASSWORD not in str([secret])       # container repr
    assert PASSWORD not in str({"pw": secret})


def test_reveal_is_the_only_way_out() -> None:
    assert SecretStr(PASSWORD).reveal() == PASSWORD


def test_secretstr_compares_without_revealing() -> None:
    assert SecretStr(PASSWORD) == SecretStr(PASSWORD)
    assert SecretStr(PASSWORD) == PASSWORD
    assert SecretStr(PASSWORD) != "something else"
    assert SecretStr(PASSWORD) != 17


def test_secretstr_hash_does_not_encode_the_value() -> None:
    """Two different secrets hash alike: a heap dump reveals nothing."""
    assert hash(SecretStr("aaaa")) == hash(SecretStr("bbbb"))


def test_secretstr_refuses_to_pickle() -> None:
    """Shipping a credential across a process boundary is not a feature."""
    import pickle

    with pytest.raises(SecretError, match="pickled"):
        pickle.dumps(SecretStr(PASSWORD))


def test_empty_secret_is_falsey() -> None:
    assert not SecretStr("")
    assert SecretStr("x")


# --------------------------------------------------------------------------- #
# Resolvers
# --------------------------------------------------------------------------- #


def test_env_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DB_PASSWORD", PASSWORD)
    secret = resolve_secret("env:TEST_DB_PASSWORD")
    assert secret is not None and secret.reveal() == PASSWORD
    assert secret.source == "env:TEST_DB_PASSWORD"


def test_bare_reference_defaults_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DB_PASSWORD", PASSWORD)
    assert resolve_secret("TEST_DB_PASSWORD").reveal() == PASSWORD


def test_missing_env_variable_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    with pytest.raises(SecretError) as info:
        resolve_secret("env:NOT_SET_ANYWHERE")
    assert "NOT_SET_ANYWHERE" in str(info.value)
    assert "export" in str(info.value)


def test_file_resolver_strips_the_trailing_newline(tmp_path: Path) -> None:
    """Every tool that writes a secret file adds a newline; no password ends in one."""
    path = tmp_path / "db-password"
    path.write_text(PASSWORD + "\n", encoding="utf-8")
    assert resolve_secret(f"file:{path}").reveal() == PASSWORD


def test_file_resolver_reports_a_missing_mount(tmp_path: Path) -> None:
    with pytest.raises(SecretError, match="does not exist"):
        resolve_secret(f"file:{tmp_path / 'absent'}")


def test_file_resolver_rejects_an_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty"
    path.write_text("", encoding="utf-8")
    with pytest.raises(SecretError, match="empty"):
        resolve_secret(f"file:{path}")


def test_literal_resolver_warns() -> None:
    """A literal credential must be noisy, every single time."""
    with pytest.warns(UserWarning, match="literal"):
        secret = resolve_secret(f"literal:{PASSWORD}")
    assert secret.reveal() == PASSWORD


def test_unknown_scheme_lists_the_valid_ones() -> None:
    with pytest.raises(SecretError) as info:
        resolve_secret("wallet:thing")
    assert "env" in str(info.value) and "file" in str(info.value)


def test_empty_reference_is_not_an_error() -> None:
    """A connection without a password is a legitimate configuration."""
    assert resolve_secret(None) is None
    assert resolve_secret("") is None


def test_custom_resolver_can_be_registered() -> None:
    register_resolver("test-store", lambda ref: SecretStr(f"resolved-{ref}", source="test"))
    try:
        assert resolve_secret("test-store:alpha").reveal() == "resolved-alpha"
        assert "test-store" in available_schemes()
    finally:
        from pydbconnect.secrets import _resolvers

        _resolvers.pop("test-store", None)


def test_keyvault_reference_shape_is_validated() -> None:
    """A malformed reference fails before any network call is attempted."""
    with pytest.raises(SecretError, match="vault"):
        resolve_secret("keyvault:no-slash-here")


# --------------------------------------------------------------------------- #
# Redaction
# --------------------------------------------------------------------------- #


def test_redact_scrubs_known_values() -> None:
    SecretStr(PASSWORD)          # registers it
    text = f"psql://etl:{PASSWORD}@db.internal:5432/analytics"
    assert PASSWORD not in redact(text)
    assert "***" in redact(text)


def test_redact_leaves_ordinary_text_intact() -> None:
    assert redact("nothing secret here") == "nothing secret here"


def test_very_short_values_are_not_registered() -> None:
    """Scrubbing the string 'ab' from every log line would destroy the logs."""
    SecretStr("ab")
    assert redact("a table of abstract values") == "a table of abstract values"


def test_redact_mapping_uses_key_names() -> None:
    data = {
        "user": "etl",
        "password": "whatever",
        "api_key": "abcd",
        "connection_string": "AccountKey=xyz",
        "nested": {"token": "t0ken", "host": "db"},
    }
    scrubbed = redact_mapping(data)
    assert scrubbed["user"] == "etl"
    assert scrubbed["password"] == "***"
    assert scrubbed["api_key"] == "***"
    assert scrubbed["connection_string"] == "***"
    assert scrubbed["nested"] == {"token": "***", "host": "db"}


def test_password_never_appears_in_config_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    """The required assertion: a password is absent from ``repr()``."""
    monkeypatch.setenv("PGPASSWORD", PASSWORD)
    config = ConnectionConfig(
        name="warehouse", backend="postgres", host="db.internal",
        database="analytics", user="etl", secret="env:PGPASSWORD",
        options={"password": PASSWORD, "sslmode": "require"},
    )
    config.resolve_password()                 # registers the value
    rendered = repr(config)
    assert PASSWORD not in rendered
    assert "***" in rendered
    assert "sslmode" in rendered              # non-secrets still visible


def test_password_never_appears_in_a_formatted_log_record() -> None:
    """The required assertion: a password is absent from a formatted record."""
    SecretStr(PASSWORD)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    handler.addFilter(RedactingFilter())
    logger = logging.getLogger("pydbconnect.test.redaction")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    logger.info("connecting with password %s", PASSWORD)          # lazy args
    logger.warning(f"dsn=postgres://etl:{PASSWORD}@host/db")      # eager f-string
    logger.error("mapping %(pw)s", {"pw": SecretStr(PASSWORD)})   # dict args
    handler.flush()

    output = stream.getvalue()
    assert PASSWORD not in output
    assert output.count("***") >= 3


def test_literal_secret_is_masked_in_config_repr() -> None:
    """Even the reference is masked when someone inlines the value."""
    config = ConnectionConfig(
        name="x", backend="sqlite", database="d", secret=f"literal:{PASSWORD}"
    )
    assert PASSWORD not in repr(config)
    assert "literal:***" in repr(config)


def test_error_messages_are_redacted() -> None:
    """Driver errors love to include the DSN. ``PyDBError.__str__`` scrubs it."""
    SecretStr(PASSWORD)
    error = ConfigurationError(f"could not connect using password {PASSWORD}")
    assert PASSWORD not in str(error)
    assert PASSWORD not in repr(error)
