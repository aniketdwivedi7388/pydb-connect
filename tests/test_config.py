"""Configuration resolution: precedence, placeholders, profiles, validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pydbconnect.config import (
    ConnectionConfig,
    PoolSettings,
    RetrySettings,
    expand_placeholders,
    load_config,
)
from pydbconnect.exceptions import BackendNotFoundError, ConfigurationError

# --------------------------------------------------------------------------- #
# Precedence
# --------------------------------------------------------------------------- #


def test_yaml_beats_defaults(config_file: Path) -> None:
    """A value in the file overrides the defaults block."""
    config = load_config(config_file, profile="prod").get("warehouse", validate=False)
    assert config.pool.max_size == 10          # from the connection
    assert config.pool.timeout == 5.0          # from defaults, not overridden
    assert config.retry.max_attempts == 2      # from defaults


def test_env_beats_yaml(config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``PYDB_<CONN>_<FIELD>`` overrides the file."""
    monkeypatch.setenv("PYDB_WAREHOUSE_HOST", "db-from-env.internal")
    monkeypatch.setenv("PYDB_WAREHOUSE_PORT", "6432")
    config = load_config(config_file, profile="prod").get("warehouse", validate=False)
    assert config.host == "db-from-env.internal"
    assert config.port == 6432


def test_kwargs_beat_env_and_yaml(config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit keyword arguments win over every other layer."""
    monkeypatch.setenv("PYDB_WAREHOUSE_HOST", "db-from-env.internal")
    config = load_config(config_file, profile="prod").get(
        "warehouse", host="db-from-code.internal", validate=False
    )
    assert config.host == "db-from-code.internal"


def test_env_only_connection_needs_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A connection can be configured entirely from the environment.

    This is the right setup for a container image that should carry no
    environment-specific data at all.
    """
    monkeypatch.setenv("PYDB_EVENTS_BACKEND", "sqlite")
    monkeypatch.setenv("PYDB_EVENTS_DATABASE", str(tmp_path / "events.db"))
    monkeypatch.setenv("PYDB_EVENTS_POOL_MAX_SIZE", "7")
    monkeypatch.chdir(tmp_path)          # no connections.yaml anywhere
    config = load_config(None).get("events")
    assert config.backend == "sqlite"
    assert config.pool.max_size == 7


def test_env_password_becomes_a_secret_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PYDB_X_PASSWORD`` sets a *reference*, never the value itself.

    This keeps the password out of the config object entirely: the reference is
    resolved later, through the same code path as every other secret.
    """
    monkeypatch.setenv("PYDB_EVENTS_BACKEND", "sqlite")
    monkeypatch.setenv("PYDB_EVENTS_DATABASE", ":memory:")
    monkeypatch.setenv("PYDB_EVENTS_PASSWORD", "s3cr3t-value-not-in-config")
    config = load_config(None).get("events")
    assert config.secret == "env:PYDB_EVENTS_PASSWORD"
    assert "s3cr3t-value-not-in-config" not in repr(config)
    assert config.resolve_password().reveal() == "s3cr3t-value-not-in-config"


def test_env_options_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_OPTIONS`` JSON and ``_OPTION_<KEY>`` both feed the options dict."""
    monkeypatch.setenv("PYDB_EVENTS_BACKEND", "sqlite")
    monkeypatch.setenv("PYDB_EVENTS_DATABASE", ":memory:")
    monkeypatch.setenv("PYDB_EVENTS_OPTIONS", json.dumps({"journal_mode": "WAL"}))
    monkeypatch.setenv("PYDB_EVENTS_OPTION_BUSY_TIMEOUT", "3000")
    config = load_config(None).get("events")
    assert config.options == {"journal_mode": "WAL", "busy_timeout": "3000"}


def test_env_options_rejects_bad_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed ``_OPTIONS`` value names itself in the error."""
    monkeypatch.setenv("PYDB_EVENTS_BACKEND", "sqlite")
    monkeypatch.setenv("PYDB_EVENTS_OPTIONS", "{not json")
    with pytest.raises(ConfigurationError, match="PYDB_EVENTS_OPTIONS"):
        load_config(None).get("events")


# --------------------------------------------------------------------------- #
# Profiles
# --------------------------------------------------------------------------- #


def test_profiles_select_different_backends(config_file: Path) -> None:
    """One file, one connection name, two very different targets."""
    dev = load_config(config_file, profile="dev").get("warehouse")
    prod = load_config(config_file, profile="prod").get("warehouse", validate=False)
    assert dev.backend == "sqlite"
    assert prod.backend == "postgres"
    assert prod.profile == "prod"


def test_default_profile_is_used(config_file: Path) -> None:
    """``default_profile:`` applies when nothing else selects one."""
    assert load_config(config_file).profile == "dev"


def test_profile_env_variable(config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``PYDB_PROFILE`` selects the profile."""
    monkeypatch.setenv("PYDB_PROFILE", "prod")
    assert load_config(config_file).get("warehouse", validate=False).backend == "postgres"


def test_unknown_profile_lists_the_valid_ones(config_file: Path) -> None:
    """A typo in the profile name produces the list of real ones."""
    with pytest.raises(ConfigurationError) as info:
        load_config(config_file, profile="staging")
    assert "dev" in str(info.value) and "prod" in str(info.value)


def test_shared_connections_visible_in_every_profile(config_file: Path) -> None:
    """Connections outside ``profiles:`` are shared."""
    for profile in ("dev", "prod"):
        assert "scratch" in load_config(config_file, profile=profile).names()


# --------------------------------------------------------------------------- #
# Placeholders
# --------------------------------------------------------------------------- #


def test_env_placeholder_is_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_DB_HOST", "resolved.internal")
    assert expand_placeholders("${env:MY_DB_HOST}") == "resolved.internal"


def test_env_placeholder_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    assert expand_placeholders("${env:NOT_SET_ANYWHERE:-fallback}") == "fallback"


def test_missing_placeholder_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    with pytest.raises(ConfigurationError, match="NOT_SET_ANYWHERE"):
        expand_placeholders("${env:NOT_SET_ANYWHERE}")


def test_placeholders_expand_inside_nested_structures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Expansion recurses into dicts and lists, not only top-level strings."""
    monkeypatch.setenv("REGION", "eu-west-1")
    data = {"a": {"b": ["${env:REGION}", "literal"]}, "n": 5}
    assert expand_placeholders(data) == {"a": {"b": ["eu-west-1", "literal"]}, "n": 5}


def test_file_placeholder_reads_a_mounted_secret(tmp_path: Path) -> None:
    """``${file:...}`` reads a file and strips the trailing newline."""
    secret_file = tmp_path / "token"
    secret_file.write_text("from-a-mounted-file\n", encoding="utf-8")
    assert expand_placeholders(f"${{file:{secret_file}}}") == "from-a-mounted-file"


def test_non_strict_mode_leaves_placeholders_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    """``strict=False`` is what lets ``config list`` run without prod secrets."""
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    assert expand_placeholders("${env:NOT_SET_ANYWHERE}", strict=False) == "${env:NOT_SET_ANYWHERE}"


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def test_missing_backend_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig.from_dict("x", {"host": "h"})
    assert info.value.context["key"] == "backend"


def test_unknown_backend_lists_the_registered_ones() -> None:
    with pytest.raises(BackendNotFoundError) as info:
        ConnectionConfig(name="x", backend="postgrez", database="d").validate()
    assert "postgres" in str(info.value)


def test_unknown_top_level_key_is_named() -> None:
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig.from_dict("x", {"backend": "sqlite", "hostname": "h"})
    assert info.value.context["key"] == "hostname"
    assert "host" in str(info.value)


def test_password_key_is_rejected_with_guidance() -> None:
    """A password in the config file is the mistake this library exists to stop."""
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig.from_dict("x", {"backend": "sqlite", "password": "hunter2"})
    assert "secret" in str(info.value)
    assert "env:" in str(info.value)


def test_backend_required_field_is_named() -> None:
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig(name="x", backend="sqlite").validate()
    assert info.value.context["key"] == "database"


def test_bad_port_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="port"):
        ConnectionConfig(name="x", backend="sqlite", database="d", port=99999).validate()


def test_non_integer_port_names_the_key() -> None:
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig.from_dict("x", {"backend": "sqlite", "port": "not-a-number"})
    assert info.value.context["key"] == "port"


def test_pool_min_above_max_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match=r"pool\.min_size"):
        ConnectionConfig(
            name="x", backend="sqlite", database="d",
            pool=PoolSettings(min_size=5, max_size=2),
        ).validate()


def test_retry_settings_are_validated() -> None:
    with pytest.raises(ConfigurationError, match=r"retry\.jitter"):
        ConnectionConfig(
            name="x", backend="sqlite", database="d",
            retry=RetrySettings(jitter="sometimes"),
        ).validate()


def test_unknown_pool_key_is_named() -> None:
    with pytest.raises(ConfigurationError) as info:
        ConnectionConfig.from_dict(
            "x", {"backend": "sqlite", "database": "d", "pool": {"maxsize": 3}}
        )
    assert info.value.context["key"] == "pool.maxsize"


def test_unknown_secret_scheme_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="scheme"):
        ConnectionConfig(
            name="x", backend="sqlite", database="d", secret="wallet:thing"
        ).validate()


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #


def test_unknown_connection_lists_the_known_ones(config_file: Path) -> None:
    with pytest.raises(ConfigurationError) as info:
        load_config(config_file).get("nope")
    message = str(info.value)
    assert "warehouse" in message and "scratch" in message
    assert "PYDB_NOPE_BACKEND" in message


def test_missing_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="not found"):
        load_config(tmp_path / "nope.yaml")


def test_absent_file_is_tolerated_when_not_required(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No file is a valid state: everything can come from the environment."""
    monkeypatch.chdir(tmp_path)
    config_file = load_config(None)
    assert config_file.path is None and config_file.names() == []


def test_invalid_yaml_names_the_file(tmp_path: Path) -> None:
    bad = tmp_path / "connections.yaml"
    bad.write_text("connections:\n  a: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="invalid YAML"):
        load_config(bad)


def test_top_level_must_be_a_mapping(tmp_path: Path) -> None:
    bad = tmp_path / "connections.yaml"
    bad.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="mapping"):
        load_config(bad)


def test_config_file_env_variable_is_honoured(config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYDB_CONFIG_FILE", str(config_file))
    assert "warehouse" in load_config(None).names()


def test_all_resolves_every_connection(config_file: Path) -> None:
    configs = load_config(config_file, profile="dev").all()
    assert {c.name for c in configs} == {"scratch", "warehouse"}


def test_to_dict_is_redacted() -> None:
    """``to_dict`` is what the CLI prints, so it must never leak."""
    config = ConnectionConfig(
        name="x", backend="sqlite", database="d",
        options={"password": "hunter2", "sslmode": "require"},
    )
    data = config.to_dict()
    assert data["options"]["password"] == "***"
    assert data["options"]["sslmode"] == "require"
