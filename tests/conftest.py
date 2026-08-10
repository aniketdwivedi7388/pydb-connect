"""Shared fixtures.

Every test in this suite runs against SQLite or an in-process fake. Nothing
here needs a database server, a container, a credential or a network
connection, which is what makes ``pytest -q`` finish in under a second and run
identically on a laptop and in CI.

The first thing the suite asserts is that **no database driver is importable**.
If someone adds a top-level ``import psycopg`` to a backend module, that test
fails immediately rather than three months later on a machine that happens not
to have it.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydbconnect.config import ConnectionConfig
from pydbconnect.connection import Connection

#: Driver modules that must never be needed for the suite to pass.
OPTIONAL_DRIVERS = (
    "mysql.connector",
    "psycopg",
    "psycopg2",
    "oracledb",
    "snowflake.connector",
    "azure.storage.blob",
    "azure.identity",
)


@pytest.fixture
def sqlite_config(tmp_path: Path) -> ConnectionConfig:
    """A validated SQLite configuration pointing at a temporary file."""
    return ConnectionConfig(
        name="test",
        backend="sqlite",
        database=str(tmp_path / "test.db"),
    ).validate()


@pytest.fixture
def memory_config() -> ConnectionConfig:
    """An in-memory SQLite configuration. Fast, and gone at teardown."""
    return ConnectionConfig(name="mem", backend="sqlite", database=":memory:").validate()


@pytest.fixture
def conn(memory_config: ConnectionConfig) -> Iterator[Connection]:
    """An open connection to an in-memory database with a ``people`` table."""
    connection = Connection.open(memory_config)
    connection.execute(
        "CREATE TABLE people ("
        " id INTEGER PRIMARY KEY,"
        " name TEXT NOT NULL,"
        " team TEXT,"
        " score REAL)"
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def people_rows() -> List[Dict[str, Any]]:
    """Twenty-five deterministic rows, three teams."""
    teams = ("alpha", "beta", "gamma")
    return [
        {"id": i, "name": f"person-{i:02d}", "team": teams[i % 3], "score": i * 1.5}
        for i in range(1, 26)
    ]


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """A connections file with two profiles and a shared connection."""
    path = tmp_path / "connections.yaml"
    path.write_text(
        "version: 1\n"
        "default_profile: dev\n"
        "defaults:\n"
        "  pool: {max_size: 4, timeout: 5}\n"
        "  retry: {max_attempts: 2}\n"
        "connections:\n"
        "  scratch:\n"
        "    backend: sqlite\n"
        "    database: ':memory:'\n"
        "profiles:\n"
        "  dev:\n"
        "    connections:\n"
        "      warehouse:\n"
        "        backend: sqlite\n"
        f"        database: {tmp_path / 'dev.db'}\n"
        "  prod:\n"
        "    connections:\n"
        "      warehouse:\n"
        "        backend: postgres\n"
        "        host: db.internal\n"
        "        port: 5432\n"
        "        database: analytics\n"
        "        user: etl_writer\n"
        "        secret: env:PGPASSWORD\n"
        "        pool: {max_size: 10}\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture(autouse=True)
def clean_pydb_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove every ``PYDB_*`` variable so tests cannot leak into each other.

    The configuration layer reads the environment by design, which makes a
    stray variable from one test able to change the outcome of another. This
    fixture is autouse for exactly that reason.
    """
    for key in list(os.environ):
        if key.startswith("PYDB_"):
            monkeypatch.delenv(key, raising=False)


class FakeCursor:
    """A minimal DB-API cursor that records calls and returns canned rows."""

    def __init__(self, owner: "FakeConnection") -> None:
        self.owner = owner
        self.description: Any = None
        self.rowcount = -1
        self._rows: List[tuple] = []
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.owner.executed.append((sql, params))
        if self.owner.fail_with is not None:
            error, self.owner.fail_with = self.owner.fail_with, None
            raise error
        self.description = [("value", None, None, None, None, None, None)]
        self._rows = [(1,)]
        self.rowcount = 1

    def executemany(self, sql: str, seq: Any) -> None:
        rows = list(seq)
        self.owner.executed.append((sql, rows))
        if self.owner.fail_with is not None:
            error, self.owner.fail_with = self.owner.fail_with, None
            raise error
        self.rowcount = len(rows)

    def fetchall(self) -> List[tuple]:
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self) -> Any:
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size: int) -> List[tuple]:
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """A DB-API-shaped connection that counts commits, rollbacks and closes."""

    def __init__(self) -> None:
        self.executed: List[tuple] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.cursors: List[FakeCursor] = []
        self.fail_with: Any = None

    def cursor(self) -> FakeCursor:
        cur = FakeCursor(self)
        self.cursors.append(cur)
        return cur

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_conn() -> FakeConnection:
    """A fake raw connection for asserting on driver-level behaviour."""
    return FakeConnection()
