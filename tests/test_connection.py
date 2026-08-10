"""Connections, transactions, the SQL guard and the pool.

Includes the test that motivates the whole library: after any exit path -
success, exception, forgotten close - the connection is closed and the driver
knows about it.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

import pytest
from conftest import FakeConnection

from pydbconnect import connect
from pydbconnect.config import ConnectionConfig, PoolSettings
from pydbconnect.connection import Connection, ConnectionPool, check_sql_safety
from pydbconnect.exceptions import (
    ConnectionFailure,
    NotSupportedError,
    PoolClosedError,
    PoolTimeout,
    QueryError,
    UnsafeSQLError,
)
from pydbconnect.registry import get_backend

# --------------------------------------------------------------------------- #
# Basic operations
# --------------------------------------------------------------------------- #


def test_query_returns_dicts(conn: Connection) -> None:
    """Rows are dicts so adding a column to the SELECT cannot break indexing."""
    conn.execute("INSERT INTO people (id, name, team) VALUES (?, ?, ?)", (1, "ada", "alpha"))
    rows = conn.query("SELECT id, name, team FROM people")
    assert rows == [{"id": 1, "name": "ada", "team": "alpha"}]


def test_execute_reports_affected_rows(conn: Connection) -> None:
    conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
    conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (2, "grace"))
    assert conn.execute("UPDATE people SET team = ?", ("alpha",)) == 2


def test_query_one_returns_none_when_empty(conn: Connection) -> None:
    assert conn.query_one("SELECT * FROM people WHERE id = ?", (404,)) is None


def test_scalar_returns_the_first_column(conn: Connection) -> None:
    conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
    assert conn.scalar("SELECT count(*) FROM people") == 1


def test_executemany_is_one_call_per_batch(fake_conn: FakeConnection) -> None:
    """The point of executemany: one driver call, not one per row."""
    config = ConnectionConfig(name="fake", backend="sqlite", database=":memory:")
    connection = Connection(config, get_backend("sqlite"), fake_conn)
    written = connection.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    assert written == 3
    assert len(fake_conn.executed) == 1
    assert fake_conn.executed[0][1] == [(1,), (2,), (3,)]


def test_executemany_with_no_rows_does_nothing(conn: Connection) -> None:
    assert conn.executemany("INSERT INTO people (id) VALUES (?)", []) == 0


def test_stream_yields_rows_without_buffering(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    conn.executemany(
        "INSERT INTO people (id, name, team, score) VALUES (?, ?, ?, ?)",
        [(r["id"], r["name"], r["team"], r["score"]) for r in people_rows],
    )
    streamed = list(conn.stream("SELECT id, name FROM people ORDER BY id", chunk_size=4))
    assert len(streamed) == 25
    assert streamed[0] == {"id": 1, "name": "person-01"}


def test_stream_rejects_a_zero_chunk_size(conn: Connection) -> None:
    with pytest.raises(ValueError, match="chunk_size"):
        list(conn.stream("SELECT 1", chunk_size=0))


def test_bad_sql_raises_query_error_with_the_cause(conn: Connection) -> None:
    with pytest.raises(QueryError) as info:
        conn.query("SELECT * FROM table_that_does_not_exist")
    assert info.value.__cause__ is not None
    assert info.value.context["connection"] == "mem"


# --------------------------------------------------------------------------- #
# Transactions
# --------------------------------------------------------------------------- #


def test_transaction_commits_on_success(conn: Connection) -> None:
    with conn.transaction():
        conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
        conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (2, "grace"))
    assert conn.scalar("SELECT count(*) FROM people") == 2


def test_transaction_rolls_back_on_exception(conn: Connection) -> None:
    with pytest.raises(RuntimeError):
        with conn.transaction():
            conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
            raise RuntimeError("something failed halfway")
    assert conn.scalar("SELECT count(*) FROM people") == 0


def test_statements_outside_a_transaction_commit_individually(conn: Connection) -> None:
    conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
    conn.rollback()
    assert conn.scalar("SELECT count(*) FROM people") == 1


def test_nested_transactions_join_the_outer_one(conn: Connection) -> None:
    """The inner block must not commit; an outer rollback undoes everything."""
    with pytest.raises(RuntimeError):
        with conn.transaction():
            conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (1, "ada"))
            with conn.transaction():
                conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (2, "grace"))
            raise RuntimeError("outer failure")
    assert conn.scalar("SELECT count(*) FROM people") == 0


def test_in_transaction_flag(conn: Connection) -> None:
    assert conn.in_transaction is False
    with conn.transaction():
        assert conn.in_transaction is True
    assert conn.in_transaction is False


def test_statements_are_not_retried_inside_a_transaction(fake_conn: FakeConnection) -> None:
    """Retrying inside a transaction re-runs against a rolled-back session."""
    config = ConnectionConfig(name="fake", backend="sqlite", database=":memory:")
    connection = Connection(config, get_backend("sqlite"), fake_conn)
    fake_conn.fail_with = ConnectionResetError("connection reset by peer")
    with pytest.raises(QueryError), connection.transaction():
        connection.execute("INSERT INTO t VALUES (?)", (1,))
    assert len(fake_conn.executed) == 1
    assert fake_conn.rollbacks == 1


def test_close_with_an_open_transaction_rolls_back(conn: Connection, tmp_path: Path) -> None:
    config = ConnectionConfig(
        name="disk", backend="sqlite", database=str(tmp_path / "d.db")
    ).validate()
    first = Connection.open(config)
    first.execute("CREATE TABLE t (id INTEGER)")
    ctx = first.transaction()
    ctx.__enter__()
    first.execute("INSERT INTO t VALUES (1)")
    first.close()                       # never exits the block

    with Connection.open(config) as second:
        assert second.scalar("SELECT count(*) FROM t") == 0


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #


def test_context_manager_closes_on_success(memory_config: ConnectionConfig) -> None:
    with Connection.open(memory_config) as conn:
        raw = conn.raw
        conn.execute("CREATE TABLE t (id INTEGER)")
    assert conn.closed
    with pytest.raises(sqlite3.ProgrammingError):
        raw.execute("SELECT 1")         # the driver object really is closed


def test_context_manager_closes_on_exception(memory_config: ConnectionConfig) -> None:
    """The failure the library exists to prevent: a leaked connection."""
    connection = None
    with pytest.raises(RuntimeError):
        with Connection.open(memory_config) as conn:
            connection = conn
            raise RuntimeError("boom")
    assert connection is not None and connection.closed


def test_close_is_idempotent(memory_config: ConnectionConfig) -> None:
    conn = Connection.open(memory_config)
    conn.close()
    conn.close()                        # must not raise
    assert conn.closed


def test_using_a_closed_connection_is_a_clear_error(memory_config: ConnectionConfig) -> None:
    conn = Connection.open(memory_config)
    conn.close()
    with pytest.raises(ConnectionFailure, match="closed"):
        conn.query("SELECT 1")


def test_connect_failure_names_the_connection(tmp_path: Path) -> None:
    config = ConnectionConfig(
        name="broken", backend="sqlite",
        database=str(tmp_path / "no-such-dir" / "x.db"),
    )
    with pytest.raises(ConnectionFailure) as info:
        Connection.open(config)
    assert "no-such-dir" in str(info.value)


def test_ping_reports_liveness(conn: Connection) -> None:
    assert conn.ping() is True
    conn.close()
    assert conn.ping() is False


def test_repr_is_safe_and_informative(conn: Connection) -> None:
    assert "Connection" in repr(conn) and "mem" in repr(conn)


def test_top_level_connect_uses_the_config_file(config_file: Path) -> None:
    with connect("warehouse", config_path=config_file) as conn:
        assert conn.scalar("SELECT 1") == 1


# --------------------------------------------------------------------------- #
# SQL guard
# --------------------------------------------------------------------------- #


def test_guard_flags_an_interpolated_literal() -> None:
    order_id = "abc"
    reason = check_sql_safety(f"SELECT * FROM orders WHERE id = '{order_id}'")
    assert reason is not None


def test_guard_flags_unsubstituted_braces() -> None:
    assert check_sql_safety("SELECT * FROM orders WHERE id = {}") is not None


def test_guard_ignores_parameterised_sql() -> None:
    assert check_sql_safety("SELECT * FROM orders WHERE id = ?", (1,)) is None


def test_guard_ignores_ddl_defaults() -> None:
    """``CREATE TABLE ... DEFAULT 'x'`` is not interpolation."""
    assert check_sql_safety("CREATE TABLE t (status TEXT DEFAULT 'new')") is None


def test_guard_can_be_switched_to_error() -> None:
    with pytest.raises(UnsafeSQLError, match="parameters"):
        check_sql_safety("DELETE FROM orders WHERE id = '17'", mode="error")


def test_guard_can_be_switched_off() -> None:
    assert check_sql_safety("DELETE FROM orders WHERE id = '17'", mode="off") is None


def test_connection_honours_the_configured_guard_mode(tmp_path: Path) -> None:
    config = ConnectionConfig(
        name="strict", backend="sqlite",
        database=str(tmp_path / "s.db"), sql_guard="error",
    ).validate()
    with Connection.open(config) as conn:
        conn.execute("CREATE TABLE t (name TEXT)")
        with pytest.raises(UnsafeSQLError):
            conn.query("SELECT * FROM t WHERE name = 'ada'")


# --------------------------------------------------------------------------- #
# Pool
# --------------------------------------------------------------------------- #


def test_pool_reuses_connections(sqlite_config: ConnectionConfig) -> None:
    with ConnectionPool(sqlite_config, size=2) as pool:
        for _ in range(6):
            with pool.acquire() as conn:
                conn.scalar("SELECT 1")
        assert pool.stats()["opened"] <= 2


def test_pool_respects_its_bound(sqlite_config: ConnectionConfig) -> None:
    """An unbounded pool is a denial-of-service tool aimed at your own database."""
    with ConnectionPool(sqlite_config, size=2) as pool:
        first = pool.borrow()
        second = pool.borrow()
        try:
            assert pool.stats()["in_use"] == 2
            with pytest.raises(PoolTimeout, match="2 connection"):
                pool.borrow(timeout=0.05)
        finally:
            first.close()
            second.close()


def test_pool_hands_a_connection_back_after_release(sqlite_config: ConnectionConfig) -> None:
    with ConnectionPool(sqlite_config, size=1) as pool:
        with pool.acquire() as conn:
            conn.scalar("SELECT 1")
        with pool.acquire(timeout=0.5) as conn:      # would time out if not returned
            assert conn.scalar("SELECT 1") == 1


def test_pool_returns_connections_after_an_exception(sqlite_config: ConnectionConfig) -> None:
    with ConnectionPool(sqlite_config, size=1) as pool:
        with pytest.raises(RuntimeError):
            with pool.acquire() as conn:
                raise RuntimeError("caller blew up")
        with pool.acquire(timeout=0.5) as conn:
            assert conn.scalar("SELECT 1") == 1


def test_pool_discards_a_dead_connection_on_checkout(sqlite_config: ConnectionConfig) -> None:
    """Liveness check on borrow: the caller never sees the corpse."""
    with ConnectionPool(sqlite_config, size=1, pre_ping=True) as pool:
        conn = pool.borrow()
        conn.raw.close()                 # kill it behind the pool's back
        conn.close()
        with pool.acquire() as fresh:
            assert fresh.scalar("SELECT 1") == 1
        assert pool.stats()["discarded"] >= 1


def test_pool_recycles_old_connections(sqlite_config: ConnectionConfig) -> None:
    with ConnectionPool(sqlite_config, size=1, recycle=0.01) as pool:
        with pool.acquire() as conn:
            conn.scalar("SELECT 1")
        time.sleep(0.02)
        with pool.acquire() as conn:
            conn.scalar("SELECT 1")
        assert pool.stats()["opened"] == 2


def test_pool_prefills_min_size(sqlite_config: ConnectionConfig) -> None:
    sqlite_config.pool = PoolSettings(min_size=2, max_size=4)
    with ConnectionPool(sqlite_config) as pool:
        assert pool.stats()["idle"] == 2


def test_pool_is_thread_safe(sqlite_config: ConnectionConfig) -> None:
    """Twelve threads, four slots, no double checkout and no lost rows."""
    errors: List[BaseException] = []
    with ConnectionPool(sqlite_config, size=4) as pool:
        with pool.acquire() as conn:
            conn.execute("CREATE TABLE t (n INTEGER)")

        def worker(n: int) -> None:
            try:
                with pool.acquire(timeout=10) as conn:
                    conn.execute("INSERT INTO t VALUES (?)", (n,))
            except BaseException as exc:  # noqa: BLE001 - the assertion is that this stays empty
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        with pool.acquire() as conn:
            assert conn.scalar("SELECT count(*) FROM t") == 12
        assert pool.stats()["in_use"] == 0


def test_closed_pool_refuses_checkouts(sqlite_config: ConnectionConfig) -> None:
    pool = ConnectionPool(sqlite_config, size=1)
    pool.close()
    with pytest.raises(PoolClosedError, match="closed"):
        pool.borrow()


def test_object_store_backend_rejects_sql() -> None:
    """The adls backend is object storage; SQL methods say so plainly."""
    backend = get_backend("adls")
    assert backend.supports_transactions is False
    with pytest.raises(NotSupportedError, match="upsert"):
        backend.upsert_sql("t", ["a"], ["a"])


# --------------------------------------------------------------------------- #
# Import hygiene
# --------------------------------------------------------------------------- #


def test_no_driver_is_imported_by_the_library() -> None:
    """The hard constraint: importing pydbconnect must not import any driver.

    If someone moves ``import psycopg`` to the top of a backend module, this
    fails here rather than on a machine that happens not to have it.
    """
    import sys

    from conftest import OPTIONAL_DRIVERS

    imported = [name for name in OPTIONAL_DRIVERS if name in sys.modules]
    assert imported == [], f"these drivers were imported at module load: {imported}"


def test_every_backend_loads_without_its_driver() -> None:
    """Backend classes must be importable and describable with no driver present."""
    from pydbconnect.registry import available_backends, describe_backends

    rows = {row["name"]: row for row in describe_backends()}
    assert set(rows) == set(available_backends())
    assert all("error" not in row for row in rows.values())
    assert rows["sqlite"]["installed"] is True


def test_missing_driver_names_the_pip_extra() -> None:
    """``ModuleNotFoundError: psycopg`` does not say which extra provides it."""
    from pydbconnect.exceptions import DriverNotInstalledError

    for name, extra in (
        ("postgres", "postgres"), ("mysql", "mysql"),
        ("oracle", "oracle"), ("snowflake", "snowflake"), ("adls", "azure"),
    ):
        backend = get_backend(name)
        if backend.driver_available():       # pragma: no cover - driver present
            continue
        with pytest.raises(DriverNotInstalledError) as info:
            backend.import_driver()
        assert f'pydb-connect[{extra}]' in str(info.value)
