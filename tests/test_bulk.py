"""Bulk loading, upserts, chunked reads and the per-backend SQL they generate."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest
from conftest import FakeConnection

from pydbconnect.bulk import (
    BulkResult,
    bulk_insert,
    chunked,
    chunked_read,
    copy_from,
    rows_from_csv,
    upsert,
)
from pydbconnect.connection import Connection
from pydbconnect.exceptions import BulkLoadError, NotSupportedError
from pydbconnect.registry import get_backend

# --------------------------------------------------------------------------- #
# chunked
# --------------------------------------------------------------------------- #


def test_chunked_splits_evenly() -> None:
    assert list(chunked(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]


def test_chunked_handles_an_empty_source() -> None:
    assert list(chunked([], 10)) == []


def test_chunked_rejects_a_zero_size() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        list(chunked([1], 0))


def test_chunked_is_lazy() -> None:
    """A generator source must not be drained before the first batch is yielded."""
    consumed = []

    def source():
        for i in range(10):
            consumed.append(i)
            yield i

    batches = chunked(source(), 3)
    next(batches)
    assert consumed == [0, 1, 2]


# --------------------------------------------------------------------------- #
# bulk_insert
# --------------------------------------------------------------------------- #


def test_bulk_insert_writes_every_row(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    result = bulk_insert(conn, "people", people_rows, chunk_size=10)
    assert result.rows_written == 25
    assert result.chunks == 3
    assert result.method == "executemany"
    assert conn.scalar("SELECT count(*) FROM people") == 25


def test_bulk_insert_batches_instead_of_looping(fake_conn: FakeConnection) -> None:
    """1000 rows at chunk_size 250 must be four driver calls, not a thousand."""
    from pydbconnect.config import ConnectionConfig

    config = ConnectionConfig(name="fake", backend="sqlite", database=":memory:")
    connection = Connection(config, get_backend("sqlite"), fake_conn)
    rows = [{"id": i, "name": f"n{i}"} for i in range(1000)]
    result = bulk_insert(connection, "t", rows, chunk_size=250)
    assert result.chunks == 4
    assert len(fake_conn.executed) == 4


def test_bulk_insert_infers_columns_from_the_first_row(conn: Connection) -> None:
    bulk_insert(conn, "people", [{"id": 1, "name": "ada"}])
    assert conn.query_one("SELECT * FROM people")["name"] == "ada"


def test_bulk_insert_accepts_sequences_with_explicit_columns(conn: Connection) -> None:
    bulk_insert(conn, "people", [(1, "ada"), (2, "grace")], columns=["id", "name"])
    assert conn.scalar("SELECT count(*) FROM people") == 2


def test_bulk_insert_projects_dicts_onto_a_fixed_column_order(conn: Connection) -> None:
    """Key order must not decide which column a value lands in."""
    bulk_insert(
        conn, "people",
        [{"id": 1, "name": "ada"}, {"name": "grace", "id": 2}],
    )
    assert conn.query_one("SELECT name FROM people WHERE id = ?", (2,))["name"] == "grace"


def test_bulk_insert_requires_columns_for_sequences(conn: Connection) -> None:
    with pytest.raises(BulkLoadError, match="columns must be given"):
        bulk_insert(conn, "people", [(1, "ada")])


def test_bulk_insert_reports_a_missing_key_by_row_number(conn: Connection) -> None:
    with pytest.raises(BulkLoadError) as info:
        bulk_insert(conn, "people", [{"id": 1, "name": "ada"}, {"id": 2}])
    assert "row 1" in str(info.value) and "name" in str(info.value)


def test_bulk_insert_reports_a_wrong_width_row(conn: Connection) -> None:
    with pytest.raises(BulkLoadError, match="value"):
        bulk_insert(conn, "people", [(1, "ada"), (2,)], columns=["id", "name"])


def test_bulk_insert_of_nothing_is_not_an_error(conn: Connection) -> None:
    result = bulk_insert(conn, "people", [])
    assert result.rows_written == 0 and result.chunks == 0


def test_progress_callback_reports_each_chunk(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    seen: List[tuple] = []
    bulk_insert(conn, "people", people_rows, chunk_size=10, on_progress=lambda w, c: seen.append((w, c)))
    assert seen == [(10, 10), (20, 10), (25, 5)]


def test_progress_callback_failure_does_not_break_the_load(conn: Connection) -> None:
    def explode(written: int, chunk_rows: int) -> None:
        raise RuntimeError("dashboard is down")

    result = bulk_insert(conn, "people", [{"id": 1, "name": "ada"}], on_progress=explode)
    assert result.rows_written == 1


def test_failed_load_reports_how_far_it_got(conn: Connection) -> None:
    """``rows_written`` is the difference between resuming and starting over."""
    conn.execute("INSERT INTO people (id, name) VALUES (?, ?)", (15, "clash"))
    rows = [{"id": i, "name": f"p{i}"} for i in range(1, 26)]
    with pytest.raises(BulkLoadError) as info:
        bulk_insert(conn, "people", rows, chunk_size=10)
    assert info.value.rows_written == 10


def test_generators_are_consumed_lazily(conn: Connection) -> None:
    """A source larger than memory must never be materialised."""
    produced: List[int] = []
    high_water: List[int] = []

    def source():
        for i in range(30):
            produced.append(i)
            yield {"id": i, "name": f"p{i}"}

    def track(written: int, chunk_rows: int) -> None:
        # How many rows the generator had produced when a chunk was written.
        high_water.append(len(produced))

    bulk_insert(conn, "people", source(), chunk_size=10, on_progress=track)
    assert conn.scalar("SELECT count(*) FROM people") == 30
    # If the source had been drained up front, every entry would read 30.
    assert high_water == [10, 20, 30]


# --------------------------------------------------------------------------- #
# upsert
# --------------------------------------------------------------------------- #


def test_upsert_inserts_then_updates(conn: Connection) -> None:
    conn.execute("CREATE UNIQUE INDEX people_id ON people (id)")
    upsert(conn, "people", [{"id": 1, "name": "ada", "score": 1.0}], key_columns=["id"])
    upsert(conn, "people", [{"id": 1, "name": "ada lovelace", "score": 2.0}], key_columns=["id"])
    row = conn.query_one("SELECT name, score FROM people WHERE id = ?", (1,))
    assert row == {"name": "ada lovelace", "score": 2.0}
    assert conn.scalar("SELECT count(*) FROM people") == 1


def test_upsert_can_preserve_columns(conn: Connection) -> None:
    """``update_columns`` is how you keep ``created_at`` from being overwritten."""
    conn.execute("CREATE UNIQUE INDEX people_id ON people (id)")
    upsert(conn, "people", [{"id": 1, "name": "ada", "team": "alpha"}], key_columns=["id"])
    upsert(
        conn, "people", [{"id": 1, "name": "ada2", "team": "beta"}],
        key_columns=["id"], update_columns=["name"],
    )
    row = conn.query_one("SELECT name, team FROM people WHERE id = ?", (1,))
    assert row == {"name": "ada2", "team": "alpha"}


def test_upsert_requires_key_columns(conn: Connection) -> None:
    with pytest.raises(BulkLoadError, match="key_columns"):
        upsert(conn, "people", [{"id": 1}], key_columns=[])


def test_upsert_rejects_a_key_outside_the_written_columns(conn: Connection) -> None:
    with pytest.raises(BulkLoadError, match="key column"):
        upsert(conn, "people", [{"name": "ada"}], key_columns=["id"])


# --------------------------------------------------------------------------- #
# copy_from
# --------------------------------------------------------------------------- #


def test_copy_from_falls_back_where_unsupported(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    """SQLite has no COPY; the fallback is reported honestly in ``method``."""
    result = copy_from(conn, "people", people_rows, chunk_size=10)
    assert result.method == "copy-fallback"
    assert result.rows_written == 25


def test_copy_from_can_refuse_to_fall_back(conn: Connection) -> None:
    """In a pipeline, a silent 10x slowdown should be an error, not a surprise."""
    with pytest.raises(NotSupportedError, match="fallback=True"):
        copy_from(conn, "people", [{"id": 1, "name": "ada"}], fallback=False)


# --------------------------------------------------------------------------- #
# chunked_read
# --------------------------------------------------------------------------- #


def test_chunked_read_batches(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    bulk_insert(conn, "people", people_rows)
    batches = list(chunked_read(conn, "SELECT id, name FROM people ORDER BY id", chunk_size=10))
    assert [len(b) for b in batches] == [10, 10, 5]
    assert batches[0][0] == {"id": 1, "name": "person-01"}


def test_chunked_read_of_an_empty_table(conn: Connection) -> None:
    assert list(chunked_read(conn, "SELECT * FROM people")) == []


def test_chunked_read_accepts_parameters(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    bulk_insert(conn, "people", people_rows)
    batches = list(chunked_read(conn, "SELECT id FROM people WHERE team = ?", ("alpha",), chunk_size=5))
    assert sum(len(b) for b in batches) == 8


def test_chunked_read_reports_progress(conn: Connection, people_rows: List[Dict[str, Any]]) -> None:
    bulk_insert(conn, "people", people_rows)
    seen: List[tuple] = []
    list(chunked_read(conn, "SELECT id FROM people", chunk_size=10, on_progress=lambda r, c: seen.append((r, c))))
    assert seen == [(10, 10), (20, 10), (25, 5)]


def test_round_trip_between_two_connections(conn: Connection, people_rows: List[Dict[str, Any]], tmp_path: Path) -> None:
    """The multi-backend copy pattern: stream out of one, batch into another."""
    from pydbconnect.config import ConnectionConfig

    bulk_insert(conn, "people", people_rows)
    target_config = ConnectionConfig(
        name="target", backend="sqlite", database=str(tmp_path / "target.db")
    ).validate()
    with Connection.open(target_config) as target:
        target.execute("CREATE TABLE people (id INTEGER, name TEXT, team TEXT, score REAL)")
        moved = 0
        for batch in chunked_read(conn, "SELECT id, name, team, score FROM people", chunk_size=7):
            moved += bulk_insert(target, "people", batch, chunk_size=7).rows_written
        assert moved == 25
        assert target.scalar("SELECT count(*) FROM people") == 25


# --------------------------------------------------------------------------- #
# CSV
# --------------------------------------------------------------------------- #


def test_rows_from_csv_streams_a_file(tmp_path: Path, conn: Connection) -> None:
    path = tmp_path / "people.csv"
    path.write_text("id,name,team\n1,ada,alpha\n2,grace,\n", encoding="utf-8")
    columns, rows = rows_from_csv(path)
    assert columns == ["id", "name", "team"]
    result = bulk_insert(conn, "people", rows, columns=columns)
    assert result.rows_written == 2
    assert conn.query_one("SELECT team FROM people WHERE id = ?", ("2",))["team"] is None


def test_rows_from_csv_rejects_a_headerless_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(BulkLoadError, match="header"):
        rows_from_csv(path)


# --------------------------------------------------------------------------- #
# Generated SQL, per backend
# --------------------------------------------------------------------------- #


def test_placeholder_styles_per_backend() -> None:
    assert get_backend("sqlite").placeholders(3) == "?, ?, ?"
    assert get_backend("postgres").placeholders(3) == "%s, %s, %s"
    assert get_backend("mysql").placeholders(3) == "%s, %s, %s"
    assert get_backend("oracle").placeholders(3) == ":1, :2, :3"
    assert get_backend("snowflake").placeholders(3) == "%s, %s, %s"


def test_identifier_quoting_per_backend() -> None:
    assert get_backend("postgres").quote_identifier("order") == '"order"'
    assert get_backend("mysql").quote_identifier("order") == "`order`"
    assert get_backend("postgres").quote_identifier("s.t") == '"s"."t"'
    assert get_backend("postgres").quote_identifier('we"ird') == '"we""ird"'


def test_upsert_dialects() -> None:
    """Every backend spells this differently; the caller should not have to care."""
    columns, keys = ["id", "name"], ["id"]
    assert "ON CONFLICT" in get_backend("sqlite").upsert_sql("t", columns, keys)
    assert "excluded" in get_backend("sqlite").upsert_sql("t", columns, keys)
    assert "EXCLUDED" in get_backend("postgres").upsert_sql("t", columns, keys)
    assert "ON DUPLICATE KEY UPDATE" in get_backend("mysql").upsert_sql("t", columns, keys)
    assert "MERGE INTO" in get_backend("oracle").upsert_sql("t", columns, keys)
    assert "MERGE INTO" in get_backend("snowflake").upsert_sql("t", columns, keys)


def test_merge_statements_bind_each_value_exactly_once() -> None:
    """Oracle and Snowflake MERGE must stay compatible with executemany."""
    for name, token in (("oracle", ":"), ("snowflake", "%s")):
        sql = get_backend(name).upsert_sql("t", ["id", "name", "score"], ["id"])
        count = sql.count("%s") if token == "%s" else len([p for p in (":1", ":2", ":3") if p in sql])
        assert count == 3, f"{name} bound {count} parameters for 3 columns"


def test_upsert_with_only_key_columns_becomes_do_nothing() -> None:
    assert "DO NOTHING" in get_backend("sqlite").upsert_sql("t", ["id"], ["id"])


def test_bulk_result_summary_is_readable() -> None:
    result = BulkResult(table="t", rows_written=50_000, chunks=10, seconds=2.0)
    text = result.summary()
    assert "50,000" in text and "25,000 rows/s" in text and "executemany" in text
    assert result.rows_per_second == 25_000
