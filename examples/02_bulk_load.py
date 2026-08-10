#!/usr/bin/env python3
"""Example 2: bulk load, and why the per-row loop is the problem.

Runs against SQLite in a temporary directory::

    python3 examples/02_bulk_load.py

The timings below are real, measured on whatever machine you run this on. They
understate the difference badly: SQLite is a local file, so a per-row loop pays
no network latency. Against a database across a network, each ``execute`` costs
a full round trip - at 2ms that is 33 minutes for a million rows, versus seconds
for the same rows in batches.
"""

from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydbconnect import Connection, ConnectionConfig, bulk_insert, copy_from, upsert

ROW_COUNT = 20_000


def banner(title: str) -> None:
    """Print a labelled section header."""
    print(f"\n=== {title} " + "=" * max(0, 58 - len(title)))


def generate(count: int, offset: int = 0) -> Iterator[Dict[str, Any]]:
    """Yield rows lazily.

    A generator, not a list: the source is never materialised, so this works
    identically for twenty thousand rows and for two hundred million.
    """
    regions = ("EMEA", "AMER", "APAC")
    for i in range(offset + 1, offset + count + 1):
        yield {
            "id": i,
            "customer": f"customer-{i % 997:03d}",
            "region": regions[i % 3],
            "amount": round((i * 37) % 9973 / 100, 2),
        }


def create_table(conn: Connection, name: str) -> None:
    """Create the target table, dropping any previous version."""
    conn.execute(f"DROP TABLE IF EXISTS {name}")
    conn.execute(
        f"CREATE TABLE {name} ("
        "  id       INTEGER PRIMARY KEY,"
        "  customer TEXT NOT NULL,"
        "  region   TEXT NOT NULL,"
        "  amount   REAL NOT NULL)"
    )


def main() -> int:
    """Run the example and return a process exit code."""
    with tempfile.TemporaryDirectory(prefix="pydbconnect-example-") as tmp:
        config = ConnectionConfig(
            name="loader", backend="sqlite", database=str(Path(tmp) / "bulk.db"),
        ).validate()

        with Connection.open(config) as conn:
            # -- the slow way ------------------------------------------------ #
            sample = list(generate(2_000))
            insert_sql = (
                "INSERT INTO orders_slow (id, customer, region, amount) "
                "VALUES (?, ?, ?, ?)"
            )
            values = [(r["id"], r["customer"], r["region"], r["amount"]) for r in sample]

            banner("1a. The anti-pattern: execute() + commit() per row")
            create_table(conn, "orders_slow")
            # Only 500 rows: at this rate, 2,000 would keep you waiting.
            worst_sample = values[:500]
            started = time.monotonic()
            for row in worst_sample:
                conn.execute(insert_sql, row)      # autocommits each statement
            worst_seconds = time.monotonic() - started
            worst_rate = len(worst_sample) / worst_seconds
            print(f"  {len(worst_sample):,} rows, one execute() and one commit() each")
            print(f"  {worst_seconds:6.3f}s  ->  {worst_rate:>10,.0f} rows/s")
            print("  every row pays a round trip and an fsync")
            print(f"  at this rate, 1,000,000 rows would take "
                  f"{1_000_000 / worst_rate / 3600:,.1f} hours")

            banner("1b. Better: the same loop inside one transaction")
            create_table(conn, "orders_slow")
            started = time.monotonic()
            with conn.transaction():
                for row in values:
                    conn.execute(insert_sql, row)
            loop_seconds = time.monotonic() - started
            loop_rate = len(values) / loop_seconds
            print(f"  {len(values):,} rows, one execute() each, a single commit")
            print(f"  {loop_seconds:6.3f}s  ->  {loop_rate:>10,.0f} rows/s")
            print(f"  {loop_rate / worst_rate:,.0f}x the throughput of 1a: "
                  f"one fsync for the batch instead of one per row")
            print("  still one network round trip per row, which SQLite hides "
                  "and a remote database does not")

            # -- the fast way ------------------------------------------------ #
            banner("2. bulk_insert: chunked executemany")
            create_table(conn, "orders")
            started = time.monotonic()
            result = bulk_insert(conn, "orders", generate(ROW_COUNT), chunk_size=5_000)
            print(f"  {result.summary()}")
            print(f"  {result.rows_per_second / worst_rate:>10,.0f}x faster than 1a")
            print(f"  {result.rows_per_second / loop_rate:>10,.1f}x faster than 1b "
                  f"on a local file; the gap widens with every millisecond of "
                  f"network latency, because this sends "
                  f"{ROW_COUNT // 5_000} batches instead of {ROW_COUNT:,} statements")

            # -- progress ---------------------------------------------------- #
            banner("3. Progress reporting")
            create_table(conn, "orders_watched")
            marks: List[str] = []

            def on_progress(written: int, chunk_rows: int) -> None:  # noqa: ARG001
                marks.append(f"{written:,}")

            bulk_insert(
                conn, "orders_watched", generate(10_000),
                chunk_size=2_500, on_progress=on_progress,
            )
            print(f"  reported after each chunk: {', '.join(marks)} rows")

            # -- chunk size -------------------------------------------------- #
            banner("4. Chunk size matters, but with diminishing returns")
            for chunk_size in (100, 1_000, 5_000, 20_000):
                create_table(conn, "orders_tuning")
                started = time.monotonic()
                bulk_insert(conn, "orders_tuning", generate(ROW_COUNT), chunk_size=chunk_size)
                elapsed = time.monotonic() - started
                print(f"  chunk_size {chunk_size:>6,}: {elapsed:6.3f}s  "
                      f"{ROW_COUNT / elapsed:>10,.0f} rows/s")

            # -- upsert ------------------------------------------------------ #
            banner("5. upsert: insert new rows, update existing ones")
            before = conn.query_one(
                "SELECT count(*) AS n, sum(amount) AS total FROM orders"
            )
            print(f"  before: {before['n']:,} rows, total {before['total']:,.2f}")
            changes = [
                {"id": 1, "customer": "customer-001", "region": "EMEA", "amount": 999.99},
                {"id": 2, "customer": "customer-002", "region": "EMEA", "amount": 888.88},
                {"id": ROW_COUNT + 1, "customer": "brand-new", "region": "APAC", "amount": 42.00},
            ]
            upsert_result = upsert(conn, "orders", changes, key_columns=["id"])
            after = conn.query_one("SELECT count(*) AS n, sum(amount) AS total FROM orders")
            print(f"  {upsert_result.summary()}")
            print(f"  after : {after['n']:,} rows, total {after['total']:,.2f}")
            print(f"  row 1 : {conn.query_one('SELECT * FROM orders WHERE id = ?', (1,))}")
            print(f"  new   : {conn.query_one('SELECT * FROM orders WHERE id = ?', (ROW_COUNT + 1,))}")

            # -- copy_from --------------------------------------------------- #
            banner("6. copy_from: native bulk path where the backend has one")
            create_table(conn, "orders_copy")
            copy_result = copy_from(conn, "orders_copy", generate(5_000), chunk_size=5_000)
            print(f"  {copy_result.summary()}")
            print(f"  sqlite has no COPY, so this fell back honestly: "
                  f"method={copy_result.method!r}")
            print("  on postgres the same call issues COPY ... FROM STDIN")

            banner("7. Final state")
            for table in ("orders_slow", "orders", "orders_watched", "orders_copy"):
                count = conn.scalar(f"SELECT count(*) FROM {table}")
                print(f"  {table:<16} {count:>8,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
