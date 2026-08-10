#!/usr/bin/env python3
"""Example 4: copy a table between two connections, in bounded memory.

Runs against two SQLite databases in a temporary directory::

    python3 examples/04_multi_backend_copy.py

Both ends are SQLite here **only so the example runs with no setup**. The code
is backend-agnostic: the source could be Oracle and the target Snowflake, and
not one line below would change. That is the point of the abstraction - the
placeholder style, the identifier quoting, the upsert dialect and the retry
classification are all the backend's problem, not the pipeline's.

The pattern is the one most data movement jobs need:

    read a chunk -> transform it -> write it -> repeat

Memory stays flat because only one chunk exists at a time. Restartability comes
from the target being upserted on a key, so re-running after a failure converges
instead of duplicating.
"""

from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydbconnect import (
    Connection,
    ConnectionConfig,
    bulk_insert,
    chunked_read,
    upsert,
)

SOURCE_ROWS = 50_000
CHUNK = 5_000


def banner(title: str) -> None:
    """Print a labelled section header."""
    print(f"\n=== {title} " + "=" * max(0, 58 - len(title)))


def generate(count: int) -> Iterator[Dict[str, Any]]:
    """Yield synthetic source rows lazily."""
    regions = ("EMEA", "AMER", "APAC")
    for i in range(1, count + 1):
        yield {
            "order_id": i,
            "customer": f"customer-{i % 1_499:04d}",
            "region": regions[i % 3],
            "amount_cents": (i * 137) % 500_000,
            "status": "shipped" if i % 7 else "cancelled",
        }


def transform(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Reshape a batch on its way from source to target.

    Real pipelines do this: rename columns, fix units, drop rows, add lineage.
    Doing it a batch at a time keeps memory bounded and keeps the transformation
    testable as a pure function.
    """
    return [
        {
            "order_id": row["order_id"],
            "customer": row["customer"].upper(),
            "region": row["region"],
            "amount": round(row["amount_cents"] / 100, 2),
            "is_cancelled": 1 if row["status"] == "cancelled" else 0,
        }
        for row in batch
        if row["amount_cents"] > 0
    ]


def main() -> int:
    """Run the example and return a process exit code."""
    with tempfile.TemporaryDirectory(prefix="pydbconnect-example-") as tmp:
        source_config = ConnectionConfig(
            name="source", backend="sqlite", database=str(Path(tmp) / "source.db"),
        ).validate()
        # In a real pipeline this second config would name a different backend
        # entirely - postgres, snowflake, oracle. Nothing below would change.
        target_config = ConnectionConfig(
            name="target", backend="sqlite", database=str(Path(tmp) / "target.db"),
        ).validate()

        banner("1. Two independent connections")
        print(f"  source: {source_config.backend}://{source_config.database}")
        print(f"  target: {target_config.backend}://{target_config.database}")

        with Connection.open(source_config) as source:
            banner("2. Seed the source")
            source.execute(
                "CREATE TABLE orders ("
                "  order_id INTEGER PRIMARY KEY, customer TEXT, region TEXT,"
                "  amount_cents INTEGER, status TEXT)"
            )
            seeded = bulk_insert(source, "orders", generate(SOURCE_ROWS), chunk_size=10_000)
            print(f"  {seeded.summary()}")

            with Connection.open(target_config) as target:
                banner("3. Prepare the target")
                target.execute(
                    "CREATE TABLE orders_clean ("
                    "  order_id     INTEGER PRIMARY KEY,"
                    "  customer     TEXT NOT NULL,"
                    "  region       TEXT NOT NULL,"
                    "  amount       REAL NOT NULL,"
                    "  is_cancelled INTEGER NOT NULL)"
                )
                print("  created: orders_clean")

                banner(f"4. Copy in chunks of {CHUNK:,}")
                started = time.monotonic()
                moved = 0
                dropped = 0
                for batch_number, batch in enumerate(
                    chunked_read(
                        source,
                        "SELECT order_id, customer, region, amount_cents, status "
                        "FROM orders ORDER BY order_id",
                        chunk_size=CHUNK,
                    ),
                    start=1,
                ):
                    cleaned = transform(batch)
                    dropped += len(batch) - len(cleaned)
                    result = bulk_insert(target, "orders_clean", cleaned, chunk_size=CHUNK)
                    moved += result.rows_written
                    print(f"  batch {batch_number:>2}: read {len(batch):>6,}  "
                          f"wrote {result.rows_written:>6,}  running total {moved:>7,}")
                elapsed = time.monotonic() - started
                print(f"  {moved:,} rows in {elapsed:.2f}s "
                      f"({moved / elapsed:,.0f} rows/s), {dropped} filtered out")

                banner("5. Verify")
                source_count = source.scalar("SELECT count(*) FROM orders WHERE amount_cents > 0")
                target_count = target.scalar("SELECT count(*) FROM orders_clean")
                print(f"  source rows (after filter): {source_count:,}")
                print(f"  target rows              : {target_count:,}")
                print(f"  match                    : {source_count == target_count}")
                print(f"  sample                   : "
                      f"{target.query_one('SELECT * FROM orders_clean WHERE order_id = ?', (7,))}")

                banner("6. Re-run safely with upsert")
                # A pipeline that cannot be re-run is a pipeline that will page
                # you at 3am. Upserting on the key makes the copy idempotent.
                changed = [
                    {"order_id": 1, "customer": "RESTATED", "region": "EMEA",
                     "amount": 1.11, "is_cancelled": 0},
                    {"order_id": SOURCE_ROWS + 1, "customer": "LATE-ARRIVING",
                     "region": "APAC", "amount": 2.22, "is_cancelled": 0},
                ]
                merge = upsert(target, "orders_clean", changed, key_columns=["order_id"])
                print(f"  {merge.summary()}")
                print(f"  updated: {target.query_one('SELECT customer, amount FROM orders_clean WHERE order_id = ?', (1,))}")
                print(f"  inserted: {target.query_one('SELECT customer, amount FROM orders_clean WHERE order_id = ?', (SOURCE_ROWS + 1,))}")
                print(f"  total rows: {target.scalar('SELECT count(*) FROM orders_clean'):,}")

                banner("7. Aggregate check on the target")
                for row in target.query(
                    "SELECT region, count(*) AS orders, round(sum(amount), 2) AS total "
                    "FROM orders_clean GROUP BY region ORDER BY region"
                ):
                    print(f"  {row['region']:<6} {row['orders']:>7,} orders  "
                          f"{row['total']:>14,.2f}")

        print("\nOne chunk existed at a time, so peak memory was set by "
              "chunk_size and not by the size of the table.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
