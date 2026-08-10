#!/usr/bin/env python3
"""Example 3: reading a large table without loading it into memory.

Runs against SQLite in a temporary directory::

    python3 examples/03_chunked_read.py

``cursor.fetchall()`` on a big table is the read-side twin of the per-row insert
loop: it works in development against ten thousand rows and gets the process
killed in production against ten million. This example measures the difference
with :mod:`tracemalloc`, so the numbers below are real allocations, not
estimates.
"""

from __future__ import annotations

import sys
import tempfile
import tracemalloc
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydbconnect import Connection, ConnectionConfig, bulk_insert, chunked_read

ROW_COUNT = 100_000


def banner(title: str) -> None:
    """Print a labelled section header."""
    print(f"\n=== {title} " + "=" * max(0, 58 - len(title)))


def generate(count: int) -> Iterator[Dict[str, Any]]:
    """Yield synthetic event rows lazily."""
    kinds = ("click", "view", "purchase", "refund")
    for i in range(1, count + 1):
        yield {
            "id": i,
            "kind": kinds[i % 4],
            "user_id": i % 5_000,
            "value": round((i * 13) % 10_007 / 100, 2),
            "payload": f"event-payload-{i:08d}-padding-to-make-rows-realistic",
        }


def megabytes(byte_count: int) -> str:
    """Format a byte count as megabytes."""
    return f"{byte_count / 1_048_576:6.1f} MB"


def main() -> int:
    """Run the example and return a process exit code."""
    with tempfile.TemporaryDirectory(prefix="pydbconnect-example-") as tmp:
        config = ConnectionConfig(
            name="events", backend="sqlite", database=str(Path(tmp) / "events.db"),
        ).validate()

        with Connection.open(config) as conn:
            banner("1. Build a table worth streaming")
            conn.execute(
                "CREATE TABLE events ("
                "  id INTEGER PRIMARY KEY, kind TEXT, user_id INTEGER,"
                "  value REAL, payload TEXT)"
            )
            result = bulk_insert(conn, "events", generate(ROW_COUNT), chunk_size=10_000)
            print(f"  {result.summary()}")

            banner("2. query() - the whole result set in memory at once")
            tracemalloc.start()
            rows = conn.query("SELECT * FROM events")
            _, eager_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            eager_total = sum(row["value"] for row in rows)
            print(f"  rows in memory : {len(rows):,}")
            print(f"  peak memory    : {megabytes(eager_peak)}")
            del rows

            banner("3. chunked_read() - one batch at a time")
            tracemalloc.start()
            chunk_total = 0.0
            batches = 0
            largest_batch = 0
            for batch in chunked_read(conn, "SELECT * FROM events", chunk_size=5_000):
                batches += 1
                largest_batch = max(largest_batch, len(batch))
                chunk_total += sum(row["value"] for row in batch)
            _, chunked_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            print(f"  batches        : {batches}")
            print(f"  largest batch  : {largest_batch:,} rows")
            print(f"  peak memory    : {megabytes(chunked_peak)}")
            print(f"  reduction      : {eager_peak / max(chunked_peak, 1):.1f}x less memory")
            print(f"  same answer    : {eager_total:,.2f} == {chunk_total:,.2f} "
                  f"-> {abs(eager_total - chunk_total) < 0.01}")

            banner("4. stream() - row at a time, still chunked underneath")
            counts: Counter = Counter()
            for row in conn.stream("SELECT kind FROM events", chunk_size=10_000):
                counts[row["kind"]] += 1
            for kind, count in sorted(counts.items()):
                print(f"  {kind:<10} {count:>8,}")

            banner("5. Streaming with a parameter, aggregating as you go")
            running = {"rows": 0, "value": 0.0}
            for batch in chunked_read(
                conn,
                "SELECT id, value FROM events WHERE kind = ? AND value > ?",
                ("purchase", 50.0),
                chunk_size=2_500,
            ):
                running["rows"] += len(batch)
                running["value"] += sum(row["value"] for row in batch)
            print(f"  matching rows  : {running['rows']:,}")
            print(f"  total value    : {running['value']:,.2f}")
            print(f"  mean value     : {running['value'] / max(running['rows'], 1):,.2f}")

            banner("6. Early exit costs nothing")
            first_ten = []
            for batch in chunked_read(conn, "SELECT id FROM events ORDER BY id", chunk_size=1_000):
                first_ten.extend(row["id"] for row in batch)
                break                      # the rest is never fetched
            print(f"  fetched {len(first_ten):,} rows, then stopped")
            print(f"  first five: {first_ten[:5]}")

            print("\nchunked_read holds one batch at a time, so memory is a "
                  "function of chunk_size, not of table size.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
