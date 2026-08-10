#!/usr/bin/env python3
"""Example 1: connect, write, read, transact.

Runs against SQLite in a temporary directory, so it needs no database, no
credentials and no setup::

    python3 examples/01_basic_crud.py

Everything here is backend-agnostic. Point the same code at PostgreSQL by
changing ``backend`` and ``host`` in the configuration - the calls do not change,
only the placeholder style does, and the library handles that.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydbconnect import Connection, ConnectionConfig
from pydbconnect.exceptions import QueryError


def banner(title: str) -> None:
    """Print a labelled section header."""
    print(f"\n=== {title} " + "=" * max(0, 58 - len(title)))


def main() -> int:
    """Run the example and return a process exit code."""
    with tempfile.TemporaryDirectory(prefix="pydbconnect-example-") as tmp:
        config = ConnectionConfig(
            name="demo",
            backend="sqlite",
            database=str(Path(tmp) / "demo.db"),
            options={"journal_mode": "WAL", "foreign_keys": True},
        ).validate()

        banner("1. Open a connection")
        print(f"config : {config!r}")

        # The context manager closes on every path, including exceptions.
        with Connection.open(config) as conn:
            print(f"opened : {conn!r}")
            print(f"alive  : {conn.ping()}")

            banner("2. Create a table")
            conn.execute(
                "CREATE TABLE orders ("
                "  id       INTEGER PRIMARY KEY,"
                "  customer TEXT    NOT NULL,"
                "  region   TEXT    NOT NULL,"
                "  amount   REAL    NOT NULL,"
                "  status   TEXT    NOT NULL DEFAULT 'new')"
            )
            print("created: orders")

            banner("3. Insert rows - parameters, never string formatting")
            rows = [
                (1, "northwind", "EMEA", 1250.00, "shipped"),
                (2, "contoso", "AMER", 890.50, "new"),
                (3, "fabrikam", "EMEA", 4310.75, "shipped"),
                (4, "northwind", "APAC", 220.00, "cancelled"),
            ]
            written = conn.executemany(
                "INSERT INTO orders (id, customer, region, amount, status) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            print(f"inserted: {written} row(s) in a single round trip")

            banner("4. Query - rows come back as dicts")
            for row in conn.query(
                "SELECT id, customer, region, amount FROM orders "
                "WHERE region = ? ORDER BY amount DESC",
                ("EMEA",),
            ):
                print(f"  #{row['id']} {row['customer']:<10} "
                      f"{row['region']} {row['amount']:>9,.2f}")

            banner("5. query_one and scalar")
            summary = conn.query_one(
                "SELECT count(*) AS orders, sum(amount) AS total FROM orders"
            )
            print(f"summary: {summary}")
            print(f"scalar : {conn.scalar('SELECT max(amount) FROM orders')}")
            print(f"missing: {conn.query_one('SELECT * FROM orders WHERE id = ?', (99,))}")

            banner("6. Transaction - commits as one unit")
            with conn.transaction():
                conn.execute("UPDATE orders SET status = ? WHERE status = ?", ("open", "new"))
                conn.execute("UPDATE orders SET amount = amount * ? WHERE region = ?", (1.1, "APAC"))
            print(f"statuses: {[r['status'] for r in conn.query('SELECT status FROM orders ORDER BY id')]}")

            banner("7. Transaction - rolls back as one unit")
            before = conn.scalar("SELECT count(*) FROM orders")
            try:
                with conn.transaction():
                    conn.execute(
                        "INSERT INTO orders (id, customer, region, amount) VALUES (?, ?, ?, ?)",
                        (5, "adventureworks", "AMER", 75.00),
                    )
                    raise RuntimeError("downstream validation failed")
            except RuntimeError as exc:
                print(f"caught  : {exc}")
            after = conn.scalar("SELECT count(*) FROM orders")
            print(f"rows    : {before} before, {after} after - the insert was undone")

            banner("8. Errors are typed and name the connection")
            try:
                conn.query("SELECT * FROM a_table_that_does_not_exist")
            except QueryError as exc:
                print(f"caught  : {exc}")
                print(f"cause   : {type(exc.__cause__).__name__}")

        banner("9. Closed on exit")
        print(f"closed  : {conn.closed}")
        print("\nThe connection closed on every path, including the exception in step 8.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
