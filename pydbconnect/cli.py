"""Command line interface: ``pydb``.

::

    pydb ping warehouse
    pydb query warehouse --sql "SELECT count(*) AS n FROM orders" --format json
    pydb query warehouse --sql "SELECT * FROM orders WHERE region = ?" -p EMEA
    pydb load warehouse --table staging_orders --file orders.csv --chunk 5000
    pydb config validate
    pydb config list

Exit codes, chosen so a shell script can branch on them:

====  ==========================================================
Code  Meaning
====  ==========================================================
0     Success.
1     Operational failure - the database refused, the load broke,
      the network went away. Retrying might work.
2     Configuration error - a missing key, an unknown backend, an
      unresolvable secret. Retrying will not work.
130   Interrupted.
====  ==========================================================

Nothing here prints a credential. Configuration is rendered through
:meth:`~pydbconnect.config.ConnectionConfig.to_dict`, which redacts, and every
error message goes through :func:`~pydbconnect.secrets.redact` on its way out.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, TextIO

from . import __version__
from .bulk import bulk_insert, rows_from_csv, upsert
from .config import ConnectionConfig, load_config
from .connection import Connection
from .exceptions import ConfigurationError, PyDBError
from .registry import describe_backends
from .secrets import RedactingFilter, redact

__all__ = ["main", "build_parser"]

log = logging.getLogger("pydbconnect.cli")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2
EXIT_INTERRUPTED = 130


# --------------------------------------------------------------------------- #
# Output formatting
# --------------------------------------------------------------------------- #


def _render_table(rows: Sequence[Dict[str, Any]], stream: TextIO) -> None:
    """Print rows as a fixed-width table.

    Values are truncated at 60 characters so one wide JSON column cannot make
    the output unreadable.
    """
    if not rows:
        print("(0 rows)", file=stream)
        return
    columns = list(rows[0].keys())
    cells = [[_cell(row.get(c)) for c in columns] for row in rows]
    widths = [
        min(60, max(len(col), *(len(r[i]) for r in cells)))
        for i, col in enumerate(columns)
    ]
    header = " | ".join(col[:w].ljust(w) for col, w in zip(columns, widths))
    print(header, file=stream)
    print("-+-".join("-" * w for w in widths), file=stream)
    for row in cells:
        print(" | ".join(v[:w].ljust(w) for v, w in zip(row, widths)), file=stream)
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})", file=stream)


def _cell(value: Any) -> str:
    """Render one value for the table format."""
    if value is None:
        return "NULL"
    return str(value)


def _render_json(rows: Sequence[Dict[str, Any]], stream: TextIO) -> None:
    """Print rows as pretty JSON, with a fallback for non-serialisable values."""
    json.dump(rows, stream, indent=2, default=str, ensure_ascii=False)
    stream.write("\n")


def _render_csv(rows: Sequence[Dict[str, Any]], stream: TextIO) -> None:
    """Print rows as CSV with a header."""
    if not rows:
        return
    writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)


_RENDERERS: Dict[str, Callable[[Sequence[Dict[str, Any]], TextIO], None]] = {
    "table": _render_table,
    "json": _render_json,
    "csv": _render_csv,
}


def _parse_param(raw: str) -> Any:
    """Convert a ``--param`` value.

    Plain values stay strings, because guessing types silently turns a zip code
    into an integer and drops its leading zero. Prefixes make intent explicit:

    * ``int:42`` -> ``42``
    * ``float:1.5`` -> ``1.5``
    * ``bool:true`` -> ``True``
    * ``null`` -> ``None``
    * ``str:null`` -> the literal string ``"null"``
    """
    if raw == "null":
        return None
    for prefix, caster in (
        ("int:", int), ("float:", float), ("str:", str),
        ("bool:", lambda v: v.strip().lower() in {"1", "true", "yes", "on"}),
    ):
        if raw.startswith(prefix):
            return caster(raw[len(prefix):])
    return raw


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


def _resolve(args: argparse.Namespace, name: str) -> ConnectionConfig:
    """Resolve one connection through the configuration layers."""
    return load_config(args.config, profile=args.profile).get(name)


def cmd_ping(args: argparse.Namespace) -> int:
    """Open a connection, run the backend's liveness check, and report."""
    config = _resolve(args, args.connection)
    with Connection.open(config) as conn:
        alive = conn.ping()
    target = config.host or config.database or "(local)"
    if alive:
        print(f"ok   {config.name} -> {config.backend}://{target}")
        return EXIT_OK
    print(f"FAIL {config.name} -> {config.backend}://{target}", file=sys.stderr)
    return EXIT_FAILURE


def cmd_query(args: argparse.Namespace) -> int:
    """Run one statement and render the result."""
    sql = args.sql
    if args.file:
        with open(args.file, encoding="utf-8") as handle:
            sql = handle.read()
    if not sql or not sql.strip():
        raise ConfigurationError("provide a statement with --sql or --file")

    params = [_parse_param(p) for p in (args.param or [])] or None
    config = _resolve(args, args.connection)
    with Connection.open(config) as conn:
        if _is_select(sql):
            rows = conn.query(sql, params)
            if args.limit and len(rows) > args.limit:
                rows = rows[: args.limit]
            _RENDERERS[args.format](rows, sys.stdout)
        else:
            affected = conn.execute(sql, params)
            print(f"{affected} row(s) affected")
    return EXIT_OK


def _is_select(sql: str) -> bool:
    """Whether a statement returns rows.

    ``WITH``, ``SHOW``, ``DESCRIBE``, ``EXPLAIN``, ``PRAGMA`` and ``VALUES`` all
    produce a result set, so all of them render as a table.
    """
    first = sql.strip().split(None, 1)[0].lower() if sql.strip() else ""
    return first in {"select", "with", "show", "describe", "desc", "explain", "pragma", "values"}


def cmd_load(args: argparse.Namespace) -> int:
    """Load a CSV file into a table with chunked ``executemany``."""
    columns, rows = rows_from_csv(
        args.file,
        delimiter=args.delimiter,
        encoding=args.encoding,
        null_marker=args.null_marker,
    )
    config = _resolve(args, args.connection)

    def progress(written: int, chunk_rows: int) -> None:  # noqa: ARG001
        print(f"  ... {written:,} row(s)", file=sys.stderr, flush=True)

    reporter = None if args.quiet else progress
    with Connection.open(config) as conn:
        if args.truncate:
            table = conn.backend.quote_identifier(args.table)
            conn.execute(f"DELETE FROM {table}")
            print(f"truncated {args.table}", file=sys.stderr)
        if args.upsert_key:
            result = upsert(
                conn, args.table, rows,
                key_columns=args.upsert_key, columns=columns,
                chunk_size=args.chunk, on_progress=reporter,
            )
        else:
            result = bulk_insert(
                conn, args.table, rows, columns=columns,
                chunk_size=args.chunk, on_progress=reporter,
            )
    print(result.summary())
    return EXIT_OK


def cmd_config_validate(args: argparse.Namespace) -> int:
    """Validate every connection, or one named connection.

    Reports each connection on its own line and returns a non-zero exit code if
    any of them is invalid, so this can gate a deployment.
    """
    config_file = load_config(args.config, profile=args.profile)
    names = [args.connection] if args.connection else config_file.names()
    if not names:
        where = str(config_file.path) if config_file.path else "the search path"
        print(f"no connections defined in {where}", file=sys.stderr)
        return EXIT_CONFIG

    failures = 0
    for name in names:
        try:
            config = config_file.get(name)
        except PyDBError as exc:
            failures += 1
            print(f"FAIL {name}: {exc}", file=sys.stderr)
            continue
        detail = config.host or config.database or ""
        print(f"ok   {name} -> {config.backend}{' ' + detail if detail else ''}")

    if config_file.path:
        print(f"\n{len(names) - failures}/{len(names)} valid in {config_file.path}")
    return EXIT_CONFIG if failures else EXIT_OK


def cmd_config_list(args: argparse.Namespace) -> int:
    """List configured connections, or the registered backends."""
    if args.backends:
        backend_rows = describe_backends()
        if args.format == "json":
            _render_json(backend_rows, sys.stdout)
        else:
            _render_table(
                [
                    {
                        "backend": r.get("name", ""),
                        "driver": r.get("driver", ""),
                        "extra": r.get("extra", ""),
                        "installed": _yes_no(r.get("installed")),
                        "param": r.get("placeholder", ""),
                        "copy": _yes_no(r.get("copy")),
                        "upsert": _yes_no(r.get("upsert")),
                        "txn": _yes_no(r.get("transactions")),
                    }
                    for r in backend_rows
                ],
                sys.stdout,
            )
        return EXIT_OK

    config_file = load_config(args.config, profile=args.profile)
    rows: List[Dict[str, Any]] = []
    for name in config_file.names():
        try:
            config = config_file.get(name, validate=False)
            data = config.to_dict(redacted=True)
            rows.append(
                {
                    "name": name,
                    "backend": data.get("backend"),
                    "host": data.get("host") or "",
                    "database": data.get("database") or "",
                    "schema": data.get("schema") or "",
                    "user": data.get("user") or "",
                    "secret": data.get("secret") or "",
                }
            )
        except PyDBError as exc:
            rows.append({"name": name, "backend": f"ERROR: {exc}"})

    if args.format == "json":
        _render_json(rows, sys.stdout)
    else:
        source = str(config_file.path) if config_file.path else "environment only"
        profile = config_file.profile or "(none)"
        print(f"config: {source}   profile: {profile}\n")
        _render_table(rows, sys.stdout)
    return EXIT_OK


def _yes_no(value: Any) -> str:
    return "yes" if value else "no"


# --------------------------------------------------------------------------- #
# Parser
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for ``pydb``."""
    parser = argparse.ArgumentParser(
        prog="pydb",
        description=(
            "Config-driven database connectivity. Credentials come from the "
            "environment or a secret store, never from the command line."
        ),
        epilog=(
            "Exit codes: 0 success, 1 operational failure, 2 configuration error."
        ),
    )
    parser.add_argument("--version", action="version", version=f"pydb-connect {__version__}")
    parser.add_argument(
        "-c", "--config", metavar="PATH",
        help="configuration file (default: $PYDB_CONFIG_FILE, then ./connections.yaml)",
    )
    parser.add_argument(
        "--profile", metavar="NAME",
        help="profile to activate, e.g. dev or prod (default: $PYDB_PROFILE)",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0,
        help="log at INFO; repeat for DEBUG",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="suppress progress output",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    ping = subparsers.add_parser("ping", help="check that a connection works")
    ping.add_argument("connection", help="connection name")
    ping.set_defaults(handler=cmd_ping)

    query = subparsers.add_parser("query", help="run a statement")
    query.add_argument("connection", help="connection name")
    query.add_argument("--sql", help="statement to run")
    query.add_argument("--file", metavar="PATH", help="read the statement from a file")
    query.add_argument(
        "-p", "--param", action="append", metavar="VALUE",
        help="bind parameter, in order. Prefix with int:, float:, bool: or use "
             "'null' to control the type",
    )
    query.add_argument(
        "-f", "--format", choices=sorted(_RENDERERS), default="table",
        help="output format (default: table)",
    )
    query.add_argument("--limit", type=int, help="print at most this many rows")
    query.set_defaults(handler=cmd_query)

    load = subparsers.add_parser("load", help="load a CSV file into a table")
    load.add_argument("connection", help="connection name")
    load.add_argument("--table", required=True, help="target table")
    load.add_argument("--file", required=True, metavar="PATH", help="CSV file to load")
    load.add_argument(
        "--chunk", type=int, default=1000, metavar="N",
        help="rows per executemany batch (default: 1000)",
    )
    load.add_argument(
        "--upsert-key", action="append", metavar="COLUMN",
        help="upsert on this key column instead of inserting; repeatable",
    )
    load.add_argument("--truncate", action="store_true", help="delete existing rows first")
    load.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    load.add_argument("--encoding", default="utf-8", help="file encoding (default: utf-8)")
    load.add_argument(
        "--null-marker", default="", metavar="TEXT",
        help="CSV value treated as SQL NULL (default: empty string)",
    )
    load.set_defaults(handler=cmd_load)

    config = subparsers.add_parser("config", help="inspect configuration")
    config_sub = config.add_subparsers(dest="config_command", metavar="SUBCOMMAND")

    validate = config_sub.add_parser("validate", help="validate every connection")
    validate.add_argument("connection", nargs="?", help="validate only this connection")
    validate.set_defaults(handler=cmd_config_validate)

    listing = config_sub.add_parser("list", help="list connections or backends")
    listing.add_argument(
        "--backends", action="store_true",
        help="list registered backends and which drivers are installed",
    )
    listing.add_argument(
        "-f", "--format", choices=("table", "json"), default="table",
        help="output format (default: table)",
    )
    listing.set_defaults(handler=cmd_config_list)
    # 'pydb config' with no subcommand: argparse hands us the namespace, which
    # this handler does not need.
    config.set_defaults(handler=lambda a: _missing_subcommand(config))  # noqa: ARG005

    return parser


def _missing_subcommand(parser: argparse.ArgumentParser) -> int:
    parser.print_help(sys.stderr)
    return EXIT_CONFIG


def _configure_logging(verbosity: int) -> None:
    """Set up logging with secret redaction on the handler.

    The filter goes on the handler rather than the logger so that records from
    driver libraries - which log to their own loggers - are scrubbed too.
    """
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)-7s %(name)s: %(message)s"))
    handler.addFilter(RedactingFilter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for the ``pydb`` command.

    Args:
        argv: Arguments, defaulting to ``sys.argv[1:]``.

    Returns:
        A process exit code: 0 success, 1 operational failure, 2 configuration
        error, 130 interrupted.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "handler", None):
        parser.print_help(sys.stderr)
        return EXIT_CONFIG

    _configure_logging(args.verbose)

    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except PyDBError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if args.verbose >= 2:
            import traceback

            traceback.print_exc()
        return int(getattr(exc, "exit_code", EXIT_FAILURE))
    except FileNotFoundError as exc:
        print(f"error: file not found: {exc.filename}", file=sys.stderr)
        return EXIT_CONFIG
    except BrokenPipeError:  # pragma: no cover - `pydb query ... | head`
        return EXIT_OK
    except Exception as exc:  # noqa: BLE001 - top level of a CLI: report, never traceback-dump
        print(f"error: {type(exc).__name__}: {redact(str(exc))}", file=sys.stderr)
        if args.verbose >= 1:
            import traceback

            traceback.print_exc()
        return EXIT_FAILURE


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
