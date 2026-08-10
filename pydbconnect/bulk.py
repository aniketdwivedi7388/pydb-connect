"""Bulk load and chunked read.

The pattern this replaces
------------------------

::

    for row in rows:                                    # 500,000 iterations
        cursor.execute("INSERT INTO t VALUES (%s, %s)", row)
        conn.commit()                                   # 500,000 commits

Every iteration pays a full network round trip, and every ``commit`` pays an
fsync. At a 2ms round trip that loop needs sixteen minutes to move half a
million rows that ``executemany`` in batches of 5000 moves in seconds. The fix
is not "make the loop faster"; it is to stop making one round trip per row.

Three tiers, fastest first:

1. :func:`copy_from` - the backend's native bulk path. PostgreSQL ``COPY``,
   Snowflake ``PUT`` + ``COPY INTO``, MySQL ``LOAD DATA LOCAL INFILE``. Usually
   an order of magnitude faster than anything else because it bypasses the
   per-statement parser entirely.
2. :func:`bulk_insert` - chunked ``executemany``. Works on every SQL backend,
   typically 20-100x a per-row loop.
3. A per-row loop - what you are replacing.

Reading has the mirror-image problem: ``cursor.fetchall()`` on a large table
loads the entire result into a Python list, and the process is killed by the OOM
killer at 3am. :func:`chunked_read` yields fixed-size batches, so memory stays
flat regardless of table size.

Every function here takes an optional ``on_progress`` callback, because a bulk
load with no output is indistinguishable from a hung one.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .connection import Connection, Row
from .exceptions import BulkLoadError, NotSupportedError

__all__ = [
    "BulkResult",
    "bulk_insert",
    "upsert",
    "copy_from",
    "chunked_read",
    "chunked",
    "rows_from_csv",
    "bulk_insert_dataframe",
    "chunked_read_dataframes",
    "ProgressCallback",
]

log = logging.getLogger("pydbconnect.bulk")

#: ``on_progress(rows_written_so_far, rows_in_this_chunk)``.
ProgressCallback = Callable[[int, int], None]

DEFAULT_CHUNK_SIZE = 1000


@dataclass
class BulkResult:
    """What a bulk operation did.

    Attributes:
        table: Target table.
        rows_written: Rows submitted to the database.
        chunks: Number of batches.
        seconds: Wall-clock duration.
        method: How it was done - ``executemany``, ``upsert``, ``copy`` or
            ``copy-fallback``. Worth logging: a load that silently fell back
            from ``COPY`` to ``executemany`` is a load that got ten times
            slower without telling anyone.
    """

    table: str
    rows_written: int = 0
    chunks: int = 0
    seconds: float = 0.0
    method: str = "executemany"
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def rows_per_second(self) -> float:
        """Throughput, or ``0.0`` when the run was too fast to measure."""
        return self.rows_written / self.seconds if self.seconds > 0 else 0.0

    def summary(self) -> str:
        """A one-line summary suitable for a log or a job report."""
        return (
            f"{self.rows_written:,} row(s) into {self.table} in {self.chunks} chunk(s), "
            f"{self.seconds:.2f}s, {self.rows_per_second:,.0f} rows/s [{self.method}]"
        )

    def __str__(self) -> str:
        return self.summary()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def chunked(items: Iterable[Any], size: int) -> Iterator[List[Any]]:
    """Yield lists of at most ``size`` items from ``items``.

    Works on any iterable, including generators and open cursors, and never
    holds more than ``size`` items at once.

    Raises:
        ValueError: ``size`` is below 1.
    """
    if size < 1:
        raise ValueError("chunk size must be at least 1")
    batch: List[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _normalise(
    rows: Iterable[Any],
    columns: Optional[Sequence[str]],
) -> Tuple[List[str], Iterator[Tuple[Any, ...]]]:
    """Turn dicts or sequences into ``(columns, tuples)``.

    Column order is fixed by the first row (or by ``columns``) and every
    subsequent row is projected onto it, so a dict whose keys happen to be in a
    different order cannot silently write values into the wrong columns.

    Raises:
        BulkLoadError: A row is missing a column, has the wrong length, or the
            rows are sequences and no ``columns`` were given.
    """
    iterator = iter(rows)
    try:
        first = next(iterator)
    except StopIteration:
        return list(columns or []), iter(())

    if isinstance(first, Mapping):
        resolved = list(columns) if columns else list(first.keys())
        if not resolved:
            raise BulkLoadError("cannot infer columns: the first row is an empty mapping")

        def project() -> Iterator[Tuple[Any, ...]]:
            for index, row in enumerate(_prepend(first, iterator)):
                if not isinstance(row, Mapping):
                    raise BulkLoadError(
                        f"row {index} is a {type(row).__name__}, but row 0 was a mapping; "
                        f"do not mix dict rows and tuple rows in one load",
                        rows_written=index,
                    )
                missing = [c for c in resolved if c not in row]
                if missing:
                    raise BulkLoadError(
                        f"row {index} is missing column(s): {', '.join(missing)}. "
                        f"Pass columns=[...] explicitly if rows are intentionally sparse "
                        f"and supply None for absent values",
                        rows_written=index,
                    )
                yield tuple(row[c] for c in resolved)

        return resolved, project()

    if isinstance(first, (str, bytes)):
        raise BulkLoadError(
            f"rows must be mappings or sequences of values, got {type(first).__name__}; "
            f"a bare string is almost never a row"
        )

    if not columns:
        raise BulkLoadError(
            "columns must be given when rows are sequences: there is nothing to "
            "infer names from. Pass columns=['id', 'name', ...]"
        )
    resolved = list(columns)
    width = len(resolved)

    def project_seq() -> Iterator[Tuple[Any, ...]]:
        for index, row in enumerate(_prepend(first, iterator)):
            values = tuple(row)
            if len(values) != width:
                raise BulkLoadError(
                    f"row {index} has {len(values)} value(s) but {width} column(s) "
                    f"were declared: {', '.join(resolved)}",
                    rows_written=index,
                )
            yield values

    return resolved, project_seq()


def _prepend(first: Any, rest: Iterator[Any]) -> Iterator[Any]:
    """Put ``first`` back at the head of ``rest``, without buffering the rest."""
    yield first
    yield from rest


def _report(callback: Optional[ProgressCallback], written: int, chunk_rows: int) -> None:
    """Invoke a progress callback without letting it break the load."""
    if callback is None:
        return
    try:
        callback(written, chunk_rows)
    except Exception:
        log.debug("on_progress callback raised; ignoring", exc_info=True)


def _load(
    conn: Connection,
    table: str,
    sql: str,
    columns: Sequence[str],
    tuples: Iterator[Tuple[Any, ...]],
    *,
    chunk_size: int,
    on_progress: Optional[ProgressCallback],
    method: str,
    transaction_per_chunk: bool,
) -> BulkResult:
    """Shared driver for :func:`bulk_insert` and :func:`upsert`."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    result = BulkResult(table=table, method=method)
    started = time.monotonic()
    log.debug("%s into %s (%d columns, chunk_size=%d)", method, table, len(columns), chunk_size)

    try:
        for batch in chunked(tuples, chunk_size):
            if transaction_per_chunk and conn.backend.supports_transactions and not conn.in_transaction:
                with conn.transaction():
                    conn.executemany(sql, batch)
            else:
                conn.executemany(sql, batch)
            result.rows_written += len(batch)
            result.chunks += 1
            _report(on_progress, result.rows_written, len(batch))
    except BulkLoadError as exc:
        exc.context.setdefault("table", table)
        raise
    except Exception as exc:
        raise BulkLoadError(
            f"bulk load into {table} failed after {result.rows_written:,} row(s) "
            f"in {result.chunks} chunk(s): {type(exc).__name__}: {exc}",
            rows_written=result.rows_written, table=table,
        ) from exc

    result.seconds = time.monotonic() - started
    log.info("%s", result.summary())
    return result


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def bulk_insert(
    conn: Connection,
    table: str,
    rows: Iterable[Any],
    *,
    columns: Optional[Sequence[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    on_progress: Optional[ProgressCallback] = None,
    transaction_per_chunk: bool = True,
) -> BulkResult:
    """Insert rows in batches using ``executemany``.

    Each chunk is one ``executemany`` call, which the driver turns into a
    single round trip (or a small number of them) rather than one per row.
    Each chunk is also its own transaction by default, so a failure halfway
    through leaves the chunks that already committed in place and
    :attr:`BulkResult.rows_written` on the raised
    :class:`~pydbconnect.exceptions.BulkLoadError` tells you where to resume.

    Args:
        conn: An open connection.
        table: Target table. Quoted with the backend's identifier rules.
        rows: Dicts (columns inferred from the first) or sequences (``columns``
            required). Generators are consumed lazily, so a 50GB source file
            never lands in memory.
        columns: Explicit column list and order.
        chunk_size: Rows per ``executemany``. 1000-10000 is the usual sweet
            spot; past that you are trading memory for nothing.
        on_progress: ``callback(rows_written, rows_in_chunk)``, called once per
            chunk.
        transaction_per_chunk: Wrap each chunk in its own transaction. Set
            ``False`` when you want the whole load to be atomic, and wrap the
            call in :meth:`~pydbconnect.connection.Connection.transaction`
            yourself - at the cost of a much longer-lived lock and a much
            bigger rollback.

    Returns:
        A :class:`BulkResult`.

    Raises:
        BulkLoadError: The load failed. ``rows_written`` says how far it got.

    Example::

        result = bulk_insert(conn, "fact_sales", rows, chunk_size=5000)
        print(result.summary())
    """
    resolved, tuples = _normalise(rows, columns)
    if not resolved:
        return BulkResult(table=table, method="executemany")
    sql = conn.backend.insert_sql(table, resolved)
    return _load(
        conn, table, sql, resolved, tuples,
        chunk_size=chunk_size, on_progress=on_progress,
        method="executemany", transaction_per_chunk=transaction_per_chunk,
    )


def upsert(
    conn: Connection,
    table: str,
    rows: Iterable[Any],
    *,
    key_columns: Sequence[str],
    update_columns: Optional[Sequence[str]] = None,
    columns: Optional[Sequence[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    on_progress: Optional[ProgressCallback] = None,
    transaction_per_chunk: bool = True,
) -> BulkResult:
    """Insert rows, updating any that already exist.

    The statement is generated by the backend, because every dialect spells
    this differently:

    ==========  ==================================================
    Backend     Syntax
    ==========  ==================================================
    sqlite      ``INSERT ... ON CONFLICT (k) DO UPDATE SET ...``
    postgres    ``INSERT ... ON CONFLICT (k) DO UPDATE SET ...``
    mysql       ``INSERT ... ON DUPLICATE KEY UPDATE ...``
    oracle      ``MERGE INTO ... USING dual ON ... WHEN MATCHED``
    snowflake   ``MERGE INTO ... USING (SELECT ...) ON ...``
    ==========  ==================================================

    Args:
        conn: An open connection.
        table: Target table.
        rows: Dicts or sequences, as for :func:`bulk_insert`.
        key_columns: Columns identifying an existing row. On PostgreSQL and
            SQLite these must carry a unique or primary key constraint - the
            ``ON CONFLICT`` target is matched against an index, not against
            column names.
        update_columns: Columns to overwrite on a match. Defaults to every
            column that is not a key. Pass a subset to preserve columns such as
            ``created_at``.
        columns: Explicit column list and order.
        chunk_size: Rows per ``executemany``.
        on_progress: ``callback(rows_written, rows_in_chunk)``.
        transaction_per_chunk: As for :func:`bulk_insert`.

    Returns:
        A :class:`BulkResult` with ``method="upsert"``.

    Raises:
        NotSupportedError: The backend has no upsert syntax.
        BulkLoadError: The load failed, or the key columns are not among the
            inserted columns.
    """
    if not key_columns:
        raise BulkLoadError("upsert requires key_columns")
    resolved, tuples = _normalise(rows, columns)
    if not resolved:
        return BulkResult(table=table, method="upsert")
    if not conn.backend.supports_upsert:
        raise NotSupportedError(
            f"backend {conn.backend.name!r} does not support upsert; load into a "
            f"staging table with bulk_insert and merge with your own statement"
        )
    try:
        sql = conn.backend.upsert_sql(table, resolved, key_columns, update_columns)
    except ValueError as exc:
        raise BulkLoadError(str(exc), table=table) from exc
    return _load(
        conn, table, sql, resolved, tuples,
        chunk_size=chunk_size, on_progress=on_progress,
        method="upsert", transaction_per_chunk=transaction_per_chunk,
    )


def copy_from(
    conn: Connection,
    table: str,
    rows: Iterable[Any],
    *,
    columns: Optional[Sequence[str]] = None,
    chunk_size: int = 50_000,
    fallback: bool = True,
    on_progress: Optional[ProgressCallback] = None,
    **options: Any,
) -> BulkResult:
    """Load rows using the backend's native bulk path, if it has one.

    What "native" actually means, honestly:

    * **PostgreSQL** - real ``COPY ... FROM STDIN``. Rows are streamed as CSV
      over the wire and never parsed as SQL. Commonly 5-20x ``executemany``.
    * **Snowflake** - rows are written to a local gzipped CSV, ``PUT`` to the
      table stage, then ``COPY INTO``. Fast for large loads, and slower than a
      plain insert for small ones because of the staging overhead. Use it above
      roughly 100k rows.
    * **MySQL** - ``LOAD DATA LOCAL INFILE``, which requires ``local_infile``
      enabled on both client and server. Many managed MySQL services disable it.
    * **SQLite and Oracle** - no native path here.
      :func:`bulk_insert` already batches; there is nothing faster to reach for.

    Args:
        conn: An open connection.
        table: Target table.
        rows: Dicts or sequences.
        columns: Explicit column list and order.
        chunk_size: Rows buffered per native call, and the ``executemany`` chunk
            size when falling back.
        fallback: When the backend has no native path, fall back to
            :func:`bulk_insert` and log a warning. Set ``False`` to raise
            instead, which is right in a pipeline where the slow path would blow
            the batch window.
        on_progress: ``callback(rows_written, rows_in_chunk)``.
        **options: Passed to the backend, e.g. ``null_marker`` or
            ``file_format``.

    Returns:
        A :class:`BulkResult` with ``method="copy"`` or ``"copy-fallback"``.

    Raises:
        NotSupportedError: No native path and ``fallback`` is False.
        BulkLoadError: The load failed.
    """
    resolved, tuples = _normalise(rows, columns)
    if not resolved:
        return BulkResult(table=table, method="copy")

    if not conn.backend.supports_copy:
        message = (
            f"backend {conn.backend.name!r} has no native bulk copy path"
        )
        if not fallback:
            raise NotSupportedError(
                f"{message}; pass fallback=True to use chunked executemany instead"
            )
        log.info("%s; falling back to chunked executemany", message)
        sql = conn.backend.insert_sql(table, resolved)
        result = _load(
            conn, table, sql, resolved, tuples,
            chunk_size=chunk_size, on_progress=on_progress,
            method="copy-fallback", transaction_per_chunk=True,
        )
        return result

    result = BulkResult(table=table, method="copy")
    started = time.monotonic()
    try:
        for batch in chunked(tuples, chunk_size):
            written = conn.backend.copy_from(conn.raw, table, batch, resolved, **options)
            result.rows_written += int(written if written is not None else len(batch))
            result.chunks += 1
            _report(on_progress, result.rows_written, len(batch))
        if not conn.in_transaction:
            conn.commit()
    except Exception as exc:
        conn.rollback()
        raise BulkLoadError(
            f"copy into {table} failed after {result.rows_written:,} row(s): "
            f"{type(exc).__name__}: {exc}",
            rows_written=result.rows_written, table=table,
        ) from exc

    result.seconds = time.monotonic() - started
    log.info("%s", result.summary())
    return result


def chunked_read(
    conn: Connection,
    sql: str,
    params: Any = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    on_progress: Optional[ProgressCallback] = None,
) -> Iterator[List[Row]]:
    """Read a result set as a sequence of batches.

    Memory stays proportional to ``chunk_size``, not to the size of the table.
    This is the read half of the same idea as :func:`bulk_insert`: process a
    hundred million rows on a laptop, one batch at a time.

    Args:
        conn: An open connection.
        sql: The query.
        params: Bind parameters.
        chunk_size: Rows per batch.
        on_progress: ``callback(rows_read, rows_in_chunk)``, called per batch.

    Yields:
        Lists of at most ``chunk_size`` dicts.

    Example::

        for batch in chunked_read(src, "SELECT * FROM events", chunk_size=5000):
            bulk_insert(dst, "events", batch, chunk_size=5000)
    """
    read = 0
    for batch in chunked(conn.stream(sql, params, chunk_size=chunk_size), chunk_size):
        read += len(batch)
        _report(on_progress, read, len(batch))
        yield batch


def rows_from_csv(
    path: Any,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    null_marker: Optional[str] = "",
    columns: Optional[Sequence[str]] = None,
) -> Tuple[List[str], Iterator[Dict[str, Any]]]:
    """Stream a CSV file as ``(columns, row dicts)``.

    The file is read lazily, so loading a file larger than RAM works. Values
    equal to ``null_marker`` become ``None``; everything else stays a string,
    because guessing types is how a zip code becomes an integer and loses its
    leading zero.

    Args:
        path: Path to the file.
        delimiter: Field separator.
        encoding: File encoding.
        null_marker: Value treated as SQL NULL. ``None`` disables the mapping.
        columns: Column names, when the file has no header row.

    Returns:
        ``(columns, iterator)``. The iterator holds the file open until it is
        exhausted or closed.

    Raises:
        BulkLoadError: The file is empty or has no header.
    """
    import csv
    from pathlib import Path as _Path

    file_path = _Path(path).expanduser()
    handle = file_path.open("r", encoding=encoding, newline="")
    try:
        if columns:
            reader = csv.DictReader(handle, fieldnames=list(columns), delimiter=delimiter)
            names = list(columns)
        else:
            reader = csv.DictReader(handle, delimiter=delimiter)
            names = list(reader.fieldnames or [])
            if not names:
                raise BulkLoadError(
                    f"{file_path} has no header row; pass columns=[...] if the file "
                    f"is headerless"
                )
    except BaseException:
        handle.close()
        raise

    def generate() -> Iterator[Dict[str, Any]]:
        try:
            for record in reader:
                if null_marker is None:
                    yield {k: record.get(k) for k in names}
                else:
                    yield {
                        k: (None if record.get(k) == null_marker else record.get(k))
                        for k in names
                    }
        finally:
            handle.close()

    return names, generate()


# --------------------------------------------------------------------------- #
# Optional pandas convenience
# --------------------------------------------------------------------------- #


def _import_pandas() -> Any:
    """Import pandas, with a message that names the install command.

    pandas is *not* a dependency of this library. Requiring a 60MB numerical
    stack to insert rows into a table would be absurd, so these two helpers
    import it on call.
    """
    try:
        import pandas
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise NotSupportedError(
            "this helper needs pandas, which pydb-connect does not depend on. "
            "Install it with: pip install pandas"
        ) from exc
    return pandas


def bulk_insert_dataframe(
    conn: Connection,
    table: str,
    frame: Any,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    on_progress: Optional[ProgressCallback] = None,
) -> BulkResult:
    """Insert a pandas DataFrame with :func:`bulk_insert`.

    ``NaN`` and ``NaT`` are converted to ``None`` so they arrive as SQL NULL
    rather than as the float ``nan``, which is the single most common surprise
    when loading a DataFrame into a database.

    Args:
        conn: An open connection.
        table: Target table.
        frame: A ``pandas.DataFrame``. Column names become column names.
        chunk_size: Rows per ``executemany``.
        on_progress: ``callback(rows_written, rows_in_chunk)``.
    """
    pandas = _import_pandas()
    columns = [str(c) for c in frame.columns]
    prepared = frame.astype(object).where(pandas.notnull(frame), None)
    rows = (tuple(record) for record in prepared.itertuples(index=False, name=None))
    return bulk_insert(
        conn, table, rows, columns=columns,
        chunk_size=chunk_size, on_progress=on_progress,
    )


def chunked_read_dataframes(
    conn: Connection,
    sql: str,
    params: Any = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[Any]:
    """Yield a ``pandas.DataFrame`` per batch of :func:`chunked_read`.

    The point is the same as :func:`chunked_read`: bounded memory. Each frame
    holds at most ``chunk_size`` rows.
    """
    pandas = _import_pandas()
    for batch in chunked_read(conn, sql, params, chunk_size=chunk_size):
        yield pandas.DataFrame(batch)
