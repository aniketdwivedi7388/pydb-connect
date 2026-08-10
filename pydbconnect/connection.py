"""Connections, transactions and pooling.

What this replaces
------------------

::

    conn = None
    try:
        conn = mysql.connector.connect(host=cfg["host"], user=cfg["user"],
                                       password=cfg["password"])
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE id = " + str(order_id))
        rows = cur.fetchall()
    except Exception as e:
        print(e)
    finally:
        conn.close()          # AttributeError when connect() itself failed

Four defects, all of them routine in production code:

* ``conn.close()`` raises ``AttributeError`` on ``None`` when the connect
  failed, so the real error is replaced by a bogus one.
* the cursor is never closed;
* the SQL is built by concatenation;
* ``rows`` is unbound if anything threw, and the caller finds out later.

:class:`Connection` closes on every path including the failure paths, closes
cursors in ``finally``, takes parameters separately from SQL, and warns when a
statement looks interpolated.

Parameters, not formatting
--------------------------

Every method takes ``sql`` and ``params`` as **separate arguments**::

    conn.query("SELECT * FROM orders WHERE id = ?", (order_id,))     # correct
    conn.query(f"SELECT * FROM orders WHERE id = {order_id}")        # flagged

The second form is an injection vector even when ``order_id`` "is only ever an
int", and it defeats the database's statement cache. :func:`check_sql_safety`
looks for the pattern and, depending on ``sql_guard``, logs a warning
(default), raises :class:`~pydbconnect.exceptions.UnsafeSQLError`, or says
nothing.

Transactions
------------

Statements outside :meth:`Connection.transaction` commit individually.
Statements inside it commit once, at the end, or roll back as a unit::

    with connect("warehouse") as conn:
        with conn.transaction():
            conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (100, 1))
            conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (100, 2))

Nested ``transaction()`` blocks join the outermost one: the inner block does not
commit, so an outer rollback still undoes everything. Savepoints are not
emulated, because a partial rollback that silently does nothing is worse than
no feature at all.
"""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import re
import threading
import time
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from .config import ConnectionConfig
from .exceptions import (
    ConnectionFailure,
    NotSupportedError,
    PoolClosedError,
    PoolTimeout,
    QueryError,
    TransactionError,
    UnsafeSQLError,
)
from .retry import RetryPolicy, retry_call
from .secrets import redact

__all__ = [
    "Connection",
    "ConnectionPool",
    "check_sql_safety",
    "Row",
    "Params",
]

log = logging.getLogger("pydbconnect.connection")

#: A result row. Dicts, not tuples: ``row["order_id"]`` survives a column being
#: added to the SELECT, ``row[3]`` does not.
Row = Dict[str, Any]

#: Bind parameters. Sequence for positional styles, mapping for named ones.
Params = Union[Sequence[Any], Mapping[str, Any], None]

_DDL_PREFIXES = (
    "create", "alter", "drop", "truncate", "pragma", "begin", "commit",
    "rollback", "grant", "revoke", "attach", "detach", "vacuum", "analyze", "set",
)

# A quoted string literal sitting where a bind parameter belongs.
_INTERPOLATED = re.compile(
    r"(?:=|<>|!=|<=|>=|<|>|\bLIKE\b|\bILIKE\b|\bIN\b)\s*\(?\s*'[^']*'",
    re.IGNORECASE,
)
# Leftover str.format / f-string braces, e.g. "WHERE id = {}" that never got formatted.
_FORMAT_BRACES = re.compile(r"\{\w*\}")


def check_sql_safety(
    sql: str,
    params: Params = None,
    mode: str = "warn",
    *,
    connection: str = "",
) -> Optional[str]:
    """Flag SQL that looks like it was built by string interpolation.

    The heuristic is deliberately narrow, because a guard that cries wolf gets
    switched off. A statement is flagged only when **all** of these hold:

    * no bind parameters were supplied;
    * the statement is not DDL (a ``DEFAULT 'x'`` in ``CREATE TABLE`` is fine);
    * a quoted string literal appears immediately after a comparison operator,
      or an unsubstituted ``{}`` / ``{name}`` placeholder is present.

    Args:
        sql: The statement.
        params: Bind parameters, if any.
        mode: ``warn`` logs, ``error`` raises, ``off`` disables the check.
        connection: Connection name, for the message.

    Returns:
        The reason string when the statement was flagged, else ``None``.

    Raises:
        UnsafeSQLError: ``mode`` is ``error`` and the statement was flagged.
    """
    if mode == "off" or params:
        return None
    stripped = sql.lstrip()
    if not stripped:
        return None
    first = stripped.split(None, 1)[0].lower()
    if first in _DDL_PREFIXES:
        return None

    reason = None
    if _FORMAT_BRACES.search(sql):
        reason = "contains an unsubstituted {} placeholder"
    elif _INTERPOLATED.search(sql):
        reason = "embeds a quoted literal where a bind parameter belongs"
    if reason is None:
        return None

    message = (
        f"SQL {reason}; pass values as parameters instead "
        f"(conn.execute('... WHERE col = ?', (value,))). "
        f"String-built SQL is an injection risk and defeats the statement cache"
    )
    if mode == "error":
        raise UnsafeSQLError(message, connection=connection, sql=_snippet(sql))
    log.warning("%s | %s", message, _snippet(sql))
    return reason


def _snippet(sql: str, limit: int = 160) -> str:
    """Return a single-line, redacted, truncated version of ``sql`` for logs."""
    flat = " ".join(str(sql).split())
    if len(flat) > limit:
        flat = flat[: limit - 3] + "..."
    return redact(flat)


class Connection:
    """A database connection that closes itself and speaks in dicts.

    Obtain one from :func:`pydbconnect.connect` or
    :meth:`ConnectionPool.acquire` rather than constructing it directly.

    Attributes:
        config: The :class:`~pydbconnect.config.ConnectionConfig` it was opened
            from.
        backend: The :class:`~pydbconnect.backends.base.Backend` in use.
        raw: The underlying driver connection. Reach for it when you need
            something backend-specific; everything here is a convenience over
            it, not a wall around it.
    """

    def __init__(
        self,
        config: ConnectionConfig,
        backend: Any,
        raw: Any,
        *,
        pool: Optional["ConnectionPool"] = None,
        entry: Optional["_PoolEntry"] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self.config = config
        self.backend = backend
        self.raw = raw
        self.retry_policy = retry_policy or RetryPolicy.from_settings(
            config.retry, classifier=backend.classify_error
        )
        self._pool = pool
        self._entry = entry
        self._closed = False
        self._broken = False
        self._tx_depth = 0
        self._sql_guard = config.sql_guard
        # Built on first retry so the reconnect hook binds to this connection
        # and not to the pool-wide policy every connection shares.
        self._retry_with_reconnect: Optional[RetryPolicy] = None

    # -- construction ------------------------------------------------------- #

    @classmethod
    def open(
        cls,
        config: ConnectionConfig,
        *,
        backend: Any = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> "Connection":
        """Open a new connection, retrying transient connect failures.

        Raises:
            ConnectionFailure: The connection could not be established. The
                driver's exception is on ``__cause__``.
        """
        from .registry import get_backend

        backend = backend or get_backend(config.backend)
        policy = retry_policy or RetryPolicy.from_settings(
            config.retry, classifier=backend.classify_error
        )
        raw = _open_raw(backend, config, policy)
        conn = cls(config, backend, raw, retry_policy=policy)
        log.debug("opened %r", conn)
        return conn

    # -- state -------------------------------------------------------------- #

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""
        return self._closed

    @property
    def in_transaction(self) -> bool:
        """Whether an explicit :meth:`transaction` block is active."""
        return self._tx_depth > 0

    @property
    def client(self) -> Any:
        """Alias for :attr:`raw`, natural for object-store backends."""
        return self.raw

    def _require_open(self) -> None:
        if self._closed:
            raise ConnectionFailure(
                "connection is closed; open a new one rather than reusing this object",
                connection=self.config.name,
            )

    # -- statement execution ------------------------------------------------ #

    def execute(self, sql: str, params: Params = None) -> int:
        """Run a statement and return the number of affected rows.

        Outside a :meth:`transaction` block the statement is committed
        immediately. Inside one, it is not - the surrounding block decides.

        Args:
            sql: A statement with placeholders, never with values in it.
            params: Bind parameters.

        Returns:
            ``cursor.rowcount``, or ``0`` when the driver reports ``-1``
            (several drivers do so for statements that affect nothing).

        Raises:
            QueryError: Execution failed. The driver exception is on
                ``__cause__``.
            UnsafeSQLError: ``sql_guard`` is ``error`` and the statement looks
                interpolated.
        """
        self._require_open()
        check_sql_safety(sql, params, self._sql_guard, connection=self.config.name)

        def _run() -> int:
            with self._cursor() as cur:
                self._execute(cur, sql, params)
                count = getattr(cur, "rowcount", -1)
            self._maybe_commit()
            return max(0, int(count if count is not None else -1))

        return self._attempt(_run, f"execute on {self.config.name}")

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> int:
        """Run one statement against many parameter sets in a single round trip.

        This is the difference between a bulk load that takes four seconds and
        one that takes four minutes. ``executemany`` hands the whole batch to
        the driver, which sends it as one (or a few) network round trips;
        calling :meth:`execute` in a Python loop pays the full latency of the
        link once per row.

        Args:
            sql: A single parameterised statement.
            seq_of_params: Parameter tuples. An iterator is materialised into a
                list, because drivers need to know the batch size.

        Returns:
            ``cursor.rowcount`` when the driver reports one, otherwise the
            number of parameter sets submitted.
        """
        self._require_open()
        rows = list(seq_of_params)
        if not rows:
            return 0

        def _run() -> int:
            with self._cursor() as cur:
                try:
                    cur.executemany(sql, rows)
                except Exception as exc:
                    raise QueryError(
                        f"executemany failed after {len(rows)} parameter set(s): "
                        f"{type(exc).__name__}: {exc}",
                        connection=self.config.name, sql=_snippet(sql),
                    ) from exc
                count = getattr(cur, "rowcount", -1)
            self._maybe_commit()
            return int(count) if count is not None and count >= 0 else len(rows)

        return self._attempt(_run, f"executemany on {self.config.name}")

    def query(self, sql: str, params: Params = None) -> List[Row]:
        """Run a query and return every row as a dict.

        Use this when the result comfortably fits in memory. When it might not,
        use :meth:`stream` or
        :func:`pydbconnect.bulk.chunked_read` - "it fit in dev" is not a
        capacity plan.
        """
        self._require_open()
        check_sql_safety(sql, params, self._sql_guard, connection=self.config.name)

        def _run() -> List[Row]:
            with self._cursor() as cur:
                self._execute(cur, sql, params)
                columns = _columns(cur)
                if not columns:
                    return []
                return [dict(zip(columns, row)) for row in cur.fetchall()]

        return self._attempt(_run, f"query on {self.config.name}")

    def query_one(self, sql: str, params: Params = None) -> Optional[Row]:
        """Return the first row as a dict, or ``None`` when there are none.

        Remaining rows are discarded, so add your own ``LIMIT``/``FETCH FIRST``
        if the query could match a lot of them.
        """
        self._require_open()
        check_sql_safety(sql, params, self._sql_guard, connection=self.config.name)

        def _run() -> Optional[Row]:
            with self._cursor() as cur:
                self._execute(cur, sql, params)
                columns = _columns(cur)
                if not columns:
                    return None
                row = cur.fetchone()
                return dict(zip(columns, row)) if row is not None else None

        return self._attempt(_run, f"query_one on {self.config.name}")

    def scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column of the first row, or ``None``.

        Handy for ``SELECT count(*)`` and ``SELECT max(loaded_at)``.
        """
        row = self.query_one(sql, params)
        if not row:
            return None
        return next(iter(row.values()))

    def stream(
        self,
        sql: str,
        params: Params = None,
        chunk_size: int = 1000,
    ) -> Iterator[Row]:
        """Iterate a result set without loading it all into memory.

        Rows are fetched ``chunk_size`` at a time with ``fetchmany`` and yielded
        one at a time. Where the backend supports a server-side cursor
        (PostgreSQL named cursors, for example) one is used, so the *server*
        does not materialise the whole result either.

        Args:
            sql: The query.
            params: Bind parameters.
            chunk_size: Rows per fetch. Larger means fewer round trips and more
                memory; 1000-10000 suits most row widths.

        Yields:
            One dict per row.

        Note:
            The statement is not retried once iteration has started - a partly
            consumed result set cannot be resumed by re-running the query, and
            silently restarting it would hand the caller duplicate rows.
        """
        self._require_open()
        check_sql_safety(sql, params, self._sql_guard, connection=self.config.name)
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")

        cur = self.backend.cursor(
            self.raw, server_side=True, name=f"pydb_{id(self):x}"
        )
        try:
            self._execute(cur, sql, params)
            columns = _columns(cur)
            if not columns:
                return
            while True:
                batch = cur.fetchmany(chunk_size)
                if not batch:
                    break
                for row in batch:
                    yield dict(zip(columns, row))
        finally:
            _close_quietly(cur)

    # -- transactions ------------------------------------------------------- #

    @contextmanager
    def transaction(self) -> Iterator["Connection"]:
        """Group statements into one commit-or-rollback unit.

        Commits when the block exits normally, rolls back on any exception
        including :class:`KeyboardInterrupt`. Nested blocks join the outermost
        transaction; only the outermost one commits.

        Raises:
            NotSupportedError: The backend has no transactions.
            TransactionError: The commit failed. A failed rollback is logged and
                the original exception is re-raised unchanged, because the
                original is the one that explains what happened.
        """
        self._require_open()
        if not self.backend.supports_transactions:
            raise NotSupportedError(
                f"backend {self.backend.name!r} does not support transactions",
                connection=self.config.name,
            )
        if self._tx_depth:
            self._tx_depth += 1
            try:
                yield self
            finally:
                self._tx_depth -= 1
            return

        self._tx_depth = 1
        try:
            yield self
        except BaseException:
            self._tx_depth = 0
            self.rollback()
            raise
        else:
            self._tx_depth = 0
            self.commit()

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            TransactionError: The driver refused the commit.
        """
        self._require_open()
        try:
            self.raw.commit()
        except Exception as exc:
            self._broken = True
            raise TransactionError(
                f"commit failed: {type(exc).__name__}: {exc}",
                connection=self.config.name,
            ) from exc

    def rollback(self) -> None:
        """Roll back the current transaction.

        Never raises: it is nearly always called while another exception is in
        flight, and masking that exception with a rollback failure loses the
        information that matters. A failed rollback marks the connection broken
        so the pool discards it instead of handing it to someone else.
        """
        if self._closed:
            return
        try:
            self.raw.rollback()
        except Exception as exc:  # noqa: BLE001 - must not mask the in-flight exception
            self._broken = True
            log.warning(
                "rollback on %s failed (%s: %s); the connection will be discarded",
                self.config.name, type(exc).__name__, redact(str(exc)),
            )

    # -- lifecycle ---------------------------------------------------------- #

    def ping(self) -> bool:
        """Return whether the connection is still alive. Never raises."""
        if self._closed:
            return False
        try:
            return bool(self.backend.ping(self.raw))
        except Exception:  # noqa: BLE001 - ping must never raise
            return False

    def close(self) -> None:
        """Close, or return to the pool. Idempotent, and never raises.

        Uncommitted work is rolled back first. Relying on the driver's
        behaviour here is a mistake - some commit on close, some roll back, and
        the difference will find you in production.
        """
        if self._closed:
            return
        if self._tx_depth:
            log.warning(
                "connection %s closed with an open transaction; rolling back",
                self.config.name,
            )
            self._tx_depth = 0
            self.rollback()

        pool, self._pool = self._pool, None
        if pool is not None:
            self._closed = True
            pool._release(self)
            return

        self._closed = True
        try:
            self.backend.close(self.raw)
        except Exception as exc:  # noqa: BLE001 - closing must not raise
            log.debug(
                "error closing %s: %s: %s",
                self.config.name, type(exc).__name__, redact(str(exc)),
            )

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        self.close()
        return False        # never suppress: closing is not handling

    def __repr__(self) -> str:
        return redact(
            f"<Connection name={self.config.name!r} backend={self.config.backend!r} "
            f"closed={self._closed} in_transaction={self.in_transaction}>"
        )

    # -- internals ---------------------------------------------------------- #

    @contextmanager
    def _cursor(self, *, server_side: bool = False) -> Iterator[Any]:
        """Yield a cursor and close it on every path, including exceptions."""
        cur = self.backend.cursor(self.raw, server_side=server_side)
        try:
            yield cur
        finally:
            _close_quietly(cur)

    def _execute(self, cur: Any, sql: str, params: Params) -> None:
        """Execute one statement on ``cur``, wrapping driver errors."""
        try:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
        except Exception as exc:
            raise QueryError(
                f"{type(exc).__name__}: {exc}",
                connection=self.config.name, sql=_snippet(sql),
            ) from exc

    def _maybe_commit(self) -> None:
        """Commit unless an explicit transaction block owns the decision."""
        if self._tx_depth:
            return
        try:
            self.raw.commit()
        except Exception as exc:
            self._broken = True
            raise TransactionError(
                f"autocommit failed: {type(exc).__name__}: {exc}",
                connection=self.config.name,
            ) from exc

    def _attempt(self, func: Any, description: str) -> Any:
        """Run ``func`` with retry, unless a transaction is open.

        Retrying a statement inside a transaction is wrong: the server has
        already rolled the transaction back, so attempt two runs against
        nothing and the earlier statements in the block are gone. Inside a
        transaction the error propagates immediately and the block rolls back,
        which is what the caller can actually handle.
        """
        if self._tx_depth or self.retry_policy.max_attempts <= 1:
            return func()
        policy = self.retry_policy
        if policy.on_retry is None:
            if self._retry_with_reconnect is None:
                self._retry_with_reconnect = _with_reconnect(policy, self)
            policy = self._retry_with_reconnect
        return retry_call(func, policy=policy, description=description)

    def _reconnect(self) -> None:
        """Replace the raw connection after a transient failure."""
        log.info("reconnecting %s", self.config.name)
        with contextlib.suppress(Exception):
            self.backend.close(self.raw)   # it is probably already dead
        self.raw = _open_raw(self.backend, self.config, RetryPolicy.none())
        self._broken = False
        if self._entry is not None:
            self._entry.raw = self.raw
            self._entry.created_at = time.monotonic()


def _with_reconnect(policy: RetryPolicy, conn: "Connection") -> RetryPolicy:
    """Return a copy of ``policy`` that reconnects between attempts.

    A retryable statement failure very often means the connection itself died.
    Retrying the statement on the same dead socket just fails again, so the
    hook checks liveness and reopens before the next attempt.
    """

    def _hook(exc: BaseException, attempt: int, delay: float) -> None:  # noqa: ARG001
        # Signature is fixed by RetryPolicy.on_retry; only the side effect matters.
        if not conn.ping():
            conn._reconnect()

    return dataclasses.replace(policy, on_retry=_hook)


def _open_raw(backend: Any, config: ConnectionConfig, policy: RetryPolicy) -> Any:
    """Open a raw driver connection, run ``on_connect``, wrap failures."""

    def _do() -> Any:
        raw = backend.connect(config)
        try:
            backend.on_connect(raw, config)
        except Exception:
            _close_quietly(raw)
            raise
        return raw

    try:
        return retry_call(_do, policy=policy, description=f"connect to {config.name}")
    except ConnectionFailure:
        raise
    except Exception as exc:
        raise ConnectionFailure(
            f"could not connect to {config.name!r} using backend "
            f"{config.backend!r}: {type(exc).__name__}: {exc}",
            connection=config.name,
        ) from exc


def _columns(cur: Any) -> List[str]:
    """Return column names from ``cursor.description``, or ``[]`` for non-selects."""
    description = getattr(cur, "description", None)
    if not description:
        return []
    return [str(d[0]) for d in description]


def _close_quietly(obj: Any) -> None:
    """Close ``obj``, swallowing and logging any error."""
    if obj is None:
        return
    try:
        obj.close()
    except Exception as exc:  # noqa: BLE001 - closing must not mask the real error
        log.debug("error closing %r: %s", type(obj).__name__, redact(str(exc)))


class _PoolEntry:
    """One pooled raw connection plus the bookkeeping the pool needs."""

    __slots__ = ("raw", "created_at", "uses")

    def __init__(self, raw: Any) -> None:
        self.raw = raw
        self.created_at = time.monotonic()
        self.uses = 0


class ConnectionPool:
    """A bounded, thread-safe connection pool.

    Opening a connection costs a TCP handshake, a TLS handshake and an
    authentication round trip - typically 20-200ms, which dwarfs most queries.
    Pooling amortises that. The bound matters just as much: an unbounded pool
    turns a slow query into ``FATAL: sorry, too many clients already`` for
    every other service sharing the database.

    Args:
        config: The connection configuration; ``config.pool`` supplies the
            defaults for every other argument.
        backend: Backend instance; looked up from the registry when omitted.
        size: Maximum open connections. Overrides ``config.pool.max_size``.
        timeout: Seconds to wait for a free connection.
        pre_ping: Check liveness on checkout.
        recycle: Close connections older than this many seconds. ``0`` disables.

    Example::

        pool = ConnectionPool(config, size=4)
        try:
            with pool.acquire() as conn:
                conn.query("SELECT 1")
        finally:
            pool.close()
    """

    def __init__(
        self,
        config: ConnectionConfig,
        *,
        backend: Any = None,
        size: Optional[int] = None,
        timeout: Optional[float] = None,
        pre_ping: Optional[bool] = None,
        recycle: Optional[float] = None,
    ) -> None:
        from .registry import get_backend

        self.config = config
        self.backend = backend or get_backend(config.backend)
        self.size = int(size if size is not None else config.pool.max_size)
        self.timeout = float(timeout if timeout is not None else config.pool.timeout)
        self.pre_ping = bool(pre_ping if pre_ping is not None else config.pool.pre_ping)
        self.recycle = float(recycle if recycle is not None else config.pool.recycle)
        if self.size < 1:
            raise ValueError("pool size must be at least 1")

        self._policy = RetryPolicy.from_settings(
            config.retry, classifier=self.backend.classify_error
        )
        self._cond = threading.Condition(threading.RLock())
        self._idle: List[_PoolEntry] = []
        self._total = 0
        self._checked_out = 0
        self._opened = 0
        self._discarded = 0
        self._closed = False

        for _ in range(min(config.pool.min_size, self.size)):
            self._idle.append(self._open_entry())

    # -- checkout ----------------------------------------------------------- #

    def borrow(self, timeout: Optional[float] = None) -> Connection:
        """Check out a connection. Prefer :meth:`acquire`, which returns it.

        Raises:
            PoolTimeout: No connection became available in time. The message
                includes the pool size, because the fix is nearly always
                "raise max_size" or "run less concurrency", and knowing the
                current value saves a round trip through the config file.
            PoolClosedError: The pool is closed.
        """
        wait = self.timeout if timeout is None else float(timeout)
        deadline = time.monotonic() + wait

        while True:
            entry: Optional[_PoolEntry]
            create = False
            with self._cond:
                if self._closed:
                    raise PoolClosedError(
                        "connection pool is closed", connection=self.config.name
                    )
                if self._idle:
                    entry = self._idle.pop()
                elif self._total < self.size:
                    self._total += 1
                    entry, create = None, True
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            f"no connection available from pool {self.config.name!r} "
                            f"within {wait:g}s; all {self.size} connection(s) are in "
                            f"use. Raise pool.max_size or reduce concurrency",
                            connection=self.config.name,
                        )
                    self._cond.wait(remaining)
                    continue

            if create:
                try:
                    entry = self._open_entry()
                except BaseException:
                    with self._cond:
                        self._total -= 1
                        self._cond.notify()
                    raise
            elif not self._usable(entry):
                self._dispose(entry)
                with self._cond:
                    self._total -= 1
                    self._cond.notify()
                continue

            assert entry is not None
            entry.uses += 1
            with self._cond:
                self._checked_out += 1
            return Connection(
                self.config, self.backend, entry.raw,
                pool=self, entry=entry, retry_policy=self._policy,
            )

    @contextmanager
    def acquire(self, timeout: Optional[float] = None) -> Iterator[Connection]:
        """Check out a connection and return it to the pool afterwards.

        The connection goes back on every path, including exceptions. A
        connection whose transaction failed to roll back is discarded rather
        than reused, because a connection in an unknown transaction state is a
        trap for whoever borrows it next.
        """
        conn = self.borrow(timeout)
        try:
            yield conn
        except BaseException:
            conn._broken = conn._broken or not conn.ping()
            raise
        finally:
            conn.close()

    # Alias: reads better at call sites that are not about pooling.
    connection = acquire

    # -- internals ---------------------------------------------------------- #

    def _open_entry(self) -> _PoolEntry:
        raw = _open_raw(self.backend, self.config, self._policy)
        self._opened += 1
        return _PoolEntry(raw)

    def _usable(self, entry: Optional[_PoolEntry]) -> bool:
        """Whether a pooled entry may be handed out: young enough, and alive."""
        if entry is None:
            return False
        if self.recycle and (time.monotonic() - entry.created_at) > self.recycle:
            log.debug("recycling connection to %s after %.0fs", self.config.name, self.recycle)
            return False
        if not self.pre_ping:
            return True
        try:
            return bool(self.backend.ping(entry.raw))
        except Exception:  # noqa: BLE001 - an unusable connection is simply discarded
            return False

    def _dispose(self, entry: Optional[_PoolEntry]) -> None:
        if entry is None:
            return
        self._discarded += 1
        try:
            self.backend.close(entry.raw)
        except Exception as exc:  # noqa: BLE001 - closing must not raise
            log.debug("error closing pooled connection: %s", redact(str(exc)))

    def _release(self, conn: Connection) -> None:
        """Return ``conn``'s entry to the idle list, or throw it away."""
        entry = conn._entry
        conn._entry = None
        drop = entry is None or conn._broken
        with self._cond:
            self._checked_out = max(0, self._checked_out - 1)
            if self._closed:
                drop = True
            if drop:
                self._total = max(0, self._total - 1)
            else:
                assert entry is not None
                self._idle.append(entry)
            self._cond.notify()
        if drop:
            self._dispose(entry)

    # -- lifecycle ---------------------------------------------------------- #

    def close(self) -> None:
        """Close every idle connection and refuse further checkouts.

        Connections currently checked out are closed when they are returned.
        """
        with self._cond:
            self._closed = True
            idle, self._idle = self._idle, []
            self._total = max(0, self._total - len(idle))
            self._cond.notify_all()
        for entry in idle:
            self._dispose(entry)

    def stats(self) -> Dict[str, int]:
        """Return a snapshot of pool counters, suitable for a metrics gauge."""
        with self._cond:
            return {
                "size": self.size,
                "total": self._total,
                "idle": len(self._idle),
                "in_use": self._checked_out,
                "opened": self._opened,
                "discarded": self._discarded,
            }

    def __enter__(self) -> "ConnectionPool":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        self.close()
        return False        # never suppress: closing is not handling

    def __repr__(self) -> str:
        stats = self.stats()
        return (
            f"<ConnectionPool name={self.config.name!r} backend={self.config.backend!r} "
            f"size={stats['size']} idle={stats['idle']} in_use={stats['in_use']}>"
        )
