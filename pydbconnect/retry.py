"""Retry with exponential backoff, full jitter, and an error classifier.

The failure mode this module exists to prevent::

    for attempt in range(10):          # retries a syntax error ten times
        try:
            cursor.execute(sql)
            break
        except Exception:              # catches KeyboardInterrupt's cousins too
            time.sleep(5)              # every worker wakes at the same instant

Three things are wrong there and all three are fixed here:

* **Blanket ``except``.** A syntax error, a missing column and a unique-key
  violation are not transient. Retrying them wastes time and hides the bug.
  :func:`classify` decides, and backends contribute their own error codes
  through :meth:`~pydbconnect.backends.base.Backend.classify_error`.
* **Fixed sleep.** When a database wobbles, every client retries in lockstep and
  the retry storm finishes what the wobble started. Full jitter -
  ``sleep = uniform(0, min(cap, base * multiplier ** attempt))`` - spreads the
  load out. It is the variant AWS measured as best for contention, and it is
  the default here.
* **Untestable.** ``time.sleep`` in a retry loop means the test suite takes
  minutes. Both the clock and the sleep function are injectable, so the tests in
  ``tests/test_retry.py`` run in microseconds and still assert real wait times.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple, TypeVar

from .exceptions import (
    ConfigurationError,
    DriverNotInstalledError,
    NotSupportedError,
    PyDBError,
    SecretError,
    UnsafeSQLError,
)

__all__ = [
    "RetryPolicy",
    "retry",
    "retry_call",
    "classify",
    "compute_backoff",
    "NEVER_RETRY",
    "TRANSIENT_MARKERS",
    "PERMANENT_MARKERS",
    "TRANSIENT_SQLSTATES",
]

log = logging.getLogger("pydbconnect.retry")

T = TypeVar("T")

#: Exception types that are always a bug or always a misconfiguration. No
#: classifier may override these - retrying them cannot possibly help.
NEVER_RETRY: Tuple[type, ...] = (
    SyntaxError,
    TypeError,
    NameError,
    AttributeError,
    IndexError,
    KeyError,
    ValueError,
    NotImplementedError,
    ConfigurationError,
    SecretError,
    UnsafeSQLError,
    NotSupportedError,
    DriverNotInstalledError,
    KeyboardInterrupt,
    SystemExit,
    MemoryError,
)

#: Substrings that mark an error as transient. Matched case-insensitively
#: against ``str(exc)`` and the exception class name.
TRANSIENT_MARKERS: Tuple[str, ...] = (
    "connection reset",
    "connection refused",
    "connection aborted",
    "connection closed",
    "connection is closed",
    "connection already closed",
    "connection timed out",
    "server closed the connection",
    "lost connection",
    "gone away",
    "broken pipe",
    "eof detected",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "try again",
    "deadlock",
    "lock wait",
    "database is locked",
    "too many connections",
    "connection pool is full",
    "cannot allocate memory for",
    "could not connect",
    "no route to host",
    "name or service not known",
    "network is unreachable",
    "service unavailable",
    "503",
    "429",
    "throttl",
    "shutdown in progress",
    "is starting up",
    "in recovery",
    "terminating connection",
)

#: Substrings that mark an error as permanent. Checked *before*
#: :data:`TRANSIENT_MARKERS`, because "syntax error at or near TIMEOUT" must not
#: be classified as transient by the word "timeout".
PERMANENT_MARKERS: Tuple[str, ...] = (
    "syntax error",
    "does not exist",
    "no such table",
    "no such column",
    "unknown column",
    "unknown database",
    "invalid identifier",
    "invalid column",
    "table or view does not exist",
    "duplicate key",
    "duplicate entry",
    "unique constraint",
    "integrity constraint",
    "not-null constraint",
    "foreign key constraint",
    "check constraint",
    "permission denied",
    "access denied",
    "insufficient privilege",
    "authentication failed",
    "password authentication failed",
    "invalid username",
    "incorrect syntax",
    "data type",
    "cannot be cast",
    "division by zero",
)

#: SQLSTATE classes and codes that are transient. ``08`` is connection
#: exception, ``40`` is transaction rollback (serialisation failure, deadlock),
#: ``57P0x`` is PostgreSQL admin shutdown, ``53`` is insufficient resources.
TRANSIENT_SQLSTATES: Tuple[str, ...] = (
    "08000", "08001", "08003", "08004", "08006", "08007", "08S01",
    "40001", "40003", "40P01",
    "53000", "53100", "53200", "53300",
    "57P01", "57P02", "57P03", "55P03",
    "HY000",
)


def _message_of(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}".lower()


def classify(
    exc: BaseException,
    *,
    backend_classifier: Optional[Callable[[BaseException], Optional[bool]]] = None,
    retry_on: Sequence[type] = (),
    never_retry: Sequence[type] = (),
) -> bool:
    """Decide whether ``exc`` is worth retrying.

    Order of decisions, first match wins:

    1. :data:`NEVER_RETRY` types (plus anything in ``never_retry``) - ``False``.
       Nothing overrides this.
    2. Types listed in ``retry_on`` - ``True``. This is the caller's explicit
       override for a driver exception the generic rules do not know.
    3. ``backend_classifier`` - the backend's own opinion, based on real error
       codes rather than string matching. ``None`` means "no opinion".
    4. A ``sqlstate``/``pgcode`` attribute matching :data:`TRANSIENT_SQLSTATES`.
    5. :data:`PERMANENT_MARKERS` in the message - ``False``.
    6. Built-in :class:`ConnectionError`, :class:`TimeoutError` and most
       :class:`OSError` subclasses - ``True``.
    7. :data:`TRANSIENT_MARKERS` in the message - ``True``.
    8. Anything else - ``False``. Unknown errors are treated as permanent,
       because retrying something you do not understand against a database you
       share with other people is how a small incident becomes a large one.

    Args:
        exc: The exception to classify.
        backend_classifier: Usually
            :meth:`~pydbconnect.backends.base.Backend.classify_error`.
        retry_on: Extra exception types that are always retryable.
        never_retry: Extra exception types that are never retryable.

    Returns:
        ``True`` if the operation should be retried.
    """
    if isinstance(exc, tuple(never_retry) + NEVER_RETRY):
        return False
    if retry_on and isinstance(exc, tuple(retry_on)):
        return True

    if backend_classifier is not None:
        verdict = backend_classifier(exc)
        if verdict is not None:
            return bool(verdict)

    state = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if state:
        state = str(state).upper()
        if state in TRANSIENT_SQLSTATES or state[:2] in ("08", "40", "53"):
            return True

    message = _message_of(exc)
    for marker in PERMANENT_MARKERS:
        if marker in message:
            return False

    if isinstance(exc, (ConnectionError, TimeoutError, BrokenPipeError)):
        return True
    if isinstance(exc, OSError) and not isinstance(exc, (FileNotFoundError, PermissionError, IsADirectoryError)):
        return True

    return any(marker in message for marker in TRANSIENT_MARKERS)


@dataclass
class RetryPolicy:
    """How hard to try, and how to wait between attempts.

    Attributes:
        max_attempts: Total attempts, including the first. ``1`` disables retry.
        initial_backoff: Base wait in seconds before the second attempt.
        max_backoff: Ceiling on any single wait.
        multiplier: Growth factor applied per attempt.
        max_elapsed: Abandon retrying once this many seconds have passed since
            the first attempt, even if attempts remain. ``0`` disables the
            wall-clock limit. This is the knob that stops a "retry 10 times with
            60s backoff" policy from blocking a job for ten minutes.
        jitter: ``full`` or ``none``. See the module docstring for why ``full``
            is the default.
        retry_on: Extra exception types that are always retryable.
        never_retry: Extra exception types that are never retryable.
        classifier: A backend classifier, consulted before the generic rules.
        on_retry: Called as ``on_retry(exc, attempt, delay)`` before each sleep.
            Useful for metrics; exceptions from it are swallowed.
    """

    max_attempts: int = 3
    initial_backoff: float = 0.2
    max_backoff: float = 10.0
    multiplier: float = 2.0
    max_elapsed: float = 60.0
    jitter: str = "full"
    retry_on: Tuple[type, ...] = ()
    never_retry: Tuple[type, ...] = ()
    classifier: Optional[Callable[[BaseException], Optional[bool]]] = None
    on_retry: Optional[Callable[[BaseException, int, float], None]] = None

    @classmethod
    def from_settings(cls, settings: Any, **overrides: Any) -> "RetryPolicy":
        """Build a policy from a :class:`~pydbconnect.config.RetrySettings`."""
        return cls(
            max_attempts=int(settings.max_attempts),
            initial_backoff=float(settings.initial_backoff),
            max_backoff=float(settings.max_backoff),
            multiplier=float(settings.multiplier),
            max_elapsed=float(settings.max_elapsed),
            jitter=str(settings.jitter),
            **overrides,
        )

    @classmethod
    def none(cls) -> "RetryPolicy":
        """A policy that never retries. Useful in tests and in tight loops."""
        return cls(max_attempts=1, max_elapsed=0.0)

    def should_retry(self, exc: BaseException) -> bool:
        """Apply :func:`classify` using this policy's hooks."""
        return classify(
            exc,
            backend_classifier=self.classifier,
            retry_on=self.retry_on,
            never_retry=self.never_retry,
        )


def compute_backoff(
    attempt: int,
    policy: RetryPolicy,
    rand: Callable[[], float] = random.random,
) -> float:
    """Return the delay in seconds before ``attempt`` is retried.

    Args:
        attempt: 1-based number of the attempt that just failed.
        policy: The policy in force.
        rand: Source of randomness returning a float in ``[0, 1)``. Injected so
            tests can pin the jitter.

    Returns:
        With ``jitter="none"``, the raw capped exponential
        ``min(max_backoff, initial_backoff * multiplier ** (attempt - 1))``.
        With ``jitter="full"``, a uniform draw from ``[0, that value]``.
    """
    ceiling = min(
        policy.max_backoff,
        policy.initial_backoff * (policy.multiplier ** max(0, attempt - 1)),
    )
    ceiling = max(0.0, ceiling)
    if policy.jitter == "none":
        return ceiling
    return ceiling * rand()


def retry_call(
    func: Callable[..., T],
    *args: Any,
    policy: Optional[RetryPolicy] = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    rand: Callable[[], float] = random.random,
    description: str = "",
    **kwargs: Any,
) -> T:
    """Call ``func(*args, **kwargs)``, retrying transient failures.

    Args:
        func: The callable to run.
        policy: Retry policy; a default :class:`RetryPolicy` is used when
            omitted.
        sleep: Sleep function. Inject a recorder to test without waiting.
        monotonic: Clock used for ``max_elapsed``. Inject a fake to test the
            wall-clock limit deterministically.
        rand: Jitter source.
        description: Label used in log messages, e.g. ``"query on warehouse"``.

    Returns:
        Whatever ``func`` returns.

    Raises:
        The last exception raised by ``func``. The exception is re-raised with
        its original type - so ``except psycopg.OperationalError`` still works -
        with ``pydb_attempts`` and ``pydb_elapsed`` attributes attached for
        diagnostics.

    A non-retryable error is raised immediately, on the first attempt, without
    sleeping. That is the entire point.
    """
    policy = policy or RetryPolicy()
    label = description or getattr(func, "__name__", repr(func))
    started = monotonic()
    attempt = 0
    last_exc: Optional[BaseException] = None

    while True:
        attempt += 1
        try:
            return func(*args, **kwargs)
        except BaseException as exc:
            last_exc = exc
            elapsed = monotonic() - started
            setattr(exc, "pydb_attempts", attempt)
            setattr(exc, "pydb_elapsed", elapsed)

            if not policy.should_retry(exc):
                log.debug(
                    "%s failed with a non-retryable error on attempt %d: %s",
                    label, attempt, type(exc).__name__,
                )
                raise
            if attempt >= policy.max_attempts:
                log.warning(
                    "%s giving up after %d attempt(s) in %.2fs: %s",
                    label, attempt, elapsed, type(exc).__name__,
                )
                raise

            delay = compute_backoff(attempt, policy, rand)
            if policy.max_elapsed and (elapsed + delay) > policy.max_elapsed:
                log.warning(
                    "%s giving up after %d attempt(s): next retry would exceed "
                    "max_elapsed of %.1fs",
                    label, attempt, policy.max_elapsed,
                )
                raise

            if policy.on_retry is not None:
                try:
                    policy.on_retry(exc, attempt, delay)
                except Exception:
                    log.debug("on_retry hook raised; ignoring", exc_info=True)

            log.info(
                "%s failed on attempt %d/%d (%s); retrying in %.2fs",
                label, attempt, policy.max_attempts, type(exc).__name__, delay,
            )
            sleep(delay)

    raise last_exc  # pragma: no cover - unreachable, keeps type checkers happy


def retry(
    policy: Optional[RetryPolicy] = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    rand: Callable[[], float] = random.random,
    **policy_kwargs: Any,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator form of :func:`retry_call`.

    ::

        @retry(max_attempts=5, initial_backoff=0.5)
        def refresh_dimension() -> None:
            ...

    Args:
        policy: A ready-made policy. When omitted, one is built from
            ``**policy_kwargs``.
        sleep: Sleep function, injectable for tests.
        monotonic: Clock, injectable for tests.
        rand: Jitter source, injectable for tests.
        **policy_kwargs: Passed to :class:`RetryPolicy` when ``policy`` is None.
    """
    resolved = policy or RetryPolicy(**policy_kwargs)

    def decorate(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_call(
                func, *args,
                policy=resolved, sleep=sleep, monotonic=monotonic, rand=rand,
                description=getattr(func, "__qualname__", func.__name__),
                **kwargs,
            )

        setattr(wrapper, "retry_policy", resolved)
        return wrapper

    return decorate


@dataclass
class RetryRecorder:
    """A drop-in ``sleep`` replacement that records instead of waiting.

    ::

        recorder = RetryRecorder()
        retry_call(flaky, policy=policy, sleep=recorder, rand=lambda: 1.0)
        assert recorder.delays == [0.2, 0.4]

    Also usable as ``monotonic``: :meth:`clock` advances by whatever was
    "slept", so ``max_elapsed`` can be exercised without real time passing.
    """

    delays: List[float] = field(default_factory=list)
    now: float = 0.0

    def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        self.now += delay

    def clock(self) -> float:
        """Return the virtual current time."""
        return self.now

    @property
    def total(self) -> float:
        """Total virtual time spent sleeping."""
        return sum(self.delays)


def attempts(
    policy: Optional[RetryPolicy] = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    rand: Callable[[], float] = random.random,
    description: str = "operation",
) -> Iterator["_Attempt"]:
    """Iterate attempts, for code that does not fit in a callable.

    ::

        for attempt in attempts(policy):
            with attempt:
                cursor.execute(sql, params)

    Each iteration yields a context manager. Leaving the block without an
    exception ends the loop; a retryable exception is swallowed and the loop
    sleeps and continues; a non-retryable exception or the last attempt
    propagates.
    """
    policy = policy or RetryPolicy()
    started = monotonic()
    number = 0
    while True:
        number += 1
        attempt = _Attempt(number, policy, started, monotonic, rand, description)
        yield attempt
        if attempt.succeeded:
            return
        if attempt.delay:
            sleep(attempt.delay)


class _Attempt:
    """One attempt produced by :func:`attempts`. Not constructed directly."""

    def __init__(
        self,
        number: int,
        policy: RetryPolicy,
        started: float,
        monotonic: Callable[[], float],
        rand: Callable[[], float],
        description: str,
    ) -> None:
        self.number = number
        self.policy = policy
        self.succeeded = False
        self.delay = 0.0
        self._started = started
        self._monotonic = monotonic
        self._rand = rand
        self._description = description

    def __enter__(self) -> "_Attempt":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc is None:
            self.succeeded = True
            return False
        elapsed = self._monotonic() - self._started
        setattr(exc, "pydb_attempts", self.number)
        setattr(exc, "pydb_elapsed", elapsed)
        if not self.policy.should_retry(exc):
            return False
        if self.number >= self.policy.max_attempts:
            return False
        delay = compute_backoff(self.number, self.policy, self._rand)
        if self.policy.max_elapsed and (elapsed + delay) > self.policy.max_elapsed:
            return False
        if self.policy.on_retry is not None:
            try:
                self.policy.on_retry(exc, self.number, delay)
            except Exception:
                log.debug("on_retry hook raised; ignoring", exc_info=True)
        self.delay = delay
        log.info(
            "%s failed on attempt %d/%d (%s); retrying in %.2fs",
            self._description, self.number, self.policy.max_attempts,
            type(exc).__name__, delay,
        )
        return True  # suppress and retry


def wrap_driver_error(exc: BaseException, message: str, **context: Any) -> PyDBError:
    """Wrap a driver exception in a :class:`PyDBError`, preserving the cause.

    Used by the connection layer so callers can catch ``PyDBError`` without
    losing the driver's error code, which stays reachable on ``__cause__``.
    """
    from .exceptions import QueryError

    err = QueryError(f"{message}: {type(exc).__name__}: {exc}", **context)
    err.__cause__ = exc
    return err
