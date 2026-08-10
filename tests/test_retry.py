"""Retry: classification, backoff, limits.

Not one test in this file sleeps. Both the sleep function and the clock are
injected, so a policy with a sixty second cap is exercised in microseconds and
the assertions are on exact values rather than on "roughly".
"""

from __future__ import annotations

import sqlite3

import pytest

from pydbconnect.exceptions import ConfigurationError, QueryError, UnsafeSQLError
from pydbconnect.registry import get_backend
from pydbconnect.retry import (
    RetryPolicy,
    RetryRecorder,
    attempts,
    classify,
    compute_backoff,
    retry,
    retry_call,
)


class Transient(Exception):
    """Stands in for a driver's connection-reset error."""

    def __init__(self) -> None:
        super().__init__("connection reset by peer")


class Permanent(Exception):
    """Stands in for a driver's syntax error."""

    def __init__(self) -> None:
        super().__init__('syntax error at or near "slect"')


def flaky(failures: int, error: type = Transient):
    """Return a callable that raises ``error`` ``failures`` times, then succeeds."""
    state = {"calls": 0}

    def run() -> str:
        state["calls"] += 1
        if state["calls"] <= failures:
            raise error()
        return "ok"

    run.calls = state  # type: ignore[attr-defined]
    return run


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #


def test_transient_message_is_retryable() -> None:
    assert classify(Transient()) is True


def test_syntax_error_is_never_retryable() -> None:
    assert classify(Permanent()) is False


def test_permanent_markers_beat_transient_ones() -> None:
    """"syntax error at or near TIMEOUT" must not be retried for the word timeout."""
    assert classify(Exception("syntax error at or near TIMEOUT")) is False


def test_constraint_violation_is_never_retryable() -> None:
    assert classify(sqlite3.IntegrityError("UNIQUE constraint failed: t.id")) is False


def test_builtin_connection_errors_are_retryable() -> None:
    assert classify(ConnectionResetError("reset")) is True
    assert classify(TimeoutError("timed out")) is True


def test_programming_mistakes_are_never_retryable() -> None:
    for exc in (TypeError("nope"), ValueError("bad"), KeyError("k"), SyntaxError("x")):
        assert classify(exc) is False


def test_library_errors_are_never_retryable() -> None:
    assert classify(ConfigurationError("missing key")) is False
    assert classify(UnsafeSQLError("interpolated")) is False


def test_unknown_errors_are_treated_as_permanent() -> None:
    """Retrying something you do not understand is how incidents get bigger."""
    assert classify(Exception("something entirely novel")) is False


def test_sqlstate_drives_classification() -> None:
    serialisation = Exception("could not serialize access")
    serialisation.sqlstate = "40001"
    duplicate = Exception("nope")
    duplicate.sqlstate = "23505"
    assert classify(serialisation) is True
    assert classify(duplicate) is False


def test_backend_classifier_is_consulted() -> None:
    """SQLite's 'database is locked' is transient; its integrity errors are not."""
    backend = get_backend("sqlite")
    locked = sqlite3.OperationalError("database is locked")
    assert classify(locked, backend_classifier=backend.classify_error) is True
    duplicate = sqlite3.IntegrityError("UNIQUE constraint failed")
    assert classify(duplicate, backend_classifier=backend.classify_error) is False


def test_retry_on_overrides_the_generic_rules() -> None:
    class Odd(Exception):
        pass

    assert classify(Odd("mystery")) is False
    assert classify(Odd("mystery"), retry_on=(Odd,)) is True


def test_never_retry_cannot_be_overridden_by_a_backend() -> None:
    """A classifier saying "retry a TypeError" is wrong and is ignored."""
    assert classify(TypeError("bug"), backend_classifier=lambda e: True) is False


# --------------------------------------------------------------------------- #
# Backoff
# --------------------------------------------------------------------------- #


def test_backoff_grows_exponentially_and_is_capped() -> None:
    policy = RetryPolicy(initial_backoff=1.0, multiplier=2.0, max_backoff=8.0, jitter="none")
    assert [compute_backoff(a, policy) for a in range(1, 6)] == [1.0, 2.0, 4.0, 8.0, 8.0]


def test_full_jitter_stays_within_the_envelope() -> None:
    """Full jitter draws uniformly from [0, ceiling] - that is the whole point."""
    policy = RetryPolicy(initial_backoff=1.0, multiplier=2.0, max_backoff=8.0, jitter="full")
    assert compute_backoff(3, policy, rand=lambda: 0.0) == 0.0
    assert compute_backoff(3, policy, rand=lambda: 1.0) == 4.0
    assert compute_backoff(3, policy, rand=lambda: 0.5) == 2.0


def test_jitter_actually_varies() -> None:
    policy = RetryPolicy(initial_backoff=1.0, max_backoff=10.0, jitter="full")
    draws = {compute_backoff(4, policy) for _ in range(50)}
    assert len(draws) > 10, "full jitter should not produce the same delay every time"


# --------------------------------------------------------------------------- #
# retry_call
# --------------------------------------------------------------------------- #


def test_success_on_the_first_attempt_never_sleeps() -> None:
    recorder = RetryRecorder()
    assert retry_call(lambda: "ok", policy=RetryPolicy(), sleep=recorder) == "ok"
    assert recorder.delays == []


def test_retries_until_success() -> None:
    func = flaky(2)
    recorder = RetryRecorder()
    policy = RetryPolicy(max_attempts=5, initial_backoff=0.5, multiplier=2.0, jitter="none")
    assert retry_call(func, policy=policy, sleep=recorder) == "ok"
    assert func.calls["calls"] == 3
    assert recorder.delays == [0.5, 1.0]


def test_non_retryable_error_fails_on_the_first_attempt() -> None:
    """The headline behaviour: a syntax error is not retried, at all."""
    func = flaky(5, Permanent)
    recorder = RetryRecorder()
    with pytest.raises(Permanent):
        retry_call(func, policy=RetryPolicy(max_attempts=10), sleep=recorder)
    assert func.calls["calls"] == 1
    assert recorder.delays == []


def test_max_attempts_is_honoured() -> None:
    func = flaky(99)
    recorder = RetryRecorder()
    policy = RetryPolicy(max_attempts=4, initial_backoff=0.1, jitter="none", max_elapsed=0)
    with pytest.raises(Transient):
        retry_call(func, policy=policy, sleep=recorder)
    assert func.calls["calls"] == 4
    assert len(recorder.delays) == 3


def test_max_elapsed_stops_early() -> None:
    """The wall-clock limit beats the attempt count, using the injected clock."""
    func = flaky(99)
    recorder = RetryRecorder()
    policy = RetryPolicy(
        max_attempts=50, initial_backoff=1.0, multiplier=2.0,
        max_backoff=100.0, max_elapsed=10.0, jitter="none",
    )
    with pytest.raises(Transient):
        retry_call(func, policy=policy, sleep=recorder, monotonic=recorder.clock)
    # 1 + 2 + 4 = 7 elapsed; the next delay of 8 would exceed 10, so it stops.
    assert recorder.delays == [1.0, 2.0, 4.0]
    assert recorder.total == 7.0


def test_attempt_count_is_attached_to_the_exception() -> None:
    func = flaky(99)
    recorder = RetryRecorder()
    policy = RetryPolicy(max_attempts=3, initial_backoff=0.1, jitter="none", max_elapsed=0)
    with pytest.raises(Transient) as info:
        retry_call(func, policy=policy, sleep=recorder)
    assert info.value.pydb_attempts == 3


def test_on_retry_hook_receives_each_failure() -> None:
    seen = []
    policy = RetryPolicy(
        max_attempts=4, initial_backoff=0.1, jitter="none", max_elapsed=0,
        on_retry=lambda exc, attempt, delay: seen.append((attempt, round(delay, 3))),
    )
    with pytest.raises(Transient):
        retry_call(flaky(99), policy=policy, sleep=RetryRecorder())
    assert seen == [(1, 0.1), (2, 0.2), (3, 0.4)]


def test_on_retry_hook_failure_does_not_break_the_retry() -> None:
    """A broken metrics hook must not turn a recoverable error into an outage."""
    def explode(exc, attempt, delay):
        raise RuntimeError("metrics backend is down")

    policy = RetryPolicy(max_attempts=3, initial_backoff=0.1, jitter="none", on_retry=explode)
    assert retry_call(flaky(1), policy=policy, sleep=RetryRecorder()) == "ok"


def test_policy_none_disables_retrying() -> None:
    func = flaky(1)
    with pytest.raises(Transient):
        retry_call(func, policy=RetryPolicy.none(), sleep=RetryRecorder())
    assert func.calls["calls"] == 1


def test_decorator_form() -> None:
    recorder = RetryRecorder()
    func = flaky(1)

    @retry(RetryPolicy(max_attempts=3, initial_backoff=0.2, jitter="none"), sleep=recorder)
    def wrapped() -> str:
        return func()

    assert wrapped() == "ok"
    assert recorder.delays == [0.2]
    assert wrapped.retry_policy.max_attempts == 3


def test_decorator_passes_arguments_through() -> None:
    @retry(max_attempts=2, sleep=RetryRecorder())
    def add(a: int, b: int = 0) -> int:
        return a + b

    assert add(2, b=3) == 5


def test_attempts_iterator_retries_a_block() -> None:
    recorder = RetryRecorder()
    func = flaky(2)
    policy = RetryPolicy(max_attempts=5, initial_backoff=0.1, jitter="none")
    results = []
    for attempt in attempts(policy, sleep=recorder):
        with attempt:
            results.append(func())
    assert results == ["ok"]
    assert func.calls["calls"] == 3


def test_attempts_iterator_propagates_permanent_errors() -> None:
    policy = RetryPolicy(max_attempts=5, initial_backoff=0.1, jitter="none")
    func = flaky(5, Permanent)
    with pytest.raises(Permanent):
        for attempt in attempts(policy, sleep=RetryRecorder()):
            with attempt:
                func()
    assert func.calls["calls"] == 1


def test_policy_from_settings() -> None:
    from pydbconnect.config import RetrySettings

    policy = RetryPolicy.from_settings(RetrySettings(max_attempts=7, jitter="none"))
    assert policy.max_attempts == 7 and policy.jitter == "none"


def test_query_error_wrapping_keeps_the_cause() -> None:
    """Callers must still be able to reach the driver's error code."""
    original = sqlite3.OperationalError("database is locked")
    wrapped = QueryError("boom")
    wrapped.__cause__ = original
    assert wrapped.__cause__ is original
