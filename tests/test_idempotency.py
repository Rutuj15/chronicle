"""Idempotency via a stable, engine-minted key.

Activities are at-least-once: a crash after execution but before the outcome is
fsync'd re-runs the activity on resume, and a retry re-runs it within a run.
The engine cannot make a side-effecting activity exactly-once -- but it can hand
it a key identical on every execution, so the activity (or the downstream
system) can dedup. These tests pin that contract.
"""

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import JsonValue
from chronicle.history import InMemoryEventLog
from chronicle.retry import RetryPolicy, idempotency_key
from chronicle.runtime import ActivitySpec
from conftest import noop_sleep, run_sync


async def call_activity(ctx: WorkflowContext, name: str, *args: JsonValue) -> JsonValue:
    """Run a single activity -- the smallest idempotency-exercising workflow."""
    return await ctx.activity(name, *args)


# --- idempotency_key unit tests ---------------------------------------------


def test_key_combines_workflow_id_and_position() -> None:
    assert idempotency_key("order-1", 0) == "order-1:0"
    assert idempotency_key("order-1", 1) == "order-1:1"  # different position
    assert idempotency_key("order-2", 0) == "order-2:0"  # different run


# --- key injection ----------------------------------------------------------


def test_idempotent_activity_receives_a_key() -> None:
    seen: list[str] = []

    async def charge(amount: int, *, idempotency_key: str) -> str:
        seen.append(idempotency_key)
        return f"charged-{amount}"

    registry = {"charge": ActivitySpec(charge, idempotent=True)}

    result = run_sync(
        call_activity,
        ("charge", 5),
        InMemoryEventLog(),
        registry,
        workflow_id="order-1",
        sleep=noop_sleep,
    )

    assert result == "charged-5"
    assert seen == ["order-1:0"]  # position 0 in this workflow's log


def test_non_idempotent_activity_gets_no_key() -> None:
    # A plain activity whose signature does NOT accept idempotency_key must still
    # work unchanged -- the runtime injects nothing when the spec isn't idempotent.
    async def greet(name: str) -> str:
        return f"hello {name}"

    registry = {"greet": greet}  # bare callable -> default spec, not idempotent

    result = run_sync(
        call_activity,
        ("greet", "world"),
        InMemoryEventLog(),
        registry,
        sleep=noop_sleep,
    )

    assert result == "hello world"


def test_key_is_stable_across_retries() -> None:
    seen: list[str] = []
    attempts = 0

    async def flaky(*, idempotency_key: str) -> str:
        nonlocal attempts
        seen.append(idempotency_key)
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")
        return "recovered"

    registry = {
        "flaky": ActivitySpec(flaky, retry=RetryPolicy(max_attempts=3), idempotent=True),
    }

    result = run_sync(
        call_activity,
        ("flaky",),
        InMemoryEventLog(),
        registry,
        workflow_id="wf",
        sleep=noop_sleep,
    )

    assert result == "recovered"
    # Every retry of the SAME invocation presents the SAME key.
    assert seen == ["wf:0", "wf:0", "wf:0"]


def test_each_invocation_gets_a_distinct_positional_key() -> None:
    async def two_calls(ctx: WorkflowContext) -> list[JsonValue]:
        a = await ctx.activity("record")  # position 0
        b = await ctx.activity("record")  # position 1
        return [a, b]

    async def record(*, idempotency_key: str) -> str:
        return idempotency_key

    registry = {"record": ActivitySpec(record, idempotent=True)}

    result = run_sync(
        two_calls, (), InMemoryEventLog(), registry, workflow_id="wf", sleep=noop_sleep
    )

    assert result == ["wf:0", "wf:1"]  # distinct positions -> distinct keys


# --- the headline: at-least-once re-execution deduped via the key -----------


def test_re_execution_after_a_lost_commit_dedups_via_key() -> None:
    """The engine re-runs an activity when its outcome wasn't committed; the
    activity's own dedup (keyed on the stable idempotency key) keeps the side
    effect to exactly-once."""
    charges: dict[str, str] = {}  # stands in for the downstream payment system
    charge_calls = 0

    async def charge(amount: int, *, idempotency_key: str) -> str:
        nonlocal charge_calls
        if idempotency_key in charges:  # downstream already saw this key -> no-op
            return charges[idempotency_key]
        charge_calls += 1  # the real side effect
        result = f"charged-{amount}"
        charges[idempotency_key] = result
        return result

    registry = {"charge": ActivitySpec(charge, idempotent=True)}

    # First execution: activity runs, side effect happens, outcome "committed".
    run_sync(
        call_activity,
        ("charge", 5),
        InMemoryEventLog(),
        registry,
        workflow_id="order-1",
        sleep=noop_sleep,
    )
    assert charge_calls == 1

    # Simulate crash-BEFORE-commit: the event was lost (a fresh empty log), but
    # the downstream dedup table (charges) survived -- it is a different
    # durability domain from the engine's own log. Replay finds nothing, crosses
    # into new ground, and re-executes charge with the SAME key (same workflow_id
    # + same position 0).
    result = run_sync(
        call_activity,
        ("charge", 5),
        InMemoryEventLog(),
        registry,
        workflow_id="order-1",
        sleep=noop_sleep,
    )

    assert result == "charged-5"
    assert charge_calls == 1  # NOT 2: the key let the activity dedup its re-run


# --- guard ------------------------------------------------------------------


def test_idempotent_activity_without_workflow_id_raises() -> None:
    async def charge(*, idempotency_key: str) -> str:
        return "charged"

    registry = {"charge": ActivitySpec(charge, idempotent=True)}

    with pytest.raises(ValueError):
        run_sync(call_activity, ("charge",), InMemoryEventLog(), registry, sleep=noop_sleep)
