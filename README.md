# Chronicle

A minimal, readable durable-workflow execution engine — event sourcing plus
deterministic replay, built from scratch in Python.

This is a learning project, not a Temporal replacement. The goal is to build the
core ideas of durable execution from first principles and end up with the right
shape: an engine, a worker runtime, a client SDK, and a gRPC protocol. "Minimal
and readable" is a hard constraint — when two designs compete, the simpler, more
explainable one wins.

## The core idea

A workflow is a deterministic function of its definition, its inputs, and a
recorded event log:

```
workflow state = f(workflow_fn, input_args, event_log)
```

The function *is* the checkpoint. You can't pickle a half-finished coroutine or a
half-sent HTTP request, but you can record "activity X returned Y" and re-run the
orchestration to reach the exact same state. That is why event sourcing isn't a
gimmick here — it's the only thing that makes durable execution work.

Three properties fall out of that and drive the rest of the design:

- **Durable execution.** Long-running workflows survive crashes and resume
  exactly where they left off.
- **Event sourcing.** Workflow state is never stored directly; it is
  reconstructed by replaying an append-only history.
- **Deterministic replay.** On resume the workflow re-runs from the start, and
  the runtime feeds back recorded results instead of re-executing. So a workflow
  must be deterministic — every non-deterministic op (wall-clock, IO, randomness)
  becomes a command the runtime intercepts.

## Architecture

```
   app ──► CLIENT SDK          start workflows, read results
                 │  gRPC
                 ▼
           ENGINE             append-only event log (SQLite), replay +
                                determinism guard, task queues, timers,
                                retry policies                ← durable "brain"
                 │  gRPC task queue (poll)
                 ▼
           WORKER RUNTIME      poll for tasks, run activity code, report results

   plus the gRPC + Protobuf protocol tying it together
```

- **Workflow code** — deterministic orchestration. No IO, no randomness, no
  wall-clock. Drives the flow by awaiting activities. Re-executed on replay.
- **Activity code** — the real, side-effectful work (HTTP, DB, files). May fail.
  Runs once per execution; never replayed.
- **Engine** — owns the durable event log, drives replay, mints idempotency
  keys, and hands work to workers.
- **Worker** — polls the engine, runs each activity under its retry/timeout
  policy, and reports the outcome.
- **Client SDK** — starts workflows and reads their results.

## Use it

Install from git (not on PyPI yet):

```bash
uv add git+https://github.com/Rutuj15/chronicle   # or: pip install git+...
```

Write a workflow (deterministic) and an activity (the real work), then run it:

```python
import asyncio

from chronicle.core.context import WorkflowContext
from chronicle.core.history import InMemoryEventLog
from chronicle.core.runtime import ActivityRegistry, run


async def greet(name: str) -> str:  # activity — real side effects live here
    return f"Hello {name}"


async def hello(ctx: WorkflowContext, name: str) -> str:  # workflow — deterministic
    return await ctx.activity("greet", name)


registry: ActivityRegistry = {"greet": greet}
print(asyncio.run(run(hello, ("world",), InMemoryEventLog(), registry)))  # Hello world
```

Swap `InMemoryEventLog` for `SqliteEventLog` to survive a restart; to run workers
on separate processes, see [`docs/getting-started.md`](docs/getting-started.md).

## Develop

Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                       # install dependencies
uv run pytest                 # 76 tests
```

Run an example end to end. This one kills the engine mid-workflow, restarts it on
the same durable database, and every in-flight workflow reconstructs and
completes:

```bash
uv run python examples/engine_crash_demo.py demo
```

One example per concept: `hello_world.py`, `durable_restart.py`, `durable_timer.py`,
`idempotent_charge.py`, `concurrent_workflows.py`, `distributed_demo.py`.

## Project layout

```
src/chronicle/
├── core/        durable-execution primitives: events, context, the replay
│                runtime, the event-log seam (history), retry, serialization
├── server/      engine servicer + the leased durable task queue
├── client/      client SDK — start_workflow / get_result
├── worker/      worker runtime — poll → run activity → report
└── proto/       gRPC + Protobuf wire contract (generated stubs checked in)
```

## A few design notes

- **One loop, three modes.** First run, pure replay, and crash-resume are all the
  same `.send()`-driven driver. Which branch runs depends only on whether the
  cursor is still inside the recorded history.
- **The determinism guard compares intent, not results** — command type, activity
  name, and args. Results legitimately exist only in the event, so changing a
  retry policy between workflow versions does not trip the guard.
- **One `fsync` per event is the durability boundary.** Activities therefore run
  at-least-once; exactly-once is the activity's job, via idempotency keys the
  engine mints.
- **JSON payloads, never pickle.** On decode, `args` is coerced back to a tuple,
  so `("x",) != ["x"]` does not falsely trip the guard after a round trip.
- **Leasing, not deletion.** A taken task is hidden behind a visibility deadline;
  a lost worker's lease expires and the task redelivers. Redelivery reuses the
  same task id, so it resolves the same parked future and a late duplicate is a
  no-op.

## Tooling

`ruff` (lint + format), `mypy --strict`, `pytest`, and `buf lint` for the `.proto`
contract. Regenerate the gRPC stubs with `uv run python tools/gen_proto.py`.
