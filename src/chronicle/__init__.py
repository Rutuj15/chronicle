"""Chronicle — a minimal, readable durable-workflow execution engine.

Workflow state is never stored directly; it is reconstructed by replaying an
append-only event log. Non-deterministic operations (activities, time, sleeps)
are intercepted as *commands*, executed once on first run, and fed back from the
recorded history on replay.

Week 1 scope: event log + deterministic replay engine, in-memory.

Scope so far (Weeks 1-3): deterministic replay engine; durable SQLite
persistence (one fsync per event); real durable timers that survive a killed
worker and resume at their original deadline.
"""

__version__ = "0.1.0"
