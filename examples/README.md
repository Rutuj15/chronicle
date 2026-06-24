# Examples

Runnable demos and workflows that exercise the Chronicle engine.

- `spike_coroutine.py` — Phase 0 throwaway proving the manual coroutine-driving
  technique (an `async def` workflow yields a `Command` to a `.send()` loop and
  receives its result back).
- `durable_restart.py` — Week 2 definition-of-done: one process records a
  workflow's history to SQLite and exits; a second process opens the same file
  cold and replays it to the identical result with **no** activity re-execution.
