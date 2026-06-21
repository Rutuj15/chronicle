# Examples

Runnable demos and workflows that exercise the Chronicle engine.

- `spike_coroutine.py` — Phase 0 throwaway proving the manual coroutine-driving
  technique (an `async def` workflow yields a `Command` to a `.send()` loop and
  receives its result back).
