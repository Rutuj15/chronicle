"""Type-preserving JSON serialization for the event log.

The in-memory log (``history.InMemoryEventLog``) holds live ``Event`` objects;
a durable log stores *bytes*. This module is the bridge: ``dump_event`` turns an
``Event`` into a JSON string, ``load_event`` turns it back.

Two properties make this more than a ``json.dumps`` of the dataclass:

* **Type preservation.** Events and commands are tagged unions (``Completed`` vs
  ``Failed``; ``ActivityCommand`` vs ``NowCommand``). The determinism guard
  compares a freshly-yielded command against the recorded one by *value*
  (frozen-dataclass equality), and ``runtime._outcome`` pattern-matches on the
  event subclass -- so decoding must reconstruct the *exact* type, not a bare
  dict, or the guard and outcome resolution both break.
* **Tuple fidelity.** ``ActivityCommand.args`` is a ``tuple``. JSON has no
  tuples, so a round trip through ``list`` would leave ``args`` as a list -- and
  ``("world",) != ["world"]``, which would make the guard falsely fire on every
  durable replay. Decoding coerces ``args`` back to a tuple. That single line is
  what lets a serialized log satisfy the determinism guard unchanged.

JSON, not pickle: the result/args payloads are already JSON-native by contract,
JSON is human-inspectable from the ``sqlite3`` CLI, and it stays
portable when the store moves to Postgres. The envelope carries a
``v`` (version) tag so a future schema change can migrate old logs rather than
reject them.
"""

import json
from typing import Any

from chronicle.core.events import (
    ActivityCommand,
    Command,
    Completed,
    Event,
    Failed,
    JsonValue,
    NowCommand,
    SleepCommand,
    TimerFired,
)

_VERSION = 1


def dump_event(event: Event) -> str:
    """Serialize an ``Event`` to a versioned JSON string."""
    return json.dumps(_encode_event(event))


def load_event(payload: str) -> Event:
    """Deserialize a JSON string produced by :func:`dump_event` back into an ``Event``."""
    return _decode_event(json.loads(payload))


# --- encode: Event -> JSON-shaped dict ---------------------------------------


def _encode_event(event: Event) -> dict[str, JsonValue]:
    match event:
        case Completed(command=command, result=result):
            return {
                "v": _VERSION,
                "kind": "completed",
                "command": _encode_command(command),
                "result": result,
            }
        case Failed(command=command, error_type=error_type, error_message=error_message):
            return {
                "v": _VERSION,
                "kind": "failed",
                "command": _encode_command(command),
                "error_type": error_type,
                "error_message": error_message,
            }
        case TimerFired(command=command, deadline=deadline):
            return {
                "v": _VERSION,
                "kind": "timer_fired",
                "command": _encode_command(command),
                "deadline": deadline,
            }
        case _:
            raise AssertionError(f"unknown event type: {type(event).__name__}")


def _encode_command(command: Command) -> dict[str, JsonValue]:
    match command:
        case ActivityCommand(name=name, args=args):
            # Coerce the tuple to a list so the encoded dict is purely JSON-shaped;
            # decoding coerces it back to a tuple (see _decode_command).
            return {"kind": "activity", "name": name, "args": list(args)}
        case NowCommand():
            return {"kind": "now"}
        case SleepCommand(duration=duration):
            return {"kind": "sleep", "duration": duration}
        case _:
            raise AssertionError(f"unknown command type: {type(command).__name__}")


# --- decode: JSON-shaped dict -> Event ---------------------------------------


def _decode_event(data: dict[str, Any]) -> Event:
    version = data["v"]
    if version != _VERSION:
        raise ValueError(f"unsupported event-log envelope version: {version!r}")
    command = _decode_command(data["command"])
    match data["kind"]:
        case "completed":
            return Completed(command=command, result=data["result"])
        case "failed":
            return Failed(
                command=command,
                error_type=data["error_type"],
                error_message=data["error_message"],
            )
        case "timer_fired":
            return TimerFired(command=command, deadline=data["deadline"])
        case kind:
            raise ValueError(f"unknown event kind: {kind!r}")


def _decode_command(data: dict[str, Any]) -> Command:
    match data["kind"]:
        case "activity":
            # CRITICAL: args must be a tuple, never a list -- the determinism guard
            # compares commands by value and tuple != list (see module docstring).
            return ActivityCommand(name=data["name"], args=tuple(data["args"]))
        case "now":
            return NowCommand()
        case "sleep":
            return SleepCommand(duration=data["duration"])
        case kind:
            raise ValueError(f"unknown command kind: {kind!r}")


__all__ = ["dump_event", "load_event"]
