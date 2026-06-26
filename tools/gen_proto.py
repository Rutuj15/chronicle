#!/usr/bin/env python3
"""Regenerate Chronicle's gRPC stubs from src/chronicle/proto/chronicle.proto.

Run after editing the .proto:

    uv run python tools/gen_proto.py

Why the flags look the way they do: the proto's path *relative to the -I root* is
what protoc bakes into the generated cross-file import (``chronicle_pb2_grpc.py``
imports ``chronicle_pb2``). By rooting both the include path and the output at
``src`` and placing the .proto at ``chronicle/proto/chronicle.proto`` underneath,
that import comes out as ``from chronicle.proto import chronicle_pb2`` -- matching
the package layout exactly, with no import-rewriting post-processing.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
PROTO = SRC / "chronicle" / "proto" / "chronicle.proto"


def main() -> int:
    if not PROTO.exists():
        print(f"error: {PROTO} not found", file=sys.stderr)
        return 1
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{SRC}",
        f"--python_out={SRC}",
        f"--grpc_python_out={SRC}",
        f"--pyi_out={SRC}",
        str(PROTO),
    ]
    subprocess.run(cmd, check=True, cwd=ROOT)
    print(f"generated gRPC stubs under {SRC / 'chronicle' / 'proto'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
