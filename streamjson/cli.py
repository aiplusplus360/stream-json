"""``stream-json`` command-line interface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from .core import JsonStream, StreamError, repair_truncated, truncation_report

__version__ = "0.1.0"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stream-json",
        description="Parse JSON as it streams, and diagnose truncation by offset.",
    )
    parser.add_argument("--version", action="version", version="%(prog)s " + __version__)
    sub = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("parse", "emit each complete value as it arrives"),
        ("check", "validate a document and report where it stops"),
        ("repair", "close open constructs and print the salvaged value"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("file", nargs="?", default="-", help="input file; '-' reads stdin")
        cmd.add_argument("--mode", choices=("auto", "array", "object", "ndjson"), default="auto")
        cmd.add_argument("--chunk-size", type=int, default=64,
                         help="bytes per feed (default 64; use 1 to prove incrementality)")
        cmd.add_argument("--json", action="store_true", help="wrap the outcome in a JSON report")
    return parser


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit("cannot read %s: %s" % (path, exc))


def _stream(text: str, mode: str, chunk_size: int):
    stream = JsonStream(mode)
    values = []
    for start in range(0, len(text), max(1, chunk_size)):
        values.extend(stream.feed(text[start:start + max(1, chunk_size)]))
    values.extend(stream.close())
    return values


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    text = _read(args.file)

    if args.command == "parse":
        try:
            values = _stream(text, args.mode, args.chunk_size)
        except StreamError as exc:
            if args.json:
                print(json.dumps({"ok": False, "error": str(exc), "offset": exc.offset}))
            else:
                print("error: %s" % exc, file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps({"ok": True, "count": len(values), "values": values}, indent=2))
        else:
            for value in values:
                print(json.dumps(value, sort_keys=True))
        return 0

    if args.command == "check":
        report = truncation_report(text)
        parsed = False
        try:
            values = _stream(text, args.mode, args.chunk_size)
            parsed = True
        except StreamError as exc:
            values = []
            report["error"] = str(exc)
        if args.json:
            print(json.dumps({"ok": parsed, "values": len(values), **report}, indent=2))
        elif parsed:
            print("ok: %d value(s)" % len(values))
        else:
            print("%s at offset %d (expected %s)"
                  % (report["message"], report["offset"], report["expected"]), file=sys.stderr)
        return 0 if parsed else 2

    # repair
    try:
        repaired = repair_truncated(text)
    except StreamError as exc:
        print("cannot repair: %s" % exc, file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({"ok": True, "repaired": repaired}, indent=2))
    else:
        print(json.dumps(repaired, indent=2, sort_keys=True))
    return 0
