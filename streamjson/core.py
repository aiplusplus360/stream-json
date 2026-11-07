"""Parse JSON incrementally, and explain exactly where a document stops.

Two problems, one scanner.

**Streaming.** A model emits a JSON array of findings token by token. Waiting
for the closing bracket means waiting for the last token; you can render the
first finding while the rest is still being generated. This module emits each
complete element the moment its closing character arrives.

**Truncation.** A response cut at the token limit produces invalid JSON, and
``json.loads`` says "Expecting ',' delimiter: line 1 column 812". That number
is where the parser gave up, not where the document stops being complete, and
it cannot tell you that four keys are missing. ``truncation_report`` walks the
same scanner and reports the innermost unfinished construct by offset.

>>> stream = JsonStream()
>>> stream.feed('[{"a": 1}, {"b"')
[{'a': 1}]
>>> stream.feed(': 2}]')
[{'b': 2}]
>>> stream.done
True
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

__all__ = [
    "Chunk",
    "JsonStream",
    "StreamError",
    "scan_value",
    "truncation_report",
    "repair_truncated",
    "iter_ndjson",
    "parse_stream",
]

_WS = " \t\r\n"
_PRIMITIVE = set("-+.eE0123456789truefalsnuN")
_DELIMITERS = set(",]}") | set(_WS)


class StreamError(ValueError):
    """A structurally invalid document, with the offset that caused it."""

    def __init__(self, message: str, offset: int):
        super().__init__("%s (at offset %d)" % (message, offset))
        self.message = message
        self.offset = offset


@dataclass(frozen=True)
class Chunk:
    """One complete value lifted out of the stream."""

    value: Any
    start: int
    end: int
    raw: str


def _skip_ws(text: str, index: int) -> int:
    while index < len(text) and text[index] in _WS:
        index += 1
    return index


def _scan_string(text: str, index: int) -> Tuple[Optional[int], str]:
    """Return the index just past a closing quote, or (None, 'incomplete')."""
    index += 1
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == '"':
            return index + 1, ""
        index += 1
    return None, "incomplete"


def _scan_container(text: str, index: int) -> Tuple[Optional[int], str]:
    opener = text[index]
    closer = "}" if opener == "{" else "]"
    depth = 0
    stack: List[str] = []
    while index < len(text):
        char = text[index]
        if char == '"':
            end, problem = _scan_string(text, index)
            if end is None:
                return None, problem
            index = end
            continue
        if char in "{[":
            stack.append(char)
            depth += 1
        elif char in "}]":
            if not stack:
                return None, "unexpected %r" % char
            expected = "}" if stack[-1] == "{" else "]"
            if char != expected:
                return None, "expected %r but found %r" % (expected, char)
            stack.pop()
            depth -= 1
            if depth == 0:
                return index + 1, ""
        index += 1
    return None, "incomplete"


def scan_value(text: str, index: int) -> Tuple[Optional[int], str]:
    """Find the end of the value starting at ``index``.

    Returns ``(end, "")`` when the value is complete, ``(None, "incomplete")``
    when more input could still change the answer, or ``(None, problem)``.

    >>> scan_value('[1, 2]', 0)
    (6, '')
    >>> scan_value('"abc', 0)
    (None, 'incomplete')
    """
    index = _skip_ws(text, index)
    if index >= len(text):
        return None, "incomplete"
    char = text[index]
    if char in "{[":
        return _scan_container(text, index)
    if char == '"':
        return _scan_string(text, index)
    if char == ",":
        return None, "unexpected ','"
    if char in "}]":
        return None, "unexpected %r" % char
    # A primitive: complete only once a delimiter (or EOF-terminated token we
    # recognise exactly) appears. A number is never complete at a chunk edge.
    end = index
    while end < len(text) and text[end] not in _DELIMITERS:
        end += 1
    token = text[index:end]
    if end == len(text):
        if token in ("true", "false", "null"):
            return end, ""
        return None, "incomplete"
    if not token:
        return None, "expected a value"
    return end, ""


class JsonStream:
    """Incremental parser that yields complete values as their input arrives.

    Modes: ``array`` (top-level array, yields elements), ``object`` (yields the
    single top-level object at the end), ``auto`` (detect from first byte),
    ``ndjson`` (one value per line).
    """

    def __init__(self, mode: str = "auto"):
        if mode not in ("auto", "array", "object", "ndjson"):
            raise ValueError("unknown mode %r" % mode)
        self.mode = mode
        self._buffer = ""
        self._consumed = 0
        self._opened = False
        self._finished = False
        self._closed = False
        self._active_mode = "unknown"
        self._resolved: Optional[str] = None
        self._error: Optional[StreamError] = None

    # -- introspection -----------------------------------------------------
    @property
    def buffer(self) -> str:
        """The unconsumed tail of the stream."""
        return self._buffer

    @property
    def offset(self) -> int:
        """Absolute offset of the first character in ``buffer``."""
        return self._consumed

    @property
    def done(self) -> bool:
        return self._finished or self._closed

    @property
    def complete(self) -> bool:
        """True when the document itself is finished, whether or not input was closed."""
        return self._finished

    @property
    def error(self) -> Optional[StreamError]:
        return self._error

    # -- feeding -----------------------------------------------------------
    def feed(self, text: str) -> List[Any]:
        """Add ``text`` to the stream and return every value completed by it."""
        if self._closed:
            raise StreamError("feed after close", self._consumed)
        if self._error:
            raise self._error
        self._buffer += text
        return self._drain(final=False)

    def close(self) -> List[Any]:
        """Signal end of input, flush the remainder, and validate completeness."""
        if self._closed:
            return []
        values = self._drain(final=True)
        if self._active_mode == "ndjson" and self._opened and not self._error:
            self._finished = True
        self._closed = True
        tail = _skip_ws(self._buffer, 0)
        if tail < len(self._buffer) or not self._finished:
            if not self._finished and not self._opened and not self._buffer.strip():
                self._error = StreamError("empty document", self._consumed)
                raise self._error
            if tail < len(self._buffer):
                self._error = StreamError("unexpected trailing data", self._consumed + tail)
                raise self._error
            report = truncation_report(self._buffer)
            self._error = StreamError("truncated document: %s" % report["message"],
                                      self._consumed + report["offset"])
            raise self._error
        return values

    # -- internals ---------------------------------------------------------
    def _detect(self) -> str:
        if self.mode != "auto":
            return self.mode
        if self._resolved:
            return self._resolved
        index = _skip_ws(self._buffer, 0)
        if index >= len(self._buffer):
            return "unknown"
        char = self._buffer[index]
        if char == "[":
            self._resolved = "array"
        elif char == "{":
            self._resolved = "object"
        else:
            self._resolved = "ndjson"
        return self._resolved

    def _drain(self, final: bool) -> List[Any]:
        values: List[Any] = []
        if self._finished:
            return values
        mode = self._detect()
        if mode != "unknown":
            self._active_mode = mode
        while True:
            index = _skip_ws(self._buffer, 0)
            if index >= len(self._buffer):
                self._trim(index)
                return values

            if mode == "array" and not self._opened:
                if self._buffer[index] != "[":
                    self._error = StreamError("expected '['", self._consumed + index)
                    raise self._error
                self._buffer = self._buffer[index + 1:]
                self._consumed += index + 1
                self._opened = True
                continue

            if mode == "ndjson":
                newline = self._buffer.find("\n")
                if newline < 0:
                    if not final:
                        self._trim(index)
                        return values
                    newline = len(self._buffer)
                line = self._buffer[:newline].strip()
                if line:
                    try:
                        values.append(json.loads(line))
                    except ValueError as exc:
                        self._error = StreamError("invalid JSON line: %s" % exc,
                                                  self._consumed)
                        raise self._error
                self._buffer = self._buffer[newline + 1 if newline < len(self._buffer) else newline:]
                self._consumed += newline + 1
                self._opened = True
                continue

            if mode == "array":
                if index < len(self._buffer) and self._buffer[index] == "]":
                    self._buffer = self._buffer[index + 1:]
                    self._consumed += index + 1
                    self._opened = False
                    self._finished = True
                    return values
                if index < len(self._buffer) and self._buffer[index] == ",":
                    self._trim(index + 1)
                    continue

            end, problem = scan_value(self._buffer, index)
            if end is None:
                if problem != "incomplete":
                    self._error = StreamError(problem, self._consumed + index)
                    raise self._error
                if final:
                    report = truncation_report(self._buffer[index:])
                    self._error = StreamError(
                        "truncated document: %s" % report["message"],
                        self._consumed + index + report["offset"])
                    raise self._error
                self._trim(index)
                return values

            raw = self._buffer[index:end]
            try:
                value = json.loads(raw)
            except ValueError as exc:
                self._error = StreamError("invalid JSON value: %s" % exc,
                                          self._consumed + index)
                raise self._error
            values.append(value)
            start_abs = self._consumed + index
            self._trim(end)
            if mode == "object":
                self._finished = True
                return values
            if mode == "array":
                # The element is complete; the array may still continue.
                continue

    def _trim(self, index: int) -> None:
        if index:
            self._consumed += index
            self._buffer = self._buffer[index:]


def truncation_report(text: str) -> Dict[str, Any]:
    """Describe the innermost unfinished construct in a truncated document.

    >>> truncation_report('{"a": 1, "b": [1, 2')
    {'message': "unclosed '[' opened at offset 14; expected ']'", 'offset': 14, 'expected': ']', 'path': '$.b'}
    """
    stack: List[Dict[str, Any]] = []
    index = 0
    in_string = False
    string_start = 0
    escaped = False
    length = len(text)

    def current_path() -> str:
        return stack[-1]["path"] if stack else "$"

    while index < length:
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
                # A string directly inside an object that is followed by ':'
                # is a key, and names the next path segment.
                if stack and stack[-1]["char"] == "{":
                    look = _skip_ws(text, index + 1)
                    if look < length and text[look] == ":":
                        stack[-1]["key"] = text[string_start + 1:index]
            index += 1
            continue
        if char == '"':
            in_string = True
            string_start = index
        elif char in "{[":
            parent = stack[-1] if stack else None
            if parent is None:
                path = "$"
            elif parent["char"] == "{":
                path = parent["path"] + "." + (parent["key"] or "?")
            else:
                path = parent["path"] + "[]"
            stack.append({"char": char, "offset": index, "path": path, "key": None})
        elif char in "}]":
            if stack:
                stack.pop()
        index += 1

    if in_string:
        return {
            "message": "unterminated string starting at offset %d" % string_start,
            "offset": string_start,
            "expected": '"',
            "path": current_path(),
        }
    stripped = text.rstrip()
    # A dangling comma or colon is a more specific diagnosis than "the object
    # is unclosed", so it is checked before the stack.
    if stripped.endswith(","):
        return {"message": "trailing comma; expected a value", "offset": len(stripped) - 1,
                "expected": "a JSON value", "path": current_path()}
    if stripped.endswith(":"):
        return {"message": "expected a value after ':'", "offset": len(stripped) - 1,
                "expected": "a JSON value", "path": current_path()}
    if stack:
        opener = stack[-1]
        closer = "}" if opener["char"] == "{" else "]"
        return {
            "message": "unclosed %r opened at offset %d; expected %r"
                       % (opener["char"], opener["offset"], closer),
            "offset": opener["offset"],
            "expected": closer,
            "path": opener["path"],
        }
    if not stripped:
        return {"message": "empty document", "offset": 0, "expected": "a JSON value", "path": "$"}
    return {"message": "document is complete", "offset": len(stripped),
            "expected": None, "path": "$"}


def repair_truncated(text: str) -> Any:
    """Best-effort recovery of a truncated document by closing open constructs.

    >>> repair_truncated('{"a": 1, "b": [1, 2')
    {'a': 1, 'b': [1, 2]}
    """
    candidate = text.rstrip()
    if not candidate:
        raise StreamError("empty document", 0)
    stack: List[str] = []
    in_string = False
    escaped = False
    last_safe = 0
    for index, char in enumerate(candidate):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
                last_safe = index + 1
            continue
        if char == '"':
            in_string = True
        elif char in "{[":
            stack.append("}" if char == "{" else "]")
        elif char in "}]":
            if stack:
                stack.pop()
            last_safe = index + 1
        elif char in _WS or char in ",:":
            last_safe = index + 1
        else:
            last_safe = index + 1

    attempt = candidate
    if in_string:
        attempt += '"'
    trimmed = attempt.rstrip()
    while trimmed.endswith(","):
        trimmed = trimmed[:-1].rstrip()
    attempt = trimmed + "".join(reversed(stack))
    try:
        return json.loads(attempt)
    except ValueError:
        # Fall back to the last syntactically safe prefix.
        prefix = candidate[:last_safe].rstrip().rstrip(",")
        return json.loads(prefix + "".join(reversed(stack))) if stack else json.loads(prefix)


def iter_ndjson(lines) -> List[Any]:
    """Parse newline-delimited JSON, skipping blank lines.

    >>> iter_ndjson(['{"a": 1}', '', '{"b": 2}'])
    [{'a': 1}, {'b': 2}]
    """
    out = []
    for line in lines:
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def parse_stream(text: str, mode: str = "auto", chunk_size: int = 4096) -> List[Any]:
    """Parse ``text`` through the streaming path in fixed-size chunks."""
    stream = JsonStream(mode)
    values: List[Any] = []
    for start in range(0, len(text), chunk_size):
        values.extend(stream.feed(text[start:start + chunk_size]))
    values.extend(stream.close())
    return values
