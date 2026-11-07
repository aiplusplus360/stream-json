# stream-json

**Parse JSON while it is still arriving, and when it gets cut off, say exactly which construct is unfinished instead of "Expecting ',' delimiter".**

[![CI](https://github.com/aiplusplus360/stream-json/actions/workflows/ci.yml/badge.svg)](https://github.com/aiplusplus360/stream-json/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.9%2B-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)](pyproject.toml)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

```console
$ stream-json check truncated.json
unclosed '[' opened at offset 14; expected ']' (path $.items)

$ stream-json repair truncated.json
{
  "items": [1, 2]
}
```

---

## Why this exists

A model streaming a JSON array of findings produces tokens, not a document. Two things go wrong if you wait for the closing bracket.

**Latency.** Nothing renders until the final token, because `json.loads` needs the whole string. The first finding was available eight seconds earlier.

**Diagnosis.** When the response hits the token limit, the document ends mid-construct and `json.loads` reports the column where it gave up — a number that tells you nothing about what is missing, and cannot distinguish "truncated" from "malformed".

Both are solved by a scanner that knows where value boundaries are.

## How the scanner works

`JsonStream.feed(text)` appends to a buffer and walks it, maintaining:

- string state, including escapes (`"a\\"b"` does not close early);
- a bracket stack, so `}` inside a string is not a close;
- primitive state, so `12` is not emitted until a delimiter proves the number ended.

When a value completes, it is `json.loads`-ed, emitted, and removed from the buffer. The buffer never holds more than one element plus the input since the last emit.

The `--chunk-size 1` flag in the CLI feeds the document one byte at a time, which is how the test suite proves incrementality. The result is independent of chunk size by construction, and tested for chunk sizes 1, 2, 3, 7, 13 and 64.

## Truncation diagnosis

`truncation_report` walks the same grammar and reports the **innermost** unfinished construct, plus a path:

| Input ends with | Report |
| --- | --- |
| `{"a": "hel` | unterminated string starting at offset 6, expected `"` |
| `{"a": [1, 2` | unclosed `[` at offset 14, expected `]`, path `$.a` |
| `{"a": 1,` | trailing comma; expected a value |
| `{"a":` | expected a value after `':'` |
| `{"a": 1}` | document is complete |

A dangling comma is a more specific diagnosis than "the object is unclosed", so punctuation is checked before the bracket stack.

## Repair

`repair_truncated` closes an open string, drops a trailing comma, and appends the missing closers in reverse order:

```python
>>> repair_truncated('{"a": 1, "b": [1, 2')
{'a': 1, 'b': [1, 2]}
```

It is a recovery tool, not a validator: the salvaged value is what the model had produced, and partial data should be labelled as partial wherever it is displayed.

## Modes

- `array` — top-level array, yields elements as they complete.
- `object` — one top-level object, yielded at the end.
- `ndjson` — one value per line, yielded per line.
- `auto` — latched from the first non-whitespace byte, never re-detected.

Auto-mode latching matters: re-detecting on each feed sees `{` after the `[` was consumed and switches to object mode mid-stream. That bug is in the test suite.

## Install and use

No dependencies. Python 3.9+.

```bash
pip install git+https://github.com/aiplusplus360/stream-json
```

```bash
stream-json parse stream.json --chunk-size 1
stream-json parse events.ndjson --mode ndjson
stream-json check truncated.json --json
stream-json repair truncated.json
```

## Library use

```python
from streamjson import JsonStream

stream = JsonStream()
for piece in token_stream:
    for item in stream.feed(piece):
        render(item)          # first element arrives before the last token
stream.close()                # raises StreamError with the truncation offset
```

## Tests

```bash
python -m unittest discover -s tests -v
```

32 core tests and 6 CLI tests: string escapes, nested brackets, braces inside strings, whitespace, empty arrays, mismatched brackets, trailing garbage, every truncation shape, repair, and identical results across six chunk sizes.
