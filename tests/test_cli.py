import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

from streamjson.cli import main


def run(args):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(args)
    return code, out.getvalue(), err.getvalue()


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write(self, name, text):
        path = Path(self.tmp.name) / name
        path.write_text(text, encoding="utf-8")
        return str(path)

    def test_parse_emits_each_value(self):
        path = self.write("a.json", '[{"a": 1}, {"b": 2}]')
        code, out, _ = run(["parse", path, "--chunk-size", "1"])
        self.assertEqual(code, 0)
        self.assertEqual([json.loads(line) for line in out.splitlines()],
                         [{"a": 1}, {"b": 2}])

    def test_parse_ndjson(self):
        path = self.write("a.ndjson", '{"a": 1}\n{"b": 2}\n')
        code, out, _ = run(["parse", path, "--mode", "ndjson"])
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 2)

    def test_check_reports_truncation(self):
        path = self.write("bad.json", '{"a": [1, 2')
        code, out, err = run(["check", path])
        self.assertEqual(code, 2)
        self.assertIn("unclosed", err)

    def test_check_json_mode(self):
        path = self.write("bad.json", '{"a": [1, 2')
        code, out, _ = run(["check", path, "--json"])
        self.assertEqual(code, 2)
        payload = json.loads(out)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["expected"], "]")

    def test_check_valid(self):
        path = self.write("ok.json", "[1, 2, 3]")
        code, out, _ = run(["check", path])
        self.assertEqual(code, 0)
        self.assertIn("3 value", out)

    def test_repair(self):
        path = self.write("cut.json", '{"a": 1, "b": [1, 2')
        code, out, _ = run(["repair", path])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"a": 1, "b": [1, 2]})


if __name__ == "__main__":
    unittest.main()
