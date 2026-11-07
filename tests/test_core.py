import json
import unittest

from streamjson.core import (
    JsonStream,
    StreamError,
    iter_ndjson,
    parse_stream,
    repair_truncated,
    scan_value,
    truncation_report,
)


class ScanTests(unittest.TestCase):
    def test_object(self):
        text = '{"a": [1, 2]}'
        self.assertEqual(scan_value(text, 0), (len(text), ""))

    def test_string(self):
        self.assertEqual(scan_value('"hi"', 0), (4, ""))

    def test_escaped_quote_does_not_close(self):
        text = '"a\\"b"'
        self.assertEqual(scan_value(text, 0), (len(text), ""))

    def test_incomplete_string(self):
        self.assertEqual(scan_value('"abc', 0), (None, "incomplete"))

    def test_incomplete_container(self):
        self.assertEqual(scan_value('{"a": 1', 0), (None, "incomplete"))

    def test_complete_literal(self):
        self.assertEqual(scan_value("true", 0), (4, ""))


class StreamTests(unittest.TestCase):
    def test_array_yields_elements_early(self):
        stream = JsonStream()
        self.assertEqual(stream.feed('[{"id": 1},'), [{"id": 1}])
        self.assertEqual(stream.feed('{"id": 2}]'), [{"id": 2}])
        self.assertTrue(stream.done)

    def test_element_split_across_many_feeds(self):
        payload = '[{"a": 1}, {"b": [2, 3]}, "x"]'
        stream = JsonStream()
        seen = []
        for char in payload:
            seen.extend(stream.feed(char))
        seen.extend(stream.close())
        self.assertEqual(seen, [{"a": 1}, {"b": [2, 3]}, "x"])

    def test_braces_inside_strings_do_not_confuse_depth(self):
        payload = '[{"t": "}{"}, {"t": "]}]"}]'
        self.assertEqual(parse_stream(payload), [{"t": "}{"}, {"t": "]}]"}])

    def test_whitespace_between_elements(self):
        self.assertEqual(parse_stream('[\n  1 ,\n 2\n]'), [1, 2])

    def test_empty_array(self):
        self.assertEqual(parse_stream("[]"), [])

    def test_single_object_mode(self):
        self.assertEqual(parse_stream('{"a": 1}', mode="object"), [{"a": 1}])

    def test_auto_detects_object(self):
        self.assertEqual(parse_stream('{"a": {"b": 2}}'), [{"a": {"b": 2}}])

    def test_ndjson(self):
        self.assertEqual(parse_stream('{"a":1}\n{"b":2}\n', mode="ndjson"),
                         [{"a": 1}, {"b": 2}])

    def test_ndjson_streams_line_by_line(self):
        stream = JsonStream("ndjson")
        self.assertEqual(stream.feed('{"a": 1}\n'), [{"a": 1}])
        self.assertEqual(stream.feed('{"b": 2}\n'), [{"b": 2}])

    def test_mismatched_bracket_reports_offset(self):
        with self.assertRaises(StreamError):
            parse_stream("[1, 2}")

    def test_trailing_garbage_rejected(self):
        with self.assertRaises(StreamError):
            parse_stream("[1] garbage")

    def test_close_on_truncated_reports_offset(self):
        stream = JsonStream()
        stream.feed('[{"a": 1}, {"b": [1,')
        with self.assertRaises(StreamError) as caught:
            stream.close()
        self.assertIn("truncated", str(caught.exception))

    def test_chunk_size_does_not_change_result(self):
        payload = json.dumps([{"i": i, "s": "x" * (i % 7)} for i in range(40)])
        expected = json.loads(payload)
        for size in (1, 2, 3, 7, 13, 64):
            self.assertEqual(parse_stream(payload, chunk_size=size), expected, size)


class TruncationTests(unittest.TestCase):
    def test_unclosed_object(self):
        report = truncation_report('{"a": 1')
        self.assertEqual(report["expected"], "}")
        self.assertEqual(report["offset"], 0)

    def test_unclosed_array_is_innermost(self):
        report = truncation_report('{"a": 1, "b": [1, 2')
        self.assertEqual(report["expected"], "]")
        self.assertEqual(report["offset"], 14)

    def test_unterminated_string(self):
        report = truncation_report('{"a": "hello')
        self.assertEqual(report["expected"], '"')
        self.assertEqual(report["offset"], 6)

    def test_trailing_comma(self):
        report = truncation_report('{"a": 1,')
        self.assertIn("trailing comma", report["message"])

    def test_after_colon(self):
        report = truncation_report('{"a":')
        self.assertIn("after ':'", report["message"])

    def test_empty(self):
        self.assertEqual(truncation_report("")["expected"], "a JSON value")

    def test_complete_document(self):
        self.assertIsNone(truncation_report('{"a": 1}')["expected"])


class RepairTests(unittest.TestCase):
    def test_closes_container(self):
        self.assertEqual(repair_truncated('{"a": 1, "b": [1, 2'),
                         {"a": 1, "b": [1, 2]})

    def test_drops_trailing_comma(self):
        self.assertEqual(repair_truncated('{"a": 1, "b": 2,'),
                         {"a": 1, "b": 2})

    def test_closes_string(self):
        self.assertEqual(repair_truncated('{"a": "hel'), {"a": "hel"})

    def test_complete_document_round_trips(self):
        self.assertEqual(repair_truncated('[1, 2, 3]'), [1, 2, 3])

    def test_empty_raises(self):
        with self.assertRaises(StreamError):
            repair_truncated("   ")


class NdjsonTests(unittest.TestCase):
    def test_skips_blank_lines(self):
        self.assertEqual(iter_ndjson(['{"a": 1}', '', '  ', '{"b": 2}']),
                         [{"a": 1}, {"b": 2}])


if __name__ == "__main__":
    unittest.main()
