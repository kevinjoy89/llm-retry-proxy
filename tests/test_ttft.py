import unittest

from retry_proxy.api import (
    _consume_responses_sse,
    _finish_responses_stream_state,
    _new_responses_stream_state,
    _response_cache_usage,
    _sse_has_token,
)


class TtftSseTests(unittest.TestCase):
    def test_openai_metadata_does_not_count_as_token(self):
        found, remaining = _sse_has_token(
            b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
        )

        self.assertFalse(found)
        self.assertEqual(remaining, b"")

    def test_openai_content_counts_as_token_across_chunks(self):
        found, remaining = _sse_has_token(
            b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
        )

        self.assertTrue(found)
        self.assertEqual(remaining, b"")

    def test_unknown_non_json_data_counts_as_token(self):
        found, _ = _sse_has_token(b"data: token\n\n")

        self.assertTrue(found)

    def test_unknown_json_data_counts_as_token(self):
        found, _ = _sse_has_token(b'data: {"token":"hello"}\n\n')

        self.assertTrue(found)


class ResponsesStreamStateTests(unittest.TestCase):
    def test_cache_usage_requires_explicit_valid_cached_tokens(self):
        payload = {"response": {"usage": {
            "input_tokens": 4096,
            "input_tokens_details": {"cached_tokens": 2048},
        }}}

        self.assertEqual(_response_cache_usage(payload), (4096, 2048))
        self.assertIsNone(_response_cache_usage({"usage": {"input_tokens": 4096}}))
        self.assertIsNone(_response_cache_usage({"usage": {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 20},
        }}))

    def test_completed_event_captures_cache_usage(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(
            state,
            b'data: {"type":"response.completed","response":{"usage":'
            b'{"input_tokens":4096,"input_tokens_details":{"cached_tokens":0}}}}\n\n',
        )

        self.assertEqual(state["cache_usage"], (4096, 0))

    def test_completed_event_marks_stream_successful_across_chunks(self):
        state = _new_responses_stream_state()

        _consume_responses_sse(state, b'event: response.completed\ndata: {"type":"response.')
        _consume_responses_sse(state, b'completed","response":{"status":"completed"}}\n\n')

        self.assertEqual(
            _finish_responses_stream_state(state, "text/event-stream"),
            ("completed", None, True),
        )

    def test_incomplete_event_is_a_valid_terminal_event(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(state, b'data: {"type":"response.incomplete","response":{}}\r')
        _consume_responses_sse(state, b'\n\r\n')

        self.assertEqual(
            _finish_responses_stream_state(state, "text/event-stream"),
            ("incomplete", None, True),
        )

    def test_embedded_error_status_is_recorded(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(
            state,
            b'event: error\ndata: {"type":"error","error":{"status_code":502}}\n\n',
        )

        self.assertEqual(
            _finish_responses_stream_state(state, "text/event-stream"),
            ("error", 502, False),
        )

    def test_stream_without_terminal_event_is_failed(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(
            state,
            b'data: {"type":"response.output_text.delta","delta":"hello"}\n\n',
        )

        self.assertEqual(
            _finish_responses_stream_state(state, "text/event-stream"),
            ("missing_terminal", None, False),
        )

    def test_cancellation_after_terminal_event_keeps_completed_status(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(
            state,
            b'data: {"type":"response.completed","response":{}}\n\n',
        )

        self.assertEqual(
            _finish_responses_stream_state(
                state, "text/event-stream", override="cancelled",
            ),
            ("completed", None, True),
        )

    def test_cancellation_before_terminal_event_stays_cancelled(self):
        state = _new_responses_stream_state()
        _consume_responses_sse(
            state,
            b'data: {"type":"response.output_item.done","item":{}}\n\n',
        )

        self.assertEqual(
            _finish_responses_stream_state(
                state, "text/event-stream", override="cancelled",
            ),
            ("cancelled", None, False),
        )

    def test_html_bad_gateway_is_not_treated_as_responses_success(self):
        state = _new_responses_stream_state()

        self.assertEqual(
            _finish_responses_stream_state(state, "text/html", saw_html=True),
            ("invalid_content_type", 502, False),
        )


if __name__ == "__main__":
    unittest.main()
