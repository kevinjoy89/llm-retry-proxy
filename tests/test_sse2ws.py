import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketDenialResponse

from retry_proxy.sse2ws import (
    BridgeError,
    ResponsesSseParser,
    Transcript,
    TurnMetrics,
    _dlp_body,
    _open_with_retries,
    _reject_oversized_websocket_message,
    create_sse2ws_handler,
)
from retry_proxy.key_pool import KeyPool


def _settings(**overrides):
    values = {
        "sse2ws_mode": "bridge",
        "sse2ws_first_event_timeout": 0.1,
        "sse2ws_first_event_retries": 1,
        "max_retries": 10,
        "proxy_api_key": "",
        "dlp_mode": "off",
        "dlp_max_body_bytes": 16_777_216,
        "max_request_body": 64 * 1024 * 1024,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _result(response):
    return SimpleNamespace(
        response=response,
        winner_attempt=1,
        total_sent=1,
        last_status=response.status_code,
        retry_codes=[],
        first_ok=True,
        key_id="",
        key_attempts=[],
        started_at=time.time(),
        key_entry=None,
        response_started_mono=time.monotonic(),
        failure_reason="",
    )


def _sse_response(events):
    body = b"".join(
        b"event: " + event["type"].encode() + b"\n"
        + b"data: " + json.dumps(event, separators=(",", ":")).encode() + b"\n\n"
        for event in events
    )
    return httpx.Response(
        200,
        content=body,
        headers={"content-type": "text/event-stream"},
        request=httpx.Request("POST", "https://upstream.test/v1/responses"),
    )


class ResponsesSseParserTests(unittest.TestCase):
    def test_fragmented_crlf_and_multiline_data_are_parsed(self):
        parser = ResponsesSseParser()

        self.assertEqual(parser.feed(b": keepalive\r\nevent: response.created\r"), [])
        events = parser.feed(
            b'\ndata: {"type":"response.created",\r\n'
            b'data: "response":{"id":"resp-1"}}\r\n\r\n',
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "response.created")
        self.assertEqual(events[0].payload["response"]["id"], "resp-1")

    def test_done_is_ignored_but_malformed_json_fails(self):
        parser = ResponsesSseParser()

        self.assertEqual(parser.feed(b"data: [DONE]\n\n"), [])
        self.assertTrue(parser.saw_done)
        with self.assertRaisesRegex(BridgeError, "malformed SSE JSON"):
            parser.feed(b"data: {broken}\n\n")


class TranscriptTests(unittest.TestCase):
    def test_incremental_turn_replays_full_transcript(self):
        state = Transcript()
        state.remember(
            "resp-1",
            [{"type": "message", "id": "user-1"}],
            [{"type": "function_call", "id": "call-1"}],
        )

        merged = state.merge({
            "previous_response_id": "resp-1",
            "input": [{"type": "function_call_output", "call_id": "call-1"}],
        })

        self.assertEqual([item["type"] for item in merged], [
            "message", "function_call", "function_call_output",
        ])

    def test_unknown_previous_response_requests_full_retry(self):
        with self.assertRaisesRegex(BridgeError, "Previous response was not found") as raised:
            Transcript().merge({"previous_response_id": "missing", "input": []})
        self.assertEqual(raised.exception.code, "previous_response_not_found")


class DlpBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_block_mode_rejects_sensitive_websocket_input(self):
        from dataclasses import replace
        from retry_proxy.config import settings

        config = replace(
            settings,
            dlp_mode="block",
            dlp_rules=frozenset({"private_key"}),
        )
        body = json.dumps({
            "input": "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
        }).encode()

        with patch("retry_proxy.sse2ws.settings", config), \
                self.assertRaises(BridgeError) as raised:
            await _dlp_body(body)

        self.assertEqual(raised.exception.code, "sensitive_data_blocked")

    async def test_bridge_body_obeys_global_request_limit_when_dlp_is_off(self):
        with patch("retry_proxy.sse2ws.settings", _settings(max_request_body=8)), \
                self.assertRaises(BridgeError) as raised:
            await _dlp_body(b"123456789")

        self.assertEqual(raised.exception.code, "request_body_too_large")

    async def test_raw_websocket_message_limit_is_checked_before_parsing(self):
        websocket = SimpleNamespace(send_text=AsyncMock())
        with patch("retry_proxy.sse2ws.settings", _settings(max_request_body=8)):
            rejected = await _reject_oversized_websocket_message(
                websocket, {"text": "123456789"},
            )

        self.assertTrue(rejected)
        payload = json.loads(websocket.send_text.await_args.args[0])
        self.assertEqual(payload["status"], 413)
        self.assertEqual(payload["error"]["code"], "request_body_too_large")


class _BlockingStream(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = asyncio.Event()

    async def __aiter__(self):
        await asyncio.Future()
        yield b""

    async def aclose(self):
        self.closed.set()


class _SequenceStream(httpx.AsyncByteStream):
    def __init__(self, *chunks, block_after=False):
        self.chunks = chunks
        self.block_after = block_after
        self.closed = asyncio.Event()

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.block_after:
            await asyncio.Future()

    async def aclose(self):
        self.closed.set()


def _streaming_response(stream, **headers):
    return httpx.Response(
        200,
        stream=stream,
        headers={"content-type": "text/event-stream", **headers},
        request=httpx.Request("POST", "https://upstream.test/v1/responses"),
    )


class FirstEventRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_level_400_is_not_reclassified_as_key_failure(self):
        pool = KeyPool(["sk-test"])
        entry = pool.entries[0]
        response = httpx.Response(
            400, json={"error": {"message": "bad request"}},
            request=httpx.Request("POST", "https://upstream.test/v1/responses"),
        )
        result = _result(response)
        result.key_id = entry.key_id
        result.key_entry = entry
        result.key_attempts = [{"key_id": entry.key_id, "available": True}]
        service = SimpleNamespace(request=AsyncMock(return_value=result))
        args = (
            "POST", "https://upstream.test/v1/responses", {},
            b'{"model":"gpt-test","stream":true}', "v1/responses",
            "test", "gpt-test", pool, "session-1",
        )

        with patch("retry_proxy.sse2ws.settings", _settings(
            sse2ws_first_event_retries=0,
        )), self.assertRaises(BridgeError) as raised:
            await _open_with_retries(
                service, args, pool, "session-1", TurnMetrics(),
            )

        self.assertTrue(raised.exception.key_failure_recorded)
        self.assertEqual(entry.total_fail, 0)
        self.assertEqual(entry.cooldown_until, 0)

    async def test_first_event_timeout_closes_attempt_and_retries(self):
        blocked = _BlockingStream()
        stalled = httpx.Response(
            200,
            stream=blocked,
            headers={"content-type": "text/event-stream"},
            request=httpx.Request("POST", "https://upstream.test/v1/responses"),
        )
        healthy = _sse_response([
            {"type": "response.created", "response": {"id": "resp-ok"}},
            {"type": "response.completed", "response": {"id": "resp-ok", "output": []}},
        ])
        service = SimpleNamespace(request=AsyncMock(side_effect=[
            _result(stalled), _result(healthy),
        ]))
        args = (
            "POST", "https://upstream.test/v1/responses", {},
            b'{"model":"gpt-test","stream":true}', "v1/responses",
            "test", "gpt-test", None, "session-1",
        )

        with patch("retry_proxy.sse2ws.settings", _settings(
            sse2ws_first_event_timeout=0.01,
        )):
            opened = await _open_with_retries(
                service, args, None, "session-1", TurnMetrics(),
            )

        self.assertEqual(service.request.await_count, 2)
        self.assertTrue(blocked.closed.is_set())
        self.assertEqual(opened.initial_events[0].event_type, "response.created")
        await opened.result.response.aclose()

    async def test_stalled_pool_key_retries_before_failover(self):
        pool = KeyPool([("slow", "slow"), ("good", "good")])
        selected = []

        class PoolService:
            async def request(self, *_args, **_kwargs):
                entry = pool.pick()
                selected.append(entry.key_id)
                if entry.key_id == "slow":
                    response = httpx.Response(
                        200,
                        stream=_BlockingStream(),
                        headers={"content-type": "text/event-stream"},
                        request=httpx.Request("POST", "https://upstream.test/v1/responses"),
                    )
                else:
                    response = _sse_response([
                        {"type": "response.created", "response": {"id": "resp-ok"}},
                        {"type": "response.completed", "response": {
                            "id": "resp-ok", "output": [],
                        }},
                    ])
                result = _result(response)
                result.key_id = entry.key_id
                result.key_entry = entry
                result.key_attempts = [{"key_id": entry.key_id, "available": None}]
                return result

        args = (
            "POST", "https://upstream.test/v1/responses", {},
            b'{"model":"gpt-test","stream":true}', "v1/responses",
            "test", "gpt-test", pool, "session-1",
        )
        config = _settings(
            sse2ws_first_event_timeout=0.01,
            key_cooldown=30,
            key_cooldown_5xx=30,
            key_cooldown_backoff=False,
            key_cooldown_max=60,
        )

        with patch("retry_proxy.sse2ws.settings", config):
            opened = await _open_with_retries(
                PoolService(), args, pool, "session-1", TurnMetrics(),
            )

        self.assertEqual(selected, ["slow", "slow", "good"])
        self.assertGreater(pool.entries[0].cooldown_until, time.time())
        self.assertEqual(opened.result.key_id, "good")
        await opened.result.response.aclose()

    async def test_done_without_terminal_is_rejected_before_downstream_events(self):
        stream = _SequenceStream(b"data: [DONE]\n\n")
        service = SimpleNamespace(request=AsyncMock(return_value=_result(
            _streaming_response(stream),
        )))
        args = (
            "POST", "https://upstream.test/v1/responses", {},
            b'{"model":"gpt-test","stream":true}', "v1/responses",
            "test", "gpt-test", None, "session-1",
        )

        with patch("retry_proxy.sse2ws.settings", _settings(
            sse2ws_first_event_retries=0,
        )), self.assertRaises(BridgeError) as raised:
            await _open_with_retries(
                service, args, None, "session-1", TurnMetrics(),
            )

        self.assertEqual(raised.exception.code, "missing_terminal")
        self.assertTrue(stream.closed.is_set())


class _Service:
    def __init__(self):
        self.bodies = []

    async def request(self, _method, _url, _headers, body, *_args, **_kwargs):
        payload = json.loads(body)
        self.bodies.append(payload)
        call = len(self.bodies)
        response_id = f"resp-{call}"
        output = [{
            "type": "function_call" if call == 1 else "message",
            "id": "call-1" if call == 1 else "answer-2",
            "call_id": "call-1" if call == 1 else None,
        }]
        return _result(_sse_response([
            {"type": "response.created", "response": {"id": response_id}},
            {"type": "response.output_item.done", "item": output[0]},
            {"type": "response.completed", "response": {
                "id": response_id, "output": output,
            }},
        ]))


class _StallingService:
    def __init__(self):
        self.stream = _BlockingStream()

    async def request(self, *_args, **_kwargs):
        response = httpx.Response(
            200,
            stream=self.stream,
            headers={"content-type": "text/event-stream"},
            request=httpx.Request("POST", "https://upstream.test/v1/responses"),
        )
        return _result(response)


class _SingleResponseService:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    async def request(self, *_args, **_kwargs):
        self.calls += 1
        return _result(self.response)


class _PoolHttpErrorService:
    async def request(self, _method, _url, _headers, _body, _path, _provider,
                      _model, pool, _session_id, **_kwargs):
        entry = pool.pick()
        response = httpx.Response(
            400, json={"error": {"message": "bad request"}},
            request=httpx.Request("POST", "https://upstream.test/v1/responses"),
        )
        result = _result(response)
        result.key_id = entry.key_id
        result.key_entry = entry
        result.key_attempts = [{"key_id": entry.key_id, "available": True}]
        return result


class WebSocketBridgeTests(unittest.TestCase):
    def _app(self, service, store):
        app = FastAPI()
        app.add_api_websocket_route(
            "/{path:path}", create_sse2ws_handler(service, store),
        )
        return app

    def test_disabled_bridge_denies_upgrade_with_426(self):
        app = self._app(SimpleNamespace(), SimpleNamespace())
        config = _settings(sse2ws_mode="off")

        with patch("retry_proxy.sse2ws.settings", config), TestClient(app) as client:
            with self.assertRaises(WebSocketDenialResponse) as raised:
                with client.websocket_connect("/v1/responses"):
                    pass

        self.assertEqual(raised.exception.status_code, 426)

    def test_non_responses_path_denies_upgrade_with_426(self):
        app = self._app(SimpleNamespace(), SimpleNamespace())

        with patch("retry_proxy.sse2ws.settings", _settings()), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "models")), \
                TestClient(app) as client:
            with self.assertRaises(WebSocketDenialResponse) as raised:
                with client.websocket_connect("/v1/models"):
                    pass

        self.assertEqual(raised.exception.status_code, 426)

    def test_invalid_binary_and_unknown_messages_are_rejected(self):
        service = SimpleNamespace(request=AsyncMock())
        app = self._app(service, SimpleNamespace(write=AsyncMock()))

        with patch("retry_proxy.sse2ws.settings", _settings()), \
                patch("retry_proxy.config.settings", _settings()), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_text("{")
                self.assertEqual(websocket.receive_json()["error"]["code"], "invalid_json")
                websocket.send_bytes(b"binary")
                self.assertEqual(
                    websocket.receive_json()["error"]["code"],
                    "unsupported_websocket_message",
                )
                websocket.send_json([])
                self.assertEqual(
                    websocket.receive_json()["error"]["code"],
                    "invalid_request_error",
                )
                websocket.send_json({"type": "unknown"})
                self.assertEqual(
                    websocket.receive_json()["error"]["code"],
                    "unsupported_websocket_event",
                )
        service.request.assert_not_awaited()

    def test_oversized_message_is_rejected_before_json_processing(self):
        service = SimpleNamespace(request=AsyncMock())
        app = self._app(service, SimpleNamespace(write=AsyncMock()))
        config = _settings(max_request_body=32)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_text("{" + "x" * 64)
                error = websocket.receive_json()

        self.assertEqual(error["status"], 413)
        self.assertEqual(error["error"]["code"], "request_body_too_large")
        service.request.assert_not_awaited()

    def test_warmup_and_tool_turns_replay_full_transcript(self):
        service = _Service()
        store = SimpleNamespace(write=AsyncMock())
        app = self._app(service, store)
        config = _settings()

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "generate": False,
                    "input": [{"type": "message", "id": "user-1"}],
                })
                warmup = [websocket.receive_json() for _ in range(3)]
                warmup_id = warmup[-1]["response"]["id"]

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": warmup_id,
                    "input": [],
                })
                first = []
                while not first or first[-1]["type"] != "response.completed":
                    first.append(websocket.receive_json())
                first_id = first[-1]["response"]["id"]

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": first_id,
                    "input": [{
                        "type": "function_call_output",
                        "call_id": "call-1",
                        "output": "done",
                    }],
                })
                second = []
                while not second or second[-1]["type"] != "response.completed":
                    second.append(websocket.receive_json())

        self.assertEqual(len(service.bodies), 2)
        self.assertNotIn("previous_response_id", service.bodies[0])
        self.assertNotIn("previous_response_id", service.bodies[1])
        self.assertEqual(
            [item["type"] for item in service.bodies[1]["input"]],
            ["message", "function_call", "function_call_output"],
        )
        self.assertEqual(store.write.await_count, 2)
        record = store.write.await_args_list[-1].args[0]
        self.assertEqual(record["downstream_transport"], "websocket")
        self.assertEqual(record["upstream_transport"], "sse")

    def test_cancel_before_first_event_closes_upstream_without_closing_websocket(self):
        service = _StallingService()
        store = SimpleNamespace(write=AsyncMock())
        app = self._app(service, store)
        config = _settings(sse2ws_first_event_timeout=5)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                websocket.send_json({"type": "response.cancel"})
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test",
                    "generate": False, "input": [],
                })
                warmup = [websocket.receive_json() for _ in range(3)]

        self.assertEqual(warmup[-1]["type"], "response.completed")
        self.assertTrue(service.stream.closed.is_set())
        self.assertEqual(store.write.await_args.args[0]["stream_status"], "cancelled")

    def test_cancel_during_active_stream_closes_upstream_and_keeps_websocket_open(self):
        created = (
            b'data: {"type":"response.created",'
            b'"response":{"id":"resp-active"}}\n\n'
        )
        stream = _SequenceStream(created, block_after=True)
        service = _SingleResponseService(_streaming_response(stream))
        store = SimpleNamespace(write=AsyncMock())
        app = self._app(service, store)

        with patch("retry_proxy.sse2ws.settings", _settings(
                    sse2ws_first_event_timeout=5,
                )), \
                patch("retry_proxy.config.settings", _settings()), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                websocket.send_json({"type": "response.cancel"})
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test",
                    "generate": False, "input": [],
                })
                warmup = [websocket.receive_json() for _ in range(3)]

        self.assertEqual(warmup[-1]["type"], "response.completed")
        self.assertTrue(stream.closed.is_set())
        self.assertEqual(store.write.await_args.args[0]["stream_status"], "cancelled")

    def test_first_error_event_preserves_only_safe_upstream_headers(self):
        event = (
            b'data: {"type":"error","error":'
            b'{"type":"server_error","code":"overloaded","message":"busy"}}\n\n'
        )
        stream = _SequenceStream(event)
        service = _SingleResponseService(_streaming_response(
            stream,
            **{"x-request-id": "req-safe", "authorization": "Bearer secret"},
        ))
        app = self._app(service, SimpleNamespace(write=AsyncMock()))
        config = _settings(sse2ws_first_event_retries=0)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                error = websocket.receive_json()

        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["code"], "overloaded")
        self.assertEqual(error["headers"], {"x-request-id": "req-safe"})

    def test_request_level_400_does_not_cool_pool_key(self):
        pool = KeyPool(["sk-test"])
        app = self._app(_PoolHttpErrorService(), SimpleNamespace(write=AsyncMock()))
        config = _settings(sse2ws_first_event_retries=0)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {
                    "https://upstream.test/v1": pool,
                }), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                error = websocket.receive_json()

        self.assertEqual(error["status"], 400)
        self.assertEqual(pool.entries[0].total_fail, 0)
        self.assertEqual(pool.entries[0].cooldown_until, 0)

    def test_malformed_stream_after_first_event_is_not_retried(self):
        created = (
            b'data: {"type":"response.created",'
            b'"response":{"id":"resp-broken"}}\n\n'
        )
        stream = _SequenceStream(created, b"data: {broken}\n\n")
        service = _SingleResponseService(_streaming_response(stream))
        store = SimpleNamespace(write=AsyncMock())
        app = self._app(service, store)
        config = _settings(sse2ws_first_event_retries=2)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                error = websocket.receive_json()

        self.assertEqual(error["error"]["code"], "malformed_sse_json")
        self.assertEqual(service.calls, 1)
        self.assertTrue(stream.closed.is_set())
        self.assertEqual(
            store.write.await_args.args[0]["stream_status"], "malformed_sse_json",
        )

    def test_eof_after_first_event_is_not_retried(self):
        created = (
            b'data: {"type":"response.created",'
            b'"response":{"id":"resp-short"}}\n\n'
        )
        stream = _SequenceStream(created)
        service = _SingleResponseService(_streaming_response(stream))
        app = self._app(service, SimpleNamespace(write=AsyncMock()))
        config = _settings(sse2ws_first_event_retries=2)

        with patch("retry_proxy.sse2ws.settings", config), \
                patch("retry_proxy.config.settings", config), \
                patch("retry_proxy.sse2ws.KEY_POOLS", {}), \
                patch("retry_proxy.sse2ws.match_route",
                      return_value=("https://upstream.test/v1", "test", "responses")), \
                TestClient(app) as client:
            with client.websocket_connect("/v1/responses") as websocket:
                websocket.send_json({
                    "type": "response.create", "model": "gpt-test", "input": [],
                })
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                error = websocket.receive_json()

        self.assertEqual(error["error"]["code"], "missing_terminal")
        self.assertEqual(service.calls, 1)


if __name__ == "__main__":
    unittest.main()
