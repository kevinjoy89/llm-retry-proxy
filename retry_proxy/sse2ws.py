import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import httpx
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.responses import Response

from .api import (
    _key_pool_secrets,
    _request_ip,
    _response_event_status,
    classify_endpoint,
    classify_model_scope,
    outbound_request_headers,
    parse_request_model,
    parse_request_session_id,
)
from .config import can_use_key_pool, logger, settings, should_retry_status
from .dlp import inspect_json_body
from .key_pool import KEY_POOLS
from .retry import _mark_key_failure, _tag, reset_client_ip, set_client_ip
from .routes import is_excluded_path, match_route


_ERROR_EVENT_TYPES = {"error", "response.error", "response.failed"}
_TERMINAL_EVENT_TYPES = {"response.completed", "response.incomplete"}
_SAFE_ERROR_HEADERS = {
    "openai-request-id", "request-id", "retry-after", "x-request-id",
}


class BridgeError(Exception):
    def __init__(self, message, status=502, code="sse2ws_bridge_error", payload=None,
                 headers=None):
        self.message = message
        self.status = status
        self.code = code
        self.payload = payload
        self.headers = headers
        self.key_failure_recorded = False
        super().__init__(message)


class ClientDisconnected(Exception):
    pass


class TurnCancelled(Exception):
    pass


@dataclass
class SseEvent:
    event_type: str
    payload: dict
    text: str


class ResponsesSseParser:
    def __init__(self):
        self.buffer = b""
        self.saw_done = False

    def feed(self, chunk):
        self.buffer += chunk
        trailing_cr = self.buffer.endswith(b"\r")
        source = self.buffer[:-1] if trailing_cr else self.buffer
        source = source.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        self.buffer = source + (b"\r" if trailing_cr else b"")
        events = []
        while b"\n\n" in self.buffer:
            frame, self.buffer = self.buffer.split(b"\n\n", 1)
            event = self._parse_frame(frame)
            if event is not None:
                events.append(event)
        if len(self.buffer) > 1_048_576:
            raise BridgeError("SSE event exceeds 1 MiB", code="invalid_sse_event")
        return events

    def finish(self):
        if self.buffer.strip(b"\r\n"):
            raise BridgeError("upstream SSE ended with a truncated event", code="truncated_sse_event")

    def _parse_frame(self, frame):
        event_name = ""
        data_lines = []
        for line in frame.splitlines():
            if not line or line.startswith(b":"):
                continue
            name, separator, value = line.partition(b":")
            if separator and value.startswith(b" "):
                value = value[1:]
            if name == b"event":
                event_name = value.decode("utf-8", errors="replace")
            elif name == b"data":
                data_lines.append(value)
        if not data_lines:
            return None
        data = b"\n".join(data_lines)
        if data.strip() == b"[DONE]":
            self.saw_done = True
            return None
        try:
            payload = json.loads(data)
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            raise BridgeError(
                "upstream returned malformed SSE JSON",
                code="malformed_sse_json",
            ) from exc
        if not isinstance(payload, dict):
            raise BridgeError("upstream SSE data must be a JSON object", code="invalid_sse_event")
        event_type = str(payload.get("type") or event_name or "").strip()
        if not event_type:
            raise BridgeError("upstream SSE event has no type", code="invalid_sse_event")
        if "type" not in payload:
            payload["type"] = event_type
        return SseEvent(
            event_type,
            payload,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )


@dataclass
class Transcript:
    response_id: str = ""
    input_items: list = field(default_factory=list)
    output_items: list = field(default_factory=list)

    def merge(self, payload):
        previous_id = str(payload.get("previous_response_id") or "").strip()
        incoming = payload.get("input", [])
        if isinstance(incoming, str):
            if previous_id:
                raise BridgeError(
                    "incremental WebSocket input must be an array",
                    status=400,
                    code="invalid_request_error",
                )
            return incoming
        if not isinstance(incoming, list):
            raise BridgeError(
                "WebSocket input must be a string or an array",
                status=400,
                code="invalid_request_error",
            )
        if not previous_id:
            return list(incoming)
        if previous_id != self.response_id:
            raise BridgeError(
                "Previous response was not found. Retrying the full request.",
                status=400,
                code="previous_response_not_found",
            )
        return [*self.input_items, *self.output_items, *incoming]

    def remember(self, response_id, input_items, output_items):
        self.response_id = response_id
        if isinstance(input_items, str):
            self.input_items = [{"role": "user", "content": input_items}]
        else:
            self.input_items = list(input_items) if isinstance(input_items, list) else []
        self.output_items = list(output_items) if isinstance(output_items, list) else []


@dataclass
class OpenedStream:
    result: object
    iterator: object
    parser: ResponsesSseParser
    initial_events: list


@dataclass
class TurnMetrics:
    started_at: float = field(default_factory=time.time)
    total_sent: int = 0
    retry_codes: list = field(default_factory=list)
    key_attempts: list = field(default_factory=list)
    bridge_attempts: int = 0
    last_status: int = 0
    key_id: str = ""
    key_entry: object = None
    first_event_s: float = 0.0
    bridge_retry_reasons: list = field(default_factory=list)

    def add_result(self, result):
        self.total_sent += max(int(getattr(result, "total_sent", 0) or 0), 0)
        self.retry_codes.extend(getattr(result, "retry_codes", None) or [])
        self.key_attempts.extend(getattr(result, "key_attempts", None) or [])
        self.last_status = int(getattr(result, "last_status", 0) or 0)
        self.key_id = getattr(result, "key_id", "") or self.key_id
        self.key_entry = getattr(result, "key_entry", None)


def _safe_headers(headers):
    output = {}
    for name, value in headers.items():
        lower = name.lower()
        if lower in _SAFE_ERROR_HEADERS or lower.startswith(("x-ratelimit-", "x-codex-")):
            output[lower] = value
    return output


def _error_envelope(error, headers=None):
    if error.payload and isinstance(error.payload, dict):
        upstream_error = error.payload.get("error", error.payload)
        if not isinstance(upstream_error, dict):
            upstream_error = {"message": str(upstream_error)}
        upstream_error = dict(upstream_error)
        upstream_error.setdefault("message", error.message)
        upstream_error.setdefault("code", error.code)
    else:
        upstream_error = {
            "type": "server_error" if error.status >= 500 else "invalid_request_error",
            "code": error.code,
            "message": error.message,
        }
    return {
        "type": "error",
        "status": error.status,
        "error": upstream_error,
        "headers": _safe_headers(headers or {}),
    }


async def _send_error(websocket, error, headers=None):
    await websocket.send_text(json.dumps(
        _error_envelope(error, headers if headers is not None else error.headers),
        ensure_ascii=False, separators=(",", ":"),
    ))


def _websocket_message_size(message):
    raw_bytes = message.get("bytes")
    if raw_bytes is not None:
        return len(raw_bytes)
    raw_text = message.get("text")
    return len(raw_text.encode("utf-8")) if isinstance(raw_text, str) else 0


async def _reject_oversized_websocket_message(websocket, message):
    limit = getattr(settings, "max_request_body", 64 * 1024 * 1024)
    if _websocket_message_size(message) <= limit:
        return False
    await _send_error(websocket, BridgeError(
        "Request body exceeds the maximum allowed size", status=413,
        code="request_body_too_large",
    ))
    return True


async def _deny(websocket, status=426, message="Responses WebSocket bridge is unavailable"):
    response = Response(
        json.dumps({"error": {"type": "websocket_unavailable", "message": message}}),
        status_code=status,
        media_type="application/json",
    )
    try:
        await websocket.send_denial_response(response)
    except RuntimeError:
        await websocket.close(code=1008, reason=message[:123])


async def _dlp_body(body):
    max_body = getattr(settings, "max_request_body", 64 * 1024 * 1024)
    if len(body) > max_body:
        raise BridgeError(
            "Request body exceeds the maximum allowed size", status=413,
            code="request_body_too_large",
        )
    if settings.dlp_mode not in ("audit", "block", "redact"):
        return body
    if len(body) > settings.dlp_max_body_bytes:
        if settings.dlp_mode in ("block", "redact"):
            raise BridgeError(
                "Request body exceeds DLP inspection limit",
                status=413,
                code="dlp_body_too_large",
            )
        return body
    result = await asyncio.to_thread(
        inspect_json_body,
        body, settings.dlp_rules, settings.dlp_exempt_start, settings.dlp_exempt_end,
        settings.dlp_strip_exempt_markers, settings.dlp_mode,
        settings.dlp_rule_file, None, settings.dlp_allow_exemptions,
        settings.dlp_decode_depth, settings.dlp_decode_max_candidates,
        settings.dlp_decode_max_bytes, _key_pool_secrets(),
        settings.dlp_known_secret_min_length,
    )
    if result.uninspectable and settings.dlp_fail_closed and body:
        raise BridgeError("Request body cannot be inspected by DLP", status=422,
                          code="dlp_uninspectable_body")
    if result.limit_exceeded and settings.dlp_mode in ("block", "redact"):
        raise BridgeError("Request body exceeds DLP decode inspection limits", status=413,
                          code="dlp_decode_limit_exceeded")
    if result.malformed_exemption and settings.dlp_mode in ("block", "redact"):
        raise BridgeError("Malformed DLP exemption markers", status=422,
                          code="dlp_malformed_exemption")
    if result.blocked_rules:
        raise BridgeError(
            "Request blocked by sensitive data policy", status=422,
            code="sensitive_data_blocked",
            payload={"error": {"rules": list(result.blocked_rules)}},
        )
    if result.matched_rules:
        action = "脱敏" if result.redactions else "告警"
        logger.warning(f"SSE2WS DLP{action} rules={','.join(result.matched_rules)}")
    return result.body


def _event_error(event):
    if event.event_type not in _ERROR_EVENT_TYPES and not isinstance(event.payload.get("error"), dict):
        return None
    status = _response_event_status(event.payload) or 502
    error = event.payload.get("error")
    message = error.get("message") if isinstance(error, dict) else None
    bridge_error = BridgeError(
        message or "upstream returned a Responses error event",
        status=status,
        code=(error.get("code") if isinstance(error, dict) else None) or "upstream_stream_error",
        payload=event.payload,
    )
    bridge_error.stream_event_error = True
    return bridge_error


async def _prime(service, method, url, headers, body, path, provider, model, pool, session_id,
                 result_holder=None):
    response = None
    result = None
    try:
        result = await service.request(
            method, url, headers, body, path, provider, model, pool, session_id,
            defer_stream_success=True,
        )
        if result_holder is not None:
            result_holder["result"] = result
        response = result.response
        if response is None:
            raise BridgeError(
                getattr(result, "failure_reason", "") or "upstream request failed",
                status=503,
                code="upstream_unavailable",
            )
        if response.status_code >= 400:
            raw = await response.aread()
            try:
                payload = json.loads(raw)
            except (ValueError, TypeError, UnicodeDecodeError):
                payload = None
            error = BridgeError(
                "upstream rejected the request", status=response.status_code,
                code="upstream_http_error", payload=payload,
            )
            # RetryProxy has already recorded this HTTP response's key outcome.
            error.key_outcome_recorded = True
            raise error
        if "text/event-stream" not in response.headers.get("content-type", "").lower():
            raise BridgeError("upstream did not return text/event-stream",
                              status=502, code="invalid_content_type")
        parser = ResponsesSseParser()
        iterator = response.aiter_bytes().__aiter__()
        while True:
            try:
                chunk = await iterator.__anext__()
            except StopAsyncIteration as exc:
                parser.finish()
                code = "missing_terminal" if parser.saw_done else "empty_stream"
                raise BridgeError(
                    "upstream stream closed before a valid Responses event",
                    code=code,
                ) from exc
            events = parser.feed(chunk)
            if not events:
                continue
            for event in events:
                error = _event_error(event)
                if error is not None:
                    raise error
            if parser.saw_done and not any(
                    event.event_type in _TERMINAL_EVENT_TYPES for event in events):
                raise BridgeError(
                    "upstream sent [DONE] without a Responses terminal event",
                    code="missing_terminal",
                )
            opened = OpenedStream(result, iterator, parser, events)
            response = None
            return opened
    except BridgeError as exc:
        exc.result = result
        if response is not None:
            exc.headers = response.headers
        raise
    finally:
        if response is not None:
            await response.aclose()


def _confirm_key_success(opened, pool, session_id, metrics):
    entry = getattr(opened.result, "key_entry", None)
    if pool is not None and entry is not None:
        pool.mark_success(entry, session_id=session_id)
        sent_at = getattr(opened.result, "response_started_mono", 0.0)
        if sent_at > 0:
            pool.record_ttft(entry, time.monotonic() - sent_at)
        for attempt in reversed(metrics.key_attempts):
            if attempt.get("key_id") == entry.key_id and attempt.get("available") is None:
                attempt["available"] = True
                break


def _mark_deferred_failure(pool, entry, session_id, metrics, status=0):
    if entry is None:
        return
    for attempt in metrics.key_attempts:
        if attempt.get("key_id") == entry.key_id:
            attempt["available"] = False
    _mark_key_failure(pool, entry, settings, status, session_id=session_id)


def _is_key_failure_status(status):
    return status == 0 or status >= 500 or status in (429, 401, 403)


async def _wait_for_prime(websocket, awaitable, timeout):
    if websocket is None:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    prime_task = asyncio.create_task(awaitable)
    deadline = time.monotonic() + timeout
    try:
        while True:
            receive_task = asyncio.create_task(websocket.receive())
            remaining = max(deadline - time.monotonic(), 0.0)
            done, _ = await asyncio.wait(
                (prime_task, receive_task), timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                receive_task.cancel()
                await asyncio.gather(receive_task, return_exceptions=True)
                raise asyncio.TimeoutError
            if prime_task in done:
                receive_task.cancel()
                await asyncio.gather(receive_task, return_exceptions=True)
                return await prime_task
            message = await receive_task
            if message.get("type") == "websocket.disconnect":
                raise ClientDisconnected
            if await _reject_oversized_websocket_message(websocket, message):
                continue
            raw = message.get("text")
            if raw is None and message.get("bytes") is not None:
                await _send_error(websocket, BridgeError(
                    "binary WebSocket messages are not supported", status=400,
                    code="unsupported_websocket_message",
                ))
                continue
            try:
                payload = json.loads(raw or "") if raw is not None else {}
            except ValueError:
                payload = {}
            if isinstance(payload, dict) and payload.get("type") == "response.cancel":
                raise TurnCancelled
            await _send_error(websocket, BridgeError(
                "only response.cancel is allowed while waiting for the first event",
                status=409,
                code="response_in_progress",
            ))
    finally:
        if not prime_task.done():
            prime_task.cancel()
        await asyncio.gather(prime_task, return_exceptions=True)


async def _open_with_retries(service, request_args, pool, session_id, metrics, websocket=None):
    retries = max(settings.sse2ws_first_event_retries, 0)
    per_key_limit = retries + 1
    key_count = max(len(getattr(pool, "entries", ())), 1)
    total_limit = per_key_limit * key_count
    if settings.max_retries > 0:
        total_limit = min(total_limit, max(settings.max_retries, per_key_limit))
    failures = {}
    last_error = BridgeError("upstream did not produce a first event", status=504,
                             code="first_event_timeout")

    for _ in range(total_limit):
        metrics.bridge_attempts += 1
        opened = None
        result = None
        holder = {}
        try:
            opened = await _wait_for_prime(
                websocket,
                _prime(service, *request_args, result_holder=holder),
                settings.sse2ws_first_event_timeout,
            )
            result = opened.result
            metrics.add_result(result)
            metrics.first_event_s = time.time() - metrics.started_at
            _confirm_key_success(opened, pool, session_id, metrics)
            return opened
        except asyncio.TimeoutError:
            result = holder.get("result")
            if result is not None:
                metrics.add_result(result)
            else:
                metrics.total_sent += 1
            last_error = BridgeError(
                f"upstream did not produce a first event within {settings.sse2ws_first_event_timeout:.1f}s",
                status=504,
                code="first_event_timeout",
            )
        except BridgeError as exc:
            last_error = exc
            if getattr(exc, "key_outcome_recorded", False):
                exc.key_failure_recorded = True
            result = getattr(exc, "result", None)
            if result is not None:
                metrics.add_result(result)
            if (exc.code == "upstream_unavailable"
                    or (exc.status < 500 and not should_retry_status(exc.status))):
                raise
        except (TurnCancelled, ClientDisconnected):
            result = holder.get("result")
            if result is not None:
                metrics.add_result(result)
            else:
                metrics.total_sent += 1
            raise

        metrics.bridge_retry_reasons.append(last_error.code)
        entry = getattr(result, "key_entry", None) if result is not None else None
        marker = getattr(entry, "key_id", "") or "__passthrough__"
        failures[marker] = failures.get(marker, 0) + 1
        if failures[marker] >= per_key_limit:
            if _is_key_failure_status(last_error.status):
                _mark_deferred_failure(pool, entry, session_id, metrics, last_error.status)
                last_error.key_failure_recorded = True
            if pool is None or not pool.has_fresh():
                break
        logger.warning(
            f"SSE2WS首事件失败 attempt={metrics.bridge_attempts}/{total_limit} "
            f"key={marker} code={last_error.code}"
        )
    raise last_error


async def _receive_during_stream(websocket, chunk_task):
    receive_task = asyncio.create_task(websocket.receive())
    try:
        done, _ = await asyncio.wait(
            (chunk_task, receive_task), return_when=asyncio.FIRST_COMPLETED,
        )
        if receive_task in done:
            return "client", await receive_task
        receive_task.cancel()
        await asyncio.gather(receive_task, return_exceptions=True)
        return "chunk", await chunk_task
    finally:
        if not receive_task.done():
            receive_task.cancel()


async def _relay(websocket, opened):
    response = opened.result.response
    parser = opened.parser
    response_id = ""
    output_items = []
    terminal = ""
    sent = False

    async def emit(events):
        nonlocal response_id, terminal, sent, output_items
        for event in events:
            error = _event_error(event)
            if error is not None:
                error.headers = response.headers
                raise error
            response_data = event.payload.get("response")
            if isinstance(response_data, dict) and response_data.get("id"):
                response_id = str(response_data["id"])
            if event.event_type == "response.output_item.done" and isinstance(event.payload.get("item"), dict):
                output_items.append(event.payload["item"])
            if event.event_type == "response.completed" and isinstance(response_data, dict):
                if isinstance(response_data.get("output"), list):
                    output_items = response_data["output"]
            await websocket.send_text(event.text)
            sent = True
            if event.event_type in _TERMINAL_EVENT_TYPES:
                terminal = event.event_type
                return True
        return False

    try:
        if await emit(opened.initial_events):
            return terminal, response_id, output_items, sent
        chunk_task = asyncio.create_task(opened.iterator.__anext__())
        while True:
            kind, value = await _receive_during_stream(websocket, chunk_task)
            if kind == "client":
                message_type = value.get("type")
                if message_type == "websocket.disconnect":
                    chunk_task.cancel()
                    await asyncio.gather(chunk_task, return_exceptions=True)
                    raise ClientDisconnected
                if await _reject_oversized_websocket_message(websocket, value):
                    continue
                raw = value.get("text")
                if raw is None and value.get("bytes") is not None:
                    await _send_error(websocket, BridgeError(
                        "binary WebSocket messages are not supported", status=400,
                        code="unsupported_websocket_message",
                    ))
                    continue
                try:
                    payload = json.loads(raw or "")
                except ValueError:
                    await _send_error(websocket, BridgeError(
                        "invalid WebSocket JSON", status=400, code="invalid_json",
                    ))
                    continue
                if not isinstance(payload, dict):
                    await _send_error(websocket, BridgeError(
                        "WebSocket JSON must be an object", status=400,
                        code="invalid_request_error",
                    ))
                    continue
                if payload.get("type") == "response.cancel":
                    chunk_task.cancel()
                    await asyncio.gather(chunk_task, return_exceptions=True)
                    return "cancelled", response_id, output_items, sent
                await _send_error(websocket, BridgeError(
                    "only one response may be in flight", status=409,
                    code="response_in_progress",
                ))
                continue
            try:
                events = parser.feed(value)
            except BridgeError:
                raise
            if await emit(events):
                return terminal, response_id, output_items, sent
            chunk_task = asyncio.create_task(opened.iterator.__anext__())
    except StopAsyncIteration as exc:
        parser.finish()
        code = "missing_terminal" if sent else "empty_stream"
        raise BridgeError("upstream stream closed before response.completed", code=code) from exc
    finally:
        await response.aclose()


async def _write_turn_log(store, path, provider, model, upstream, request_pool, client_ip,
                          metrics, final_status, stream_status, succeeded):
    await store.write({
        "ts": datetime.now().isoformat(timespec="milliseconds"),
        "method": "POST",
        "path": "/" + path,
        "provider": provider,
        "model": model,
        "upstream_status": metrics.last_status,
        "final_status": final_status,
        "attempts": metrics.total_sent,
        "retries": max(metrics.total_sent - 1, 0),
        "retry_codes": metrics.retry_codes,
        "mode": "off",
        "first_ok": metrics.total_sent == 1 and succeeded,
        "key_id": metrics.key_id,
        "key_pool": upstream if request_pool is not None else "",
        "key_attempts": metrics.key_attempts,
        "client_ip": client_ip,
        "duration_s": round(time.time() - metrics.started_at, 3),
        "succeeded": succeeded,
        "stream_status": stream_status,
        "downstream_transport": "websocket",
        "upstream_transport": "sse",
        "bridge_retries": max(metrics.bridge_attempts - 1, 0),
        "bridge_retry_reasons": metrics.bridge_retry_reasons,
        "first_event_s": round(metrics.first_event_s, 3) if metrics.first_event_s else None,
    })


def create_sse2ws_handler(service, store):
    async def websocket_proxy(websocket: WebSocket, path: str):
        if (settings.sse2ws_mode != "bridge" or is_excluded_path(path)):
            await _deny(websocket)
            return
        upstream, provider, remaining = match_route(path)
        if classify_endpoint(remaining) != "responses":
            await _deny(websocket, message="WebSocket bridge only supports Responses API paths")
            return
        if settings.sse2ws_first_event_timeout <= 0 or settings.sse2ws_first_event_retries < 0:
            await _deny(websocket, status=503, message="Invalid SSE2WS timeout configuration")
            return

        await websocket.accept()
        client_ip = _request_ip(websocket)
        transcript = Transcript()
        base_pool = KEY_POOLS.get(upstream)
        pool_credential_ok = can_use_key_pool(websocket.headers)
        url = f"{upstream}/{remaining}" if remaining else upstream
        if websocket.url.query:
            url += f"?{websocket.url.query}"

        while True:
            try:
                message = await websocket.receive()
                if message.get("type") == "websocket.disconnect":
                    return
                if await _reject_oversized_websocket_message(websocket, message):
                    continue
                if message.get("bytes") is not None:
                    await _send_error(websocket, BridgeError(
                        "binary WebSocket messages are not supported", status=400,
                        code="unsupported_websocket_message",
                    ))
                    continue
                try:
                    payload = json.loads(message.get("text") or "")
                except ValueError:
                    await _send_error(websocket, BridgeError(
                        "invalid WebSocket JSON", status=400, code="invalid_json",
                    ))
                    continue
                if not isinstance(payload, dict):
                    await _send_error(websocket, BridgeError(
                        "WebSocket JSON must be an object", status=400,
                        code="invalid_request_error",
                    ))
                    continue
                if payload.get("type") != "response.create":
                    await _send_error(websocket, BridgeError(
                        "expected response.create", status=400,
                        code="unsupported_websocket_event",
                    ))
                    continue

                try:
                    merged_input = transcript.merge(payload)
                    http_payload = dict(payload)
                    http_payload.pop("type", None)
                    http_payload.pop("generate", None)
                    http_payload.pop("background", None)
                    http_payload.pop("previous_response_id", None)
                    http_payload["input"] = merged_input
                    http_payload["stream"] = True
                    body = json.dumps(http_payload, ensure_ascii=False,
                                      separators=(",", ":")).encode("utf-8")
                    body = await _dlp_body(body)
                    sanitized = json.loads(body)
                    merged_input = sanitized.get("input", merged_input)
                except BridgeError as exc:
                    await _send_error(websocket, exc)
                    continue

                if payload.get("generate") is False:
                    response_id = "resp_sse2ws_" + uuid.uuid4().hex
                    response_base = {
                        "id": response_id,
                        "object": "response",
                        "model": str(payload.get("model") or ""),
                        "output": [],
                    }
                    for index, (event_type, status) in enumerate((
                        ("response.created", "in_progress"),
                        ("response.in_progress", "in_progress"),
                        ("response.completed", "completed"),
                    )):
                        event = {
                            "type": event_type,
                            "sequence_number": index,
                            "response": {**response_base, "status": status},
                        }
                        await websocket.send_text(json.dumps(
                            event, ensure_ascii=False, separators=(",", ":"),
                        ))
                    transcript.remember(response_id, merged_input, [])
                    logger.debug(f"{_tag('WS', path, provider, str(payload.get('model') or ''), client_ip)} SSE2WS warmup完成")
                    continue

                body = json.dumps(sanitized, ensure_ascii=False,
                                  separators=(",", ":")).encode("utf-8")
                model = parse_request_model(body, remaining)
                session_id = parse_request_session_id(body)
                model_scope = classify_model_scope(model, "responses")
                outbound_headers = outbound_request_headers(websocket.headers, remaining, model)
                outbound_headers["content-type"] = "application/json"
                outbound_headers["accept"] = "text/event-stream"
                if settings.proxy_api_key and pool_credential_ok and base_pool is None:
                    await _send_error(websocket, BridgeError(
                        "Key pool is unavailable for this upstream", status=503,
                        code="key_pool_unavailable",
                    ))
                    continue
                pool_access = bool(base_pool and pool_credential_ok)
                request_pool = base_pool.for_request(
                    model, remaining, "responses", model_scope,
                ) if pool_access else None
                if pool_access and request_pool is None:
                    await _send_error(websocket, BridgeError(
                        "No compatible key pool route for this endpoint and model",
                        status=403, code="key_pool_no_compatible_route",
                    ))
                    continue

                metrics = TurnMetrics()
                ip_token = set_client_ip(client_ip)
                try:
                    request_args = (
                        "POST", url, outbound_headers, body, path, provider, model,
                        request_pool, session_id,
                    )
                    opened = await _open_with_retries(
                        service, request_args, request_pool, session_id, metrics,
                        websocket,
                    )
                    status, response_id, output_items, sent = await _relay(websocket, opened)
                    succeeded = status == "response.completed"
                    await _write_turn_log(
                        store, path, provider, model, upstream, request_pool, client_ip,
                        metrics, 200, status, succeeded,
                    )
                    if status == "response.completed":
                        transcript.remember(response_id, merged_input, output_items)
                    if status == "cancelled":
                        logger.info(f"{_tag('WS', path, provider, model, client_ip)} SSE2WS客户端取消")
                except ClientDisconnected:
                    await _write_turn_log(
                        store, path, provider, model, upstream, request_pool, client_ip,
                        metrics, 499, "cancelled", False,
                    )
                    return
                except TurnCancelled:
                    await _write_turn_log(
                        store, path, provider, model, upstream, request_pool, client_ip,
                        metrics, 499, "cancelled", False,
                    )
                    logger.info(
                        f"{_tag('WS', path, provider, model, client_ip)} "
                        "SSE2WS首事件前客户端取消"
                    )
                    continue
                except BridgeError as exc:
                    if (not exc.key_failure_recorded
                            and not getattr(exc, "key_outcome_recorded", False)
                            and _is_key_failure_status(exc.status)):
                        _mark_deferred_failure(
                            request_pool, metrics.key_entry, session_id, metrics, exc.status,
                        )
                    await _write_turn_log(
                        store, path, provider, model, upstream, request_pool, client_ip,
                        metrics, exc.status, exc.code, False,
                    )
                    try:
                        await _send_error(websocket, exc)
                        await websocket.close(code=1011, reason=exc.code[:123])
                    except (RuntimeError, WebSocketDisconnect):
                        pass
                    return
                finally:
                    reset_client_ip(ip_token)
            except WebSocketDisconnect:
                return

    return websocket_proxy
