"""Token usage extraction from upstream responses.

The proxy streams upstream responses straight through to the client, but it also
needs to record how many tokens each request consumed so the stats layer can
aggregate them. ``UsageAccumulator`` parses the response stream on the fly and
reports ``prompt_tokens`` / ``completion_tokens`` / ``total_tokens`` once the
stream finishes.

Extraction rules are dispatched by ``endpoint_family`` (see ``classify_endpoint``)
and whether the response is SSE:

- ``chat`` / ``embeddings``: OpenAI shape. Non-stream JSON carries top-level
  ``usage``; streaming carries it on the final ``data:`` frame (only when the
  upstream was asked to include usage).
- ``responses``: OpenAI Responses API. ``response.completed`` event embeds
  ``response.usage``.
- ``gemini``: ``usageMetadata`` on every chunk, with ``totalTokenCount`` filled
  on the final chunk; each chunk reports cumulative totals so the last seen
  value wins.
- ``messages``: Anthropic. ``message_start`` carries ``message.usage.input_tokens``
  and ``message_delta`` carries ``usage.output_tokens``.

When no usage can be parsed (failure, interruption, upstream that omits it) the
accumulator simply reports ``None`` so the record stays token-free and remains
backwards compatible with historical logs.
"""

import json


def _as_int(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


class UsageAccumulator:
    """Parse usage tokens from a streamed or buffered upstream response."""

    # Families whose usage we know how to extract. Anything else is ignored so
    # unknown endpoints simply produce token-less records.
    SUPPORTED = frozenset({"chat", "embeddings", "responses", "gemini", "messages"})

    def __init__(self, endpoint_family, is_sse, content_type=""):
        self.family = endpoint_family or ""
        self.is_sse = bool(is_sse)
        self.content_type = content_type or ""
        self.buffer = b""
        self.prompt = 0
        self.completion = 0
        self.total = 0
        # Cache-read tokens — the prompt portion served from a prompt cache.
        # Field shape differs per family and is extracted alongside usage:
        #   chat/embeddings: usage.prompt_tokens_details.cached_tokens
        #   responses:       response.usage.input_tokens_details.cached_tokens
        #   messages:        usage.cache_read_input_tokens
        #   gemini:          usageMetadata.cachedContentTokenCount
        self.cached = 0
        self.seen = False
        # Cap the buffered body so a pathological non-stream JSON response can
        # not hold unbounded memory. OpenAI/Anthropic place ``usage`` near the
        # *end* of the JSON body, so for large non-stream responses we keep only
        # the tail and let finalize() attempt a best-effort parse; bodies larger
        # than the cap may yield None (rare — completions are usually small).
        self._max_buffer = 1_048_576
        self._supported = self.family in self.SUPPORTED

    def feed_chunk(self, chunk):
        """Consume one ``aiter_bytes`` chunk from the upstream response."""
        if not self._supported or not chunk:
            return
        if self.is_sse:
            self._feed_sse(chunk)
        else:
            # Non-stream JSON: usage lives at the tail, so keep the most recent
            # _max_buffer bytes when the body overflows the cap.
            self.buffer += chunk
            if len(self.buffer) > self._max_buffer:
                self.buffer = self.buffer[-self._max_buffer:]

    def _feed_sse(self, chunk):
        """Split SSE frames and extract usage from each complete one."""
        self.buffer = (self.buffer + chunk).replace(b"\r\n", b"\n")
        while b"\n\n" in self.buffer:
            frame, self.buffer = self.buffer.split(b"\n\n", 1)
            self._handle_sse_frame(frame)
        if len(self.buffer) > self._max_buffer:
            # Keep the tail so a final partial frame can still complete later.
            self.buffer = self.buffer[-65_536:]

    def _handle_sse_frame(self, frame):
        data_lines = []
        event_name = ""
        for line in frame.splitlines():
            if line.startswith(b"event:"):
                event_name = line[6:].strip().decode("utf-8", errors="replace")
            elif line.startswith(b"data:"):
                data_lines.append(line[5:].strip())
        if not data_lines:
            return
        data = b"\n".join(data_lines)
        if data == b"[DONE]":
            return
        try:
            payload = json.loads(data)
        except (TypeError, ValueError, UnicodeDecodeError):
            return
        if not isinstance(payload, dict):
            return
        event_type = payload.get("type") or event_name
        self._extract(payload, event_type)

    def _extract(self, payload, event_type=""):
        family = self.family
        if family in ("chat", "embeddings"):
            # Streaming chat carries usage on the final frame's top-level object.
            usage = payload.get("usage")
            if isinstance(usage, dict):
                self._apply(
                    usage.get("prompt_tokens"),
                    usage.get("completion_tokens"),
                    usage.get("total_tokens"),
                )
                self._apply_cached(usage.get("prompt_tokens_details"))
        elif family == "responses":
            response = payload.get("response")
            if isinstance(response, dict):
                usage = response.get("usage")
                if isinstance(usage, dict):
                    self._apply(
                        usage.get("input_tokens"),
                        usage.get("output_tokens"),
                        usage.get("total_tokens"),
                    )
                    self._apply_cached(usage.get("input_tokens_details"))
        elif family == "gemini":
            meta = payload.get("usageMetadata")
            if isinstance(meta, dict):
                # Each chunk reports cumulative totals, so the last one wins.
                prompt = meta.get("promptTokenCount", self.prompt)
                completion = meta.get("candidatesTokenCount", meta.get("totalTokenCount"))
                total = meta.get("totalTokenCount", self.total)
                self._apply(prompt, completion, total)
                self._apply_cached(meta.get("cachedContentTokenCount"))
        elif family == "messages":
            if event_type == "message_start":
                message = payload.get("message")
                if isinstance(message, dict):
                    usage = message.get("usage")
                    if isinstance(usage, dict):
                        self._apply(
                            usage.get("input_tokens"),
                            usage.get("output_tokens"),
                            usage.get("total_tokens") or (self.prompt + self.completion),
                        )
                        self._apply_cached(usage.get("cache_read_input_tokens"))
            elif event_type == "message_delta":
                usage = payload.get("usage")
                if isinstance(usage, dict):
                    output = usage.get("output_tokens")
                    if output is not None:
                        self.completion = _as_int(output)
                        self.total = self.prompt + self.completion
                        self.seen = True

    def _apply_cached(self, cached):
        """Record cache-read tokens from either a *_details dict or a raw int.

        OpenAI nests the value under ``prompt_tokens_details``/``input_tokens_details``
        while Gemini/Anthropic expose it as a flat field, so callers pass whatever
        shape they found; a missing value leaves ``cached`` untouched.
        """
        if isinstance(cached, dict):
            cached = cached.get("cached_tokens")
        if cached is None:
            return
        self.cached = _as_int(cached)
        self.seen = True

    def _apply(self, prompt, completion, total):
        if prompt is not None:
            self.prompt = _as_int(prompt)
        if completion is not None:
            self.completion = _as_int(completion)
        if total is not None:
            self.total = _as_int(total)
        elif self.prompt or self.completion:
            self.total = self.prompt + self.completion
        if self.prompt or self.completion or self.total:
            self.seen = True

    def finalize(self):
        """Return ``(prompt, completion, total, cached)`` or ``None`` when no usage found."""
        if not self._supported:
            return None
        if not self.is_sse and self.buffer:
            # Non-stream JSON response: parse the buffered body in one shot.
            try:
                payload = json.loads(self.buffer)
            except (TypeError, ValueError, UnicodeDecodeError):
                payload = None
            if isinstance(payload, dict):
                self._extract(payload)
        if not self.seen:
            return None
        if not self.total and (self.prompt or self.completion):
            self.total = self.prompt + self.completion
        return self.prompt, self.completion, self.total, self.cached
