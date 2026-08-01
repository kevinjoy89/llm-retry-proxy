import unittest
from types import SimpleNamespace

from starlette.requests import Request

from retry_proxy.api import _request_diagnostic, _request_ip, outbound_request_headers


class OutboundRequestHeadersTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(
            image_upstream_user_agent="codex_cli_rs/0.114.0",
            image_upstream_originator="codex_cli_rs",
        )

    def test_image_request_overrides_client_identity(self):
        headers = outbound_request_headers(
            {"user-agent": "Python-urllib/3.12", "content-type": "application/json"},
            "images/generations", "gpt-image-2", self.config,
        )

        self.assertEqual(headers["user-agent"], "codex_cli_rs/0.114.0")
        self.assertEqual(headers["originator"], "codex_cli_rs")

    def test_text_request_preserves_client_identity(self):
        headers = outbound_request_headers(
            {"user-agent": "client/1.0", "accept-encoding": "gzip, br, zstd"},
            "responses", "gpt-5.6", self.config,
        )

        self.assertEqual(headers["user-agent"], "client/1.0")
        self.assertEqual(headers["accept-encoding"], "gzip, deflate")
        self.assertNotIn("originator", headers)

    def test_downstream_proxy_identity_is_not_forwarded_upstream(self):
        headers = outbound_request_headers({
            "authorization": "Bearer downstream-token",
            "cf-connecting-ip": "203.0.113.7",
            "cf-ipcountry": "CN",
            "cf-ray": "test-ray",
            "cdn-loop": "cloudflare; loops=1",
            "forwarded": "for=203.0.113.7;proto=https",
            "remote-host": "client.example.com",
            "true-client-ip": "203.0.113.7",
            "x-client-ip": "203.0.113.7",
            "x-forwarded-for": "203.0.113.7, 198.51.100.20",
            "x-forwarded-host": "proxy.example.com",
            "x-forwarded-proto": "https",
            "x-original-forwarded-for": "203.0.113.7",
            "x-real-ip": "203.0.113.7",
        }, "responses", "gpt-5.6", self.config)

        self.assertEqual(headers["authorization"], "Bearer downstream-token")
        self.assertEqual(headers["accept-encoding"], "gzip, deflate")
        for name in (
            "cf-connecting-ip", "cf-ipcountry", "cf-ray", "cdn-loop",
            "forwarded", "remote-host", "true-client-ip", "x-client-ip",
            "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
            "x-original-forwarded-for", "x-real-ip",
        ):
            self.assertNotIn(name, headers)


class RequestIpTests(unittest.TestCase):
    @staticmethod
    def _request(headers, direct="127.0.0.1"):
        return Request({
            "type": "http", "method": "GET", "path": "/",
            "headers": [(name.encode(), value.encode()) for name, value in headers.items()],
            "query_string": b"", "server": ("test", 80),
            "client": (direct, 1234),
        })

    def test_forwarded_for_uses_first_address(self):
        request = self._request({
            "x-forwarded-for": "203.0.113.7, 198.51.100.20",
            "x-real-ip": "192.0.2.10",
        }, "172.20.0.3")

        self.assertEqual(_request_ip(request), "203.0.113.7")

    def test_cf_connecting_ip_takes_priority(self):
        request = self._request({
            "cf-connecting-ip": "198.51.100.8",
            "x-forwarded-for": "203.0.113.7",
            "x-real-ip": "192.0.2.10",
        }, "172.20.0.3")

        self.assertEqual(_request_ip(request), "198.51.100.8")

    def test_direct_ip_is_fallback(self):
        request = self._request({}, "172.20.0.3")

        self.assertEqual(_request_ip(request), "172.20.0.3")


class RequestDiagnosticTests(unittest.TestCase):
    def test_reports_shape_and_protocol_headers_without_secret_values(self):
        body = (
            b'{"model":"gpt-test","input":[{"role":"user","content":"secret prompt"}],'
            b'"tools":[{}],"instructions":"private instructions","stream":true}'
        )
        summary = _request_diagnostic(
            body,
            {
                "authorization": "Bearer downstream-secret",
                "cookie": "session=private-cookie",
                "cf-connecting-ip": "203.0.113.7",
                "user-agent": "codex_cli_rs/0.146.0",
            },
            {
                "authorization": "Bearer upstream-secret",
                "content-type": "application/json",
                "user-agent": "codex_cli_rs/0.146.0",
                "openai-beta": "feature_x=v1",
            },
            "http",
        )

        self.assertIn("transport=http", summary)
        self.assertIn("input_items=1", summary)
        self.assertIn("tools=1", summary)
        self.assertIn("client_metadata_fields=-", summary)
        self.assertIn(
            "inbound_headers=authorization,cf-connecting-ip,cookie,user-agent",
            summary,
        )
        self.assertIn("openai-beta:feature_x", summary)
        self.assertIn("user-agent:codex_cli_rs/0.146.0", summary)
        for secret in (
            "secret prompt", "private instructions", "downstream-secret",
            "private-cookie", "upstream-secret", "2026-02-06", "v1",
        ):
            self.assertNotIn(secret, summary)


if __name__ == "__main__":
    unittest.main()
