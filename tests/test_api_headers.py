import unittest
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import Request

from retry_proxy.api import _request_ip, outbound_request_headers


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


class RequestIpTests(unittest.TestCase):
    @staticmethod
    def _request(headers, direct="127.0.0.1"):
        return Request({
            "type": "http", "method": "GET", "path": "/",
            "headers": [(name.encode(), value.encode()) for name, value in headers.items()],
            "query_string": b"", "server": ("test", 80),
            "client": (direct, 1234),
        })

    def test_forwarded_chain_discards_attacker_prefix(self):
        request = self._request({
            "x-forwarded-for": "203.0.113.7, 198.51.100.20",
            "x-real-ip": "198.51.100.20",
        })
        config = SimpleNamespace(trusted_proxies=frozenset({"127.0.0.1"}))

        with patch("retry_proxy.api.settings", config):
            client_ip = _request_ip(request)

        self.assertEqual(client_ip, "198.51.100.20")

    def test_untrusted_peer_cannot_supply_forwarded_ip(self):
        request = self._request({"x-forwarded-for": "203.0.113.7"}, "198.51.100.20")
        config = SimpleNamespace(trusted_proxies=frozenset({"127.0.0.1"}))

        with patch("retry_proxy.api.settings", config):
            client_ip = _request_ip(request)

        self.assertEqual(client_ip, "198.51.100.20")

    def test_trusted_proxy_addresses_are_compared_in_canonical_form(self):
        request = self._request(
            {"x-forwarded-for": "2001:db8::20"}, "2001:0db8:0:0:0:0:0:1",
        )
        config = SimpleNamespace(trusted_proxies=frozenset({"2001:db8::1"}))

        with patch("retry_proxy.api.settings", config):
            client_ip = _request_ip(request)

        self.assertEqual(client_ip, "2001:db8::20")

    def test_docker_proxy_network_can_be_trusted_by_cidr(self):
        request = self._request(
            {"x-forwarded-for": "203.0.113.7, 172.20.0.2"}, "172.20.0.3",
        )
        config = SimpleNamespace(trusted_proxies=frozenset({"172.20.0.0/16"}))

        with patch("retry_proxy.api.settings", config):
            client_ip = _request_ip(request)

        self.assertEqual(client_ip, "203.0.113.7")

    def test_untrusted_docker_network_cannot_supply_forwarded_ip(self):
        request = self._request(
            {"x-forwarded-for": "203.0.113.7"}, "172.21.0.3",
        )
        config = SimpleNamespace(trusted_proxies=frozenset({"172.20.0.0/16"}))

        with patch("retry_proxy.api.settings", config):
            client_ip = _request_ip(request)

        self.assertEqual(client_ip, "172.21.0.3")


if __name__ == "__main__":
    unittest.main()
