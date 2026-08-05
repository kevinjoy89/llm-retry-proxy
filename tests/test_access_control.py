import ipaddress
import tempfile
import unittest
from unittest.mock import patch

from retry_proxy.access_control import (
    IPBlocklistMiddleware,
    ip_in_networks,
    parse_ip_networks,
    resolve_client_ip,
)


def _scope(client, headers=None, scope_type="http", path="/health"):
    scope = {
        "type": scope_type,
        "path": path,
        "client": (client, 1234),
        "headers": [
            (name.encode("latin-1"), value.encode("latin-1"))
            for name, value in (headers or {}).items()
        ],
    }
    if scope_type == "http":
        scope.update({"method": "GET", "query_string": b""})
    return scope


class IPNetworkTests(unittest.TestCase):
    def test_parses_addresses_and_cidr_ranges(self):
        networks = parse_ip_networks(
            "152.32.129.213, 198.51.100.0/24;2001:db8::/32", "IP_BLACKLIST",
        )

        self.assertEqual(len(networks), 3)
        self.assertTrue(ip_in_networks(
            ipaddress.ip_address("198.51.100.25"), networks,
        ))

    def test_rejects_invalid_network(self):
        with self.assertRaisesRegex(ValueError, "IP_BLACKLIST"):
            parse_ip_networks("not-an-ip", "IP_BLACKLIST")

    def test_direct_client_cannot_spoof_forwarded_ip(self):
        scope = _scope("152.32.129.213", {
            "x-forwarded-for": "203.0.113.10",
        })

        self.assertEqual(resolve_client_ip(scope), "152.32.129.213")

    def test_trusted_proxy_uses_first_untrusted_ip_from_right(self):
        trusted = parse_ip_networks("172.20.0.0/16,10.0.0.0/8", "TRUSTED_PROXY_IPS")
        scope = _scope("172.20.0.3", {
            "x-forwarded-for": "192.0.2.99, 203.0.113.7, 10.0.0.2",
        })

        self.assertEqual(resolve_client_ip(scope, trusted), "203.0.113.7")


class IPBlocklistMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def test_blacklisted_http_request_is_rejected_before_application(self):
        called = False

        async def application(scope, receive, send):
            nonlocal called
            called = True

        middleware = IPBlocklistMiddleware(
            application,
            blacklist=parse_ip_networks("152.32.129.213", "IP_BLACKLIST"),
        )
        messages = []

        async def send(message):
            messages.append(message)

        await middleware(_scope("152.32.129.213"), None, send)

        self.assertFalse(called)
        self.assertEqual(messages[0]["status"], 403)

    async def test_static_blacklist_rejects_without_request_log(self):
        async def application(scope, receive, send):
            self.fail("blacklisted request reached application")

        middleware = IPBlocklistMiddleware(
            application,
            blacklist=parse_ip_networks("152.32.129.213", "IP_BLACKLIST"),
        )
        async def send(message):
            pass

        with patch("retry_proxy.access_control.logger.warning") as warning:
            await middleware(_scope("152.32.129.213"), None, send)

        warning.assert_not_called()

    async def test_allowed_request_reaches_application(self):
        called = False

        async def application(scope, receive, send):
            nonlocal called
            called = True

        middleware = IPBlocklistMiddleware(
            application,
            blacklist=parse_ip_networks("198.51.100.0/24", "IP_BLACKLIST"),
        )

        await middleware(_scope("203.0.113.7"), None, None)

        self.assertTrue(called)

    async def test_distinct_path_burst_creates_silent_dynamic_ban(self):
        calls = 0
        now = [1000.0]

        async def application(scope, receive, send):
            nonlocal calls
            calls += 1

        middleware = IPBlocklistMiddleware(
            application,
            auto_ban_threshold=3,
            auto_ban_window=10,
            auto_ban_duration=100,
            clock=lambda: now[0],
        )

        async def send(message):
            pass

        with self.assertLogs("forward", level="WARNING") as records:
            for index in range(3):
                await middleware(
                    _scope("203.0.113.7", path=f"/scan/{index}"), None, send,
                )
            await middleware(_scope("203.0.113.7", path="/after-ban"), None, send)

        self.assertEqual(calls, 2)
        self.assertEqual(len(records.records), 1)
        self.assertNotIn("/scan/", records.records[0].getMessage())

    async def test_repeated_path_does_not_trigger_dynamic_ban(self):
        calls = 0

        async def application(scope, receive, send):
            nonlocal calls
            calls += 1

        middleware = IPBlocklistMiddleware(
            application, auto_ban_threshold=3, auto_ban_window=10,
        )

        for _ in range(4):
            await middleware(_scope("203.0.113.8", path="/same"), None, None)

        self.assertEqual(calls, 4)

    async def test_permanent_dynamic_ban_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = f"{directory}/ip_bans.json"
            async def application(scope, receive, send):
                self.fail("persisted dynamic ban reached application")

            first = IPBlocklistMiddleware(
                application, auto_ban_threshold=1, auto_ban_duration=0,
                state_file=state_file, clock=lambda: 1000.0,
            )
            async def first_send(message):
                pass

            await first(_scope("203.0.113.9", path="/scan"), None, first_send)

            second = IPBlocklistMiddleware(
                application, auto_ban_threshold=1, auto_ban_duration=0,
                state_file=state_file, clock=lambda: 999999999.0,
            )
            messages = []

            async def send(message):
                messages.append(message)

            await second(_scope("203.0.113.9", path="/other"), None, send)

            self.assertEqual(messages[0]["status"], 403)

    async def test_positive_dynamic_ban_duration_expires(self):
        calls = 0
        now = [1000.0]

        async def application(scope, receive, send):
            nonlocal calls
            calls += 1

        middleware = IPBlocklistMiddleware(
            application, auto_ban_threshold=2, auto_ban_window=10,
            auto_ban_duration=10, clock=lambda: now[0],
        )

        async def send(message):
            pass

        await middleware(_scope("203.0.113.10", path="/one"), None, send)
        await middleware(_scope("203.0.113.10", path="/two"), None, send)
        now[0] = 1011.0
        await middleware(_scope("203.0.113.10", path="/three"), None, send)

        self.assertEqual(calls, 2)

    async def test_exempt_client_is_not_dynamically_banned(self):
        calls = 0

        async def application(scope, receive, send):
            nonlocal calls
            calls += 1

        middleware = IPBlocklistMiddleware(
            application, auto_ban_threshold=1,
            auto_ban_exempt=parse_ip_networks("192.0.2.0/24", "IP_AUTO_BAN_EXEMPT"),
        )

        await middleware(_scope("192.0.2.7", path="/scan"), None, None)

        self.assertEqual(calls, 1)

    async def test_blacklisted_forwarded_client_is_rejected_from_trusted_proxy(self):
        async def application(scope, receive, send):
            self.fail("blacklisted forwarded client reached application")

        middleware = IPBlocklistMiddleware(
            application,
            blacklist=parse_ip_networks("152.32.129.213", "IP_BLACKLIST"),
            trusted_proxies=parse_ip_networks("172.20.0.0/16", "TRUSTED_PROXY_IPS"),
        )
        messages = []

        async def send(message):
            messages.append(message)

        scope = _scope("172.20.0.3", {
            "x-forwarded-for": "152.32.129.213",
        })
        await middleware(scope, None, send)

        self.assertEqual(messages[0]["status"], 403)

    async def test_blacklisted_websocket_is_closed_before_accept(self):
        async def application(scope, receive, send):
            self.fail("blacklisted websocket reached application")

        middleware = IPBlocklistMiddleware(
            application,
            blacklist=parse_ip_networks("2001:db8::/32", "IP_BLACKLIST"),
        )
        messages = []

        async def send(message):
            messages.append(message)

        await middleware(_scope("2001:db8::42", scope_type="websocket"), None, send)

        self.assertEqual(messages, [{
            "type": "websocket.close", "code": 1008, "reason": "IP blocked",
        }])


if __name__ == "__main__":
    unittest.main()
