"""配置中心 API 测试：GET 全量/掩码、POST 校验/热应用/持久化"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request

from retry_proxy.application import settings_get, settings_post
from retry_proxy.config import settings
from retry_proxy.settings_meta import CONFIG_ITEMS


def _json_request(payload: dict) -> Request:
    body = json.dumps(payload).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({
        "type": "http", "method": "POST", "path": "/admin/settings",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"", "server": ("test", 80),
    }, receive=receive)


class SettingsGetTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_all_meta_items_with_groups(self):
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        self.assertEqual(len(data["items"]), len(CONFIG_ITEMS))
        self.assertIn("服务与访问控制", data["groups"])
        self.assertFalse(data["persisted"])

    async def test_secret_items_are_masked(self):
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        for item in data["items"]:
            if item["key"] == "ADMIN_PASSWORD":
                self.assertTrue(item["secret"])
                self.assertNotIn("effective_value", item)
                self.assertIn("configured", item)
            if not item["secret"]:
                self.assertIn("effective_value", item)

    async def test_secret_items_do_not_leak_file_value(self):
        # .env 中存在敏感键明文时，GET 也不得回传（文档承诺敏感项不回显明文）
        tmpdir = tempfile.mkdtemp()
        env_path = os.path.join(tmpdir, ".env")
        try:
            with open(env_path, "w", encoding="utf-8") as f:
                f.write("ADMIN_PASSWORD=super-secret-pass\nKEY_POOLS=sk-1234567890abcdef\n"
                        "RETRY_INTERVAL=2.5\n")
            with patch("retry_proxy.application._ENV_FILE_PATH", env_path):
                data = await settings_get()
        finally:
            shutil.rmtree(tmpdir)
        by_key = {i["key"]: i for i in data["items"]}
        for key in ("ADMIN_PASSWORD", "ADMIN_TOKEN", "PROXY_API_KEY",
                    "KEY_POOLS", "KEY_POOL_SYNC_SECRET"):
            self.assertNotIn("file_value", by_key[key], f"{key} 不应回传明文")
            self.assertIn("configured", by_key[key], key)
        # 非敏感项仍回传 .env 文件值供输入框回填
        self.assertEqual(by_key["RETRY_INTERVAL"]["file_value"], "2.5")

    async def test_effective_value_comes_from_settings(self):
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        self.assertEqual(by_key["RETRY_INTERVAL"]["effective_value"], str(settings.retry_interval))
        self.assertEqual(by_key["UPSTREAM_URL"]["effective_value"], settings.upstream_url)

    async def test_items_carry_chinese_names(self):
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        self.assertEqual(by_key["TZ"]["name"], "容器时区")
        self.assertEqual(by_key["DOCKER_REGISTRY"]["name"], "Docker 仓库域名")
        for item in data["items"]:
            self.assertTrue(item["name"], item["key"])

    async def test_effective_value_falls_back_to_meta_default(self):
        # Settings 无属性的键（TZ 等）未配置时，生效值按元数据默认值显示而非空
        with patch.dict(os.environ, {}, clear=False), \
             patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            os.environ.pop("TZ", None)
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        self.assertEqual(by_key["TZ"]["effective_value"], "Asia/Shanghai")

    async def test_build_only_items_are_editable_cards(self):
        # 构建期配置渲染为可编辑卡片，hidden 应为 false
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        for key in ("DOCKER_REGISTRY", "PYTHON_BASE_IMAGE", "PIP_INDEX_URL"):
            self.assertFalse(by_key[key]["hidden"], key)

    async def test_retry_status_codes_serialized_as_csv(self):
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        self.assertEqual(by_key["RETRY_STATUS_CODES"]["effective_value"], ",".join(
            str(c) for c in sorted(settings.retry_status_codes)))

    async def test_time_items_carry_unit(self):
        # 时间类配置项下发单位供输入框展示；非时间项为空
        with patch("retry_proxy.application._ENV_FILE_PATH", "/nonexistent/.env"):
            data = await settings_get()
        by_key = {i["key"]: i for i in data["items"]}
        self.assertEqual(by_key["RETRY_INTERVAL"]["unit"], "秒")
        self.assertEqual(by_key["TIMEOUT"]["unit"], "秒")
        self.assertEqual(by_key["LOG_RETENTION_DAYS"]["unit"], "天")
        self.assertEqual(by_key["MAX_REQUEST_BODY"]["unit"], "")
        self.assertEqual(by_key["LISTEN_PORT"]["unit"], "")


class SettingsPostTests(unittest.IsolatedAsyncioTestCase):
    async def _post(self, payload):
        return await settings_post(_json_request(payload))

    async def test_hot_item_applies_immediately(self):
        original = settings.retry_interval
        try:
            with patch("retry_proxy.application.update_env_file", return_value=True):
                result = await self._post({"updates": {"RETRY_INTERVAL": "2.5"}})
            self.assertIn("RETRY_INTERVAL", result["applied"])
            self.assertEqual(result["need_restart"], [])
            self.assertTrue(result["persisted"])
            self.assertEqual(settings.retry_interval, 2.5)
        finally:
            settings.retry_interval = original

    async def test_restart_item_is_not_applied_hot(self):
        original = settings.listen_port
        try:
            with patch("retry_proxy.application.update_env_file", return_value=True):
                result = await self._post({"updates": {"LISTEN_PORT": "9090"}})
            self.assertEqual(result["applied"], [])
            self.assertEqual(result["need_restart"], ["LISTEN_PORT"])
            self.assertEqual(settings.listen_port, original)
        finally:
            settings.listen_port = original

    async def test_empty_value_means_remove(self):
        with patch("retry_proxy.application.update_env_file", return_value=True) as writer:
            result = await self._post({"updates": {"RETRY_BACKOFF": ""}})
        self.assertIn("RETRY_BACKOFF", result["removed"])
        self.assertEqual(result["applied"], ["RETRY_BACKOFF"])
        writer.assert_called_once()
        # 写文件时该键应出现在移除列表而非写入列表
        _, updates_arg, removes_arg = writer.call_args.args
        self.assertNotIn("RETRY_BACKOFF", updates_arg)
        self.assertIn("RETRY_BACKOFF", removes_arg)

    async def test_remove_array_applies_hot_default(self):
        # 页面"重置"路径：空值走 remove 数组，HOT 项必须立即恢复默认值
        original = settings.retry_interval
        try:
            settings.retry_interval = 9.9
            with patch("retry_proxy.application.update_env_file", return_value=True) as writer:
                result = await self._post({"updates": {}, "remove": ["RETRY_INTERVAL"]})
            self.assertIn("RETRY_INTERVAL", result["applied"])
            self.assertEqual(settings.retry_interval, 1.0)
            _, updates_arg, removes_arg = writer.call_args.args
            self.assertNotIn("RETRY_INTERVAL", updates_arg)
            self.assertIn("RETRY_INTERVAL", removes_arg)
        finally:
            settings.retry_interval = original

    async def test_remove_array_restart_item_needs_restart(self):
        # 页面"重置"一个重启生效项：不热应用，但提示需重启
        with patch("retry_proxy.application.update_env_file", return_value=True):
            result = await self._post({"updates": {}, "remove": ["LISTEN_PORT"]})
        self.assertEqual(result["applied"], [])
        self.assertEqual(result["need_restart"], ["LISTEN_PORT"])

    async def test_remove_array_unknown_key_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {}, "remove": ["NOT_A_KEY"]})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_remove_array_rebuild_key_allowed(self):
        # 构建期配置可重置：从 .env 删除该键，标记需重建镜像后生效
        with patch("retry_proxy.application.update_env_file", return_value=True):
            result = await self._post({"updates": {}, "remove": ["DOCKER_REGISTRY"]})
        self.assertEqual(result["need_rebuild"], ["DOCKER_REGISTRY"])
        self.assertIn("DOCKER_REGISTRY", result["removed"])

    async def test_update_wins_over_remove_for_same_key(self):
        # 同一键同时出现在 updates 与 remove 时以新值为准
        original = settings.retry_interval
        try:
            with patch("retry_proxy.application.update_env_file", return_value=True) as writer:
                result = await self._post({"updates": {"RETRY_INTERVAL": "3.3"},
                                           "remove": ["RETRY_INTERVAL"]})
            self.assertEqual(result["applied"], ["RETRY_INTERVAL"])
            self.assertEqual(settings.retry_interval, 3.3)
            # 写文件时以写入新值为主，文件行不会被删除
            _, updates_arg, _ = writer.call_args.args
            self.assertIn("RETRY_INTERVAL", updates_arg)
        finally:
            settings.retry_interval = original

    async def test_invalid_csv_rejected(self):
        # csv 类型需校验元素（RETRY_STATUS_CODES 必须为整数），非法值返回 400 而非 500
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"RETRY_STATUS_CODES": "503,abc"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_valid_csv_accepted(self):
        original = settings.retry_status_codes
        try:
            with patch("retry_proxy.application.update_env_file", return_value=True):
                result = await self._post({"updates": {"RETRY_STATUS_CODES": "503,504"}})
            self.assertIn("RETRY_STATUS_CODES", result["applied"])
            self.assertEqual(settings.retry_status_codes, frozenset({503, 504}))
        finally:
            settings.retry_status_codes = original

    async def test_save_log_redacts_secret_values(self):
        # 保存日志不得包含敏感配置明文（KEY_POOLS 等 secret 项以掩码输出）
        with patch("retry_proxy.application.update_env_file", return_value=True), \
             patch("retry_proxy.application.logger") as mock_logger:
            await self._post({"updates": {"PROXY_API_KEY": "sk-very-secret"}})
        logged = " ".join(str(call) for call in mock_logger.info.call_args_list)
        self.assertIn("***", logged)
        self.assertNotIn("sk-very-secret", logged)

    async def test_unknown_key_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"NOT_A_KEY": "1"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_rebuild_key_allowed(self):
        # 构建期配置可编辑：写入 .env 并标记需重建镜像后生效，不再 400 拒绝
        with patch("retry_proxy.application.update_env_file", return_value=True) as writer:
            result = await self._post({"updates": {"DOCKER_REGISTRY": "registry.example.com"}})
        self.assertEqual(result["need_rebuild"], ["DOCKER_REGISTRY"])
        _, updates_arg, _ = writer.call_args.args
        self.assertEqual(updates_arg["DOCKER_REGISTRY"], "registry.example.com")

    async def test_invalid_int_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"MAX_RETRIES": "abc"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_invalid_float_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"RETRY_INTERVAL": "abc"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_trailing_backslash_rejected(self):
        # 尾部反斜杠触发 python-dotenv 1.2.2 解析 bug（整个文件被丢弃），必须拒绝
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"LOG_DIR": "C:\\temp\\"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_newline_rejected(self):
        # 换行会破坏 .env 单行结构，写入后无法被行式解析还原
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"LOG_DIR": "a\nb"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_csv_trailing_backslash_rejected(self):
        # csv 分支同样不得绕过尾部反斜杠校验，否则写出的引号值会触发 python-dotenv 整文件丢弃
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"DLP_RULES": "rule1\\"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_csv_newline_rejected(self):
        # int() 容忍首尾空白，换行可能混入 csv 值，必须同样拦截
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"RETRY_STATUS_CODES": "503,\n502"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_unicode_line_separator_rejected(self):
        # \u2028 等通用换行符同样被 splitlines 视为行分隔，需拦截
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"LOG_DIR": "a\u2028b"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_invalid_enum_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"HEDGE_MODE": "hyper"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_invalid_bool_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self._post({"updates": {"RETRY_BACKOFF": "maybe"}})
        self.assertEqual(raised.exception.status_code, 400)

    async def test_no_changes_skips_file_write(self):
        with patch("retry_proxy.application.update_env_file") as writer:
            result = await self._post({"updates": {}})
        writer.assert_not_called()
        self.assertFalse(result["persisted"])


class SettingsPageToggleTests(unittest.IsolatedAsyncioTestCase):
    async def test_nav_link_hidden_when_disabled(self):
        # SETTINGS_PAGE_ENABLED 关闭时三页导航不含配置入口
        from retry_proxy.api import create_handlers
        from retry_proxy.application import key_pools_page

        handlers = create_handlers(None, None, None)
        pages = [handlers[1], handlers[3], key_pools_page]  # stats_page, logs_page, key_pools_page
        for page in pages:
            with patch.object(settings, "settings_page_enabled", False):
                res = await page()
                body = getattr(res, "body", b"")
                text = body.decode("utf-8") if isinstance(body, bytes) else str(body)
                self.assertNotIn('<a href="/settings">配置</a>', text)

    async def test_nav_link_present_when_enabled(self):
        # SETTINGS_PAGE_ENABLED 开启时三页导航保留配置入口
        from retry_proxy.api import create_handlers
        from retry_proxy.application import key_pools_page

        handlers = create_handlers(None, None, None)
        pages = [handlers[1], handlers[3], key_pools_page]
        for page in pages:
            with patch.object(settings, "settings_page_enabled", True):
                res = await page()
                body = getattr(res, "body", b"")
                text = body.decode("utf-8") if isinstance(body, bytes) else str(body)
                self.assertIn('<a href="/settings">配置</a>', text)


if __name__ == "__main__":
    unittest.main()
