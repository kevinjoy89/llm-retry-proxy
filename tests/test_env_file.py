"""配置中心 .env 文件读写模块测试"""

import os
import tempfile
import unittest
from unittest.mock import patch

from retry_proxy.env_file import load_env_file, update_env_file

SAMPLE = """# ============ 注释头 ============

# 服务
LISTEN_PORT=8080

# 空值键
ADMIN_PASSWORD=

# 带引号与特殊字符
DLP_EXEMPT_START="[[ALLOW_SENSITIVE]]"
UPSTREAM_URL=https://example.com/v2
""" .lstrip()


class EnvFileTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, ".env")

    def tearDown(self):
        self.dir.cleanup()

    def _write_sample(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(SAMPLE)

    def test_load_parses_active_keys_only(self):
        self._write_sample()
        data = load_env_file(self.path)
        self.assertEqual(data["LISTEN_PORT"], "8080")
        self.assertEqual(data["ADMIN_PASSWORD"], "")
        self.assertEqual(data["DLP_EXEMPT_START"], "[[ALLOW_SENSITIVE]]")
        self.assertEqual(data["UPSTREAM_URL"], "https://example.com/v2")
        self.assertNotIn("注释头", data)

    def test_load_missing_file_returns_empty(self):
        self.assertEqual(load_env_file(os.path.join(self.dir.name, "nope")), {})

    def test_load_strips_inline_comments(self):
        # 行尾注释不进入值（与启动加载 python-dotenv 的语义一致）
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("KEY=value # 行尾注释\n"
                    'KEY2="a # b"\n'
                    "KEY3=value#nospace\n"
                    'KEY4="abc" # 带引号值后接注释\n')
        data = load_env_file(self.path)
        self.assertEqual(data["KEY"], "value")
        self.assertEqual(data["KEY2"], "a # b")
        self.assertEqual(data["KEY3"], "value#nospace")
        self.assertEqual(data["KEY4"], "abc")

    def test_load_strips_comments_after_quoted_values(self):
        # 引号内含 " # " 时，闭合引号后的注释仍须剥离（与 python-dotenv 语义一致）
        with open(self.path, "w", encoding="utf-8") as f:
            f.write('KEY="abc # def" # 行尾注释\n'
                    "KEY2='a # b' # c\n"
                    'KEY3="a # b"#c\n')
        data = load_env_file(self.path)
        self.assertEqual(data["KEY"], "abc # def")
        self.assertEqual(data["KEY2"], "a # b")
        self.assertEqual(data["KEY3"], "a # b")

    def test_update_replaces_inline_comment_of_edited_key(self):
        # 编辑带行尾注释的键时整行替换，注释随旧值丢弃（行为锁定，防止误改）
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("KEY=value # 行尾注释\nOTHER=1\n")
        update_env_file(self.path, {"KEY": "new"}, set())
        with open(self.path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[0], "KEY=new")
        data = load_env_file(self.path)
        self.assertEqual(data["KEY"], "new")

    def test_load_decodes_quoted_escapes(self):
        # 引号内转义按 python-dotenv 规则解码，保证展示值与运行时加载值一致
        with open(self.path, "w", encoding="utf-8") as f:
            f.write('KEY="a\\"b"\n'
                    'KEY2="C:\\\\temp\\\\logs"\n'
                    "KEY3='a\\\\b'\n"
                    "KEY4='a\\'b'\n"
                    'KEY5="a\\\'b"\n')
        data = load_env_file(self.path)
        self.assertEqual(data["KEY"], 'a"b')
        self.assertEqual(data["KEY2"], "C:\\temp\\logs")
        self.assertEqual(data["KEY3"], "a\\b")
        self.assertEqual(data["KEY4"], "a'b")
        self.assertEqual(data["KEY5"], "a'b")

    def test_update_round_trips_quotes_and_backslashes(self):
        # 含双引号/反斜杠的值写回时转义，重载后还原（与 python-dotenv 解码对称）
        self._write_sample()
        update_env_file(self.path, {"UPSTREAM_URL": 'https://x/y"z', "LOG_DIR": "C:\\temp\\logs"}, set())
        with open(self.path, encoding="utf-8") as f:
            text = f.read()
        self.assertIn('UPSTREAM_URL="https://x/y\\"z"', text)
        self.assertIn('LOG_DIR="C:\\\\temp\\\\logs"', text)
        data = load_env_file(self.path)
        self.assertEqual(data["UPSTREAM_URL"], 'https://x/y"z')
        self.assertEqual(data["LOG_DIR"], "C:\\temp\\logs")

    def test_update_preserves_comments_and_order(self):
        self._write_sample()
        ok = update_env_file(self.path, {"LISTEN_PORT": "9090"}, set())
        self.assertTrue(ok)
        with open(self.path, encoding="utf-8") as f:
            text = f.read()
        # 注释与其它行保留
        self.assertIn("# ============ 注释头 ============", text)
        self.assertIn("# 服务", text)
        self.assertIn('DLP_EXEMPT_START="[[ALLOW_SENSITIVE]]"', text)
        # 键顺序不变：注释后仍先是 LISTEN_PORT
        self.assertLess(text.index("LISTEN_PORT=9090"), text.index("ADMIN_PASSWORD="))

    def test_update_quotes_special_values(self):
        self._write_sample()
        update_env_file(self.path, {"UPSTREAM_URL": "https://a.b/c#frag", "RETRY_CODES": "503, 504"}, set())
        data = load_env_file(self.path)
        self.assertEqual(data["UPSTREAM_URL"], "https://a.b/c#frag")
        self.assertEqual(data["RETRY_CODES"], "503, 504")

    def test_update_appends_new_keys_at_end(self):
        self._write_sample()
        update_env_file(self.path, {"NEW_KEY": "v1"}, set())
        with open(self.path, encoding="utf-8") as f:
            text = f.read()
        self.assertTrue(text.rstrip().endswith("NEW_KEY=v1"))

    def test_update_removes_keys(self):
        self._write_sample()
        update_env_file(self.path, {}, {"ADMIN_PASSWORD"})
        data = load_env_file(self.path)
        self.assertNotIn("ADMIN_PASSWORD", data)
        # 其余键保留
        self.assertIn("LISTEN_PORT", data)

    def test_update_missing_file_returns_false_without_creating(self):
        ok = update_env_file(self.path, {"A": "1"}, set())
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(self.path))

    def test_update_on_cross_device_mount_writes_directly(self):
        # 容器挂载宿主机 .env 时目标文件与父目录 st_dev 不同（bind mount），
        # os.replace 会替换挂载点切断绑定，必须改为直接写
        self._write_sample()
        real_stat = os.stat

        def fake_stat(p, *args, **kwargs):
            st = real_stat(p, *args, **kwargs)
            if os.path.abspath(str(p)) == os.path.abspath(self.path):
                values = list(st)
                values[2] += 1  # st_dev 改为与目录不同
                return os.stat_result(values)
            return st

        with patch("retry_proxy.env_file.os.stat", side_effect=fake_stat):
            ok = update_env_file(self.path, {"LISTEN_PORT": "9090"}, set())
        self.assertTrue(ok)
        data = load_env_file(self.path)
        self.assertEqual(data["LISTEN_PORT"], "9090")
        self.assertEqual(data["ADMIN_PASSWORD"], "")
        # 直接写路径不应残留临时文件
        self.assertEqual([p for p in os.listdir(self.dir.name) if p.startswith(".env.")], [])


if __name__ == "__main__":
    unittest.main()
