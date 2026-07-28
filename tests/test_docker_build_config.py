import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = REPO_ROOT / "Dockerfile"
ENV_EXAMPLE = REPO_ROOT / ".env.example"

# 两来源拼接的预期完整镜像引用：Dockerfile 兜底默认走 Docker Hub，
# .env.example 预置国内镜像站，便于无代理构建。
EXPECTED_DOCKERFILE_REFERENCE = "docker.io/library/python:3.12-slim"
EXPECTED_ENV_REFERENCE = "docker.m.daocloud.io/library/python:3.12-slim"


def _arg_defaults(text):
    """提取 Dockerfile 中 `ARG NAME=VALUE` 的默认值映射。"""
    defaults = {}
    for m in re.finditer(r"^ARG\s+(\w+)=(\S+)\s*$", text, re.MULTILINE):
        defaults[m.group(1)] = m.group(2)
    return defaults


def _env_values(text):
    """提取 .env.example 中 `NAME=VALUE` 赋值（跳过注释与空行）。"""
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([A-Z_]+)=(\S+)$", line)
        if m:
            values[m.group(1)] = m.group(2)
    return values


def _is_valid_reference(ref):
    """粗略校验拼接结果是合法 Docker 镜像引用：无 scheme、无连续斜杠、含域名/路径/tag。"""
    if "://" in ref or "//" in ref:
        return False
    return re.match(r"^[^/\s]+(?::\d+)?(?:/[^/\s]+)+(?::[^\s/]+)?$", ref) is not None


class DockerBuildConfigTests(unittest.TestCase):
    def setUp(self):
        self.arg_defaults = _arg_defaults(DOCKERFILE.read_text())
        self.env = _env_values(ENV_EXAMPLE.read_text())

    def test_dockerfile_defaults_produce_valid_reference(self):
        ref = f"{self.arg_defaults['DOCKER_REGISTRY']}/{self.arg_defaults['PYTHON_BASE_IMAGE']}"
        self.assertTrue(_is_valid_reference(ref), f"非法镜像引用: {ref}")
        self.assertEqual(ref, EXPECTED_DOCKERFILE_REFERENCE)

    def test_env_example_produces_expected_reference(self):
        ref = f"{self.env['DOCKER_REGISTRY']}/{self.env['PYTHON_BASE_IMAGE']}"
        self.assertTrue(_is_valid_reference(ref), f"非法镜像引用: {ref}")
        self.assertEqual(ref, EXPECTED_ENV_REFERENCE)

    def test_docker_registry_is_bare_domain(self):
        # DOCKER_REGISTRY 必须是纯域名，不含路径分隔符；library 命名空间误放入此
        # 处会破坏换站时的复用，且与 PYTHON_BASE_IMAGE 的命名空间职责混淆。
        for source, values in (("arg_defaults", self.arg_defaults), ("env", self.env)):
            value = values["DOCKER_REGISTRY"]
            self.assertNotIn("/", value, f"DOCKER_REGISTRY({source}) 应为纯域名: {value}")

    def test_python_base_image_includes_namespace(self):
        # PYTHON_BASE_IMAGE 必须含命名空间段（如 library/），否则与纯域名拼接会缺命名空间。
        for source, values in (("arg_defaults", self.arg_defaults), ("env", self.env)):
            value = values["PYTHON_BASE_IMAGE"]
            self.assertIn("/", value, f"PYTHON_BASE_IMAGE({source}) 应含命名空间: {value}")

    def test_image_values_carry_no_scheme(self):
        # Docker 镜像引用语法不支持 http(s):// 前缀，Docker 默认以 HTTPS 拉取。
        for values in (self.arg_defaults, self.env):
            for key in ("DOCKER_REGISTRY", "PYTHON_BASE_IMAGE"):
                self.assertNotIn("://", values[key], f"{key} 镜像引用不支持 scheme: {values[key]}")


if __name__ == "__main__":
    unittest.main()
