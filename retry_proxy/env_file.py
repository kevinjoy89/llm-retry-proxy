"""配置中心 .env 文件读写

保留原文件的注释与键顺序，仅就地更新目标键的值（被编辑键的
行尾注释随旧值一并替换）；写入使用临时文件 + os.replace 原子
替换。文件不存在时不创建（容器模式 .env 未挂载时由调用方降级
为仅热应用）
"""

import os
import re
import tempfile
from pathlib import Path

# 匹配激活的赋值行：export 可选，值可能带引号
_ASSIGN_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")

# 值需要加引号保护的字符
_QUOTE_NEEDED = re.compile(r'[\s#"\'\\]')

# 行尾注释分隔符（# 前需空白分隔，与 python-dotenv 语义一致）
_COMMENT_RE = re.compile(r"\s+#")

# 与 python-dotenv 一致的引号内转义解码表，保证页面展示值与运行时加载值一致
_DOUBLE_QUOTE_ESCAPES = {
    '\\"': '"', "\\\\": "\\", "\\'": "'", "\\a": "\a", "\\b": "\b",
    "\\f": "\f", "\\n": "\n", "\\r": "\r", "\\t": "\t", "\\v": "\v",
}
_SINGLE_QUOTE_ESCAPES = {"\\\\": "\\", "\\'": "'"}
_DOUBLE_ESCAPE_RE = re.compile(r"\\(?:[\\'\"abfnrtv])")
_SINGLE_ESCAPE_RE = re.compile(r"\\(?:[\\'])")


def _find_closing_quote(value: str, quote: str) -> int:
    """查找未被转义的闭合引号（前面有奇数个反斜杠的引号视为转义）"""
    i = 1
    while i < len(value):
        if value[i] == quote:
            backslashes = 0
            j = i - 1
            while j >= 0 and value[j] == "\\":
                backslashes += 1
                j -= 1
            if backslashes % 2 == 0:
                return i
        i += 1
    return -1


def _strip_quotes(value: str) -> str:
    value = value.strip()
    # 引号开头：先定位闭合引号，闭合后的剩余部分仅当为空白或注释（# 开头）才被剥离，
    # 避免把引号内的 " # " 误当注释；引号内按 python-dotenv 规则解码转义
    if len(value) >= 2 and value[0] in ("'", '"'):
        quote = value[0]
        end = _find_closing_quote(value, quote)
        if end > 0:
            rest = value[end + 1:]
            if not rest.strip() or rest.lstrip().startswith("#"):
                inner = value[1:end]
                if quote == '"':
                    return _DOUBLE_ESCAPE_RE.sub(
                        lambda m: _DOUBLE_QUOTE_ESCAPES.get(m.group(0), m.group(0)), inner,
                    )
                return _SINGLE_ESCAPE_RE.sub(
                    lambda m: _SINGLE_QUOTE_ESCAPES.get(m.group(0), m.group(0)), inner,
                )
        return value
    # 无引号值：剥离行尾注释（# 前需空白分隔，与 python-dotenv 语义一致）
    match = _COMMENT_RE.search(value)
    if match:
        return value[: match.start()].rstrip()
    return value


def load_env_file(path: str | os.PathLike) -> dict:
    """解析 .env 文件中激活的键值对；文件不存在或无法解析时返回空 dict"""
    if not os.path.exists(path):
        return {}
    raw = Path(path).read_bytes()
    text = None
    for enc in ("utf-8-sig", "utf-8", "gbk"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return {}
    result = {}
    for line in text.splitlines():
        m = _ASSIGN_RE.match(line)
        if m:
            result[m.group(1)] = _strip_quotes(m.group(2))
    return result


def _needs_quote(value: str) -> bool:
    return bool(_QUOTE_NEEDED.search(value))


def _render_value(key: str, value: str) -> str:
    # 空值直接写 KEY=（等价于未配置，但保留显式空值语义）
    if value == "":
        return f"{key}="
    if _needs_quote(value):
        # 引号内转义 \ 与 "，与 python-dotenv 解码规则对称，避免重启后配置被丢弃或误解码
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'{key}="{escaped}"'
    return f"{key}={value}"


def update_env_file(path: str | os.PathLike, updates: dict, removes: set) -> bool:
    """就地更新 .env 文件

    updates 中的键写入新值（保留原行位置）；removes 中的键删除激活行。
    文件不存在时不创建并返回 False（调用方应降级为仅热应用）。
    返回是否成功写入。
    """
    if not os.path.exists(path):
        return False
    raw = Path(path).read_bytes()
    text = None
    for enc in ("utf-8-sig", "utf-8", "gbk"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return False
    lines = text.splitlines()
    pending = dict(updates)
    out = []
    for line in lines:
        m = _ASSIGN_RE.match(line)
        if m:
            key = m.group(1)
            if key in pending:
                out.append(_render_value(key, pending.pop(key)))
                continue
            if key in removes:
                removes = removes - {key}
                continue
        out.append(line)
    # 文件中不存在的键追加到末尾
    for key, value in pending.items():
        out.append(_render_value(key, value))
    payload = "\n".join(out) + "\n"
    _write_env_file(path, payload)
    return True


def _write_env_file(path: str | os.PathLike, payload: str) -> None:
    """写入 .env 文件

    普通文件系统上使用临时文件 + os.replace 原子替换；目标为挂载点
    （容器模式挂载宿主机 .env，st_dev 与父目录不同）时 os.replace 会
    替换挂载点、切断与宿主文件的绑定，必须改为直接写。
    """
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if os.path.exists(path):
        try:
            if os.stat(path).st_dev != os.stat(directory).st_dev:
                with open(path, "w", encoding="utf-8", newline="\n") as f:
                    f.write(payload)
                return
        except OSError:
            pass
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".env.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(payload)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
