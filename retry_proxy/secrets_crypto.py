"""对称加密号池同步状态文件中的凭据字段。

内存中保持明文供适配器使用；落盘时用 Fernet 加密敏感字段。无密钥时
退化为明文（向后兼容旧部署与无 ADMIN_PASSWORD 的后台同步场景）。
"""

import base64
import hashlib
import json

from cryptography.fernet import Fernet, InvalidToken

# 固定应用盐，仅用于防止预计算彩虹表；密钥强度完全来自传入的 secret 本身。
_SALT = b"llm-retry-proxy/pool-sync/v1"
_ENCRYPTED_MARKER = "__encrypted__"

# 状态文件中需要加密保护的凭据字段。
SENSITIVE_FIELDS = ("password", "access_token", "refresh_token", "cookies")


def derive_key(secret):
    """由主密钥派生 Fernet key；secret 为空返回 None（表示不加密）。"""
    if not secret:
        return None
    raw = hashlib.scrypt(
        secret.encode("utf-8"), salt=_SALT, n=16384, r=8, p=1, dklen=32,
    )
    return base64.urlsafe_b64encode(raw)


def encrypt_session(session, sensitive_fields, key):
    """加密 session 中的敏感字段，返回可序列化的封装结构。

    非敏感字段（email、username 等）保持明文便于排查；敏感字段整体序列化
    后加密为单个 Fernet token。无敏感字段时原样返回。
    """
    if not isinstance(session, dict) or key is None:
        return session
    payload = {}
    for field in sensitive_fields:
        if field in session:
            payload[field] = session[field]
    if not payload:
        return session
    token = Fernet(key).encrypt(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )
    sealed = {k: v for k, v in session.items() if k not in sensitive_fields}
    sealed[_ENCRYPTED_MARKER] = True
    sealed["v"] = 1
    sealed["fields"] = list(payload.keys())
    sealed["data"] = token.decode("ascii")
    return sealed


def decrypt_session(session, key):
    """解密已加密的 session，返回合并后的明文字典。

    非 ``__encrypted__`` 结构（旧明文文件）原样返回，以便自动迁移。
    解密失败抛 ``ValueError``，由调用方决定清空凭据并要求重新登录。
    """
    if not isinstance(session, dict) or not session.get(_ENCRYPTED_MARKER):
        return session
    token = session.get("data")
    if not isinstance(token, str) or key is None:
        raise ValueError("加密凭据无法解密（缺少密钥或数据）")
    try:
        raw = Fernet(key).decrypt(token.encode("ascii"))
    except InvalidToken as exc:
        raise ValueError("加密凭据解密失败（密钥不匹配）") from exc
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("加密凭据载荷格式无法识别")
    merged = {k: v for k, v in session.items()
              if k not in (_ENCRYPTED_MARKER, "v", "fields", "data")}
    merged.update(payload)
    return merged
