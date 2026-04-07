from __future__ import annotations

from typing import Any, Dict, Set

import requests


DEFAULT_APPEAL_TIMEOUT_SECONDS = 15
DEFAULT_APPEAL_REASON_CODE = "no_receive"
DEFAULT_APPEAL_DESCRIPTION = "等待超过 5 分钟，未收到任何验证码邮件，订单已超时失效"
VALID_LUCKMAIL_APPEAL_REASONS: Set[str] = {
    "email_unavailable",
    "no_receive",
    "wrong_code",
    "already_used",
    "other",
}


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0

    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def is_auto_appeal_enabled(config: Dict[str, Any]) -> bool:
    return _coerce_bool((config or {}).get("auto_submit_appeal_on_failure", True), True)


def map_failure_reason_to_appeal_reason(reason: str) -> str:
    _ = str(reason or "").strip()
    return DEFAULT_APPEAL_REASON_CODE


def build_failure_cause_hint(reason: str) -> str:
    text = str(reason or "").strip()
    reason_lower = text.lower()

    if (
        "already exists" in reason_lower
        or "user_already_exists" in reason_lower
        or "failed to register username" in reason_lower
        or "已存在" in text
        or "已被使用" in text
        or "用户名注册失败" in text
        or "创建用户账户失败" in text
    ):
        return "邮箱大概率已被 OpenAI 使用，或用户名阶段触发了风控"

    if (
        "wrong code" in reason_lower
        or "invalid code" in reason_lower
        or "wrong otp" in reason_lower
        or "验证码校验失败" in text
        or "验证码错误" in text
        or "otp 校验失败" in text
    ):
        return "验证码大概率填错了，或命中了旧码 / 过期码"

    if (
        "未收到验证码" in text
        or "收不到验证码" in text
        or "发送验证码失败" in text
        or "no_receive" in reason_lower
        or "no code" in reason_lower
        or "verification code" in reason_lower
    ):
        return "邮箱大概率没有收到任何验证码邮件"

    if (
        "mailbox unavailable" in reason_lower
        or "email unavailable" in reason_lower
        or "邮箱不可用" in text
        or "邮箱失效" in text
        or "邮箱异常" in text
    ):
        return "邮箱本身大概率不可用，或收件链路异常"

    if text:
        return text
    return "注册链路异常，建议结合前面的原始报错一起排查"


def build_failure_appeal_description(reason: str) -> str:
    _ = str(reason or "").strip()
    return DEFAULT_APPEAL_DESCRIPTION


def submit_luckmail_appeal(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    timeout_seconds: int = DEFAULT_APPEAL_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    current_config = dict(config or {})
    base_url = str(current_config.get("base_url") or "").strip().rstrip("/")
    api_key = str(current_config.get("api_key") or "").strip()
    if not base_url or not api_key:
        raise RuntimeError("LuckMail 服务缺少 base_url 或 api_key")

    appeal_reason = str((payload or {}).get("reason") or "").strip()
    if appeal_reason not in VALID_LUCKMAIL_APPEAL_REASONS:
        raise RuntimeError(f"LuckMail 申诉 reason 无效: {appeal_reason}")

    response = requests.post(
        f"{base_url}/api/v1/openapi/appeal/create",
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        json=dict(payload or {}),
        timeout=max(int(timeout_seconds or DEFAULT_APPEAL_TIMEOUT_SECONDS), 1),
    )
    response.raise_for_status()
    data = response.json()
    if int(data.get("code", -1)) != 0:
        raise RuntimeError(
            f"LuckMail 申诉接口返回失败: code={data.get('code')} message={data.get('message')}"
        )

    payload_data = data.get("data") or {}
    appeal_no = ""
    if isinstance(payload_data, dict):
        appeal_no = str(payload_data.get("appeal_no") or "").strip()

    return {
        "appeal_no": appeal_no,
        "response": data,
    }

