"""
LuckMail 邮箱服务实现
"""

import logging
import json
import re
import sys
import threading
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..config.constants import OTP_CODE_PATTERN
from .luckmail_rust_cli import LuckMailRustCliBackend, resolve_luckmail_rust_cli_path


logger = logging.getLogger(__name__)
_STATE_LOCK = threading.RLock()
_INFLIGHT_EMAIL_TTL_SECONDS = 30 * 60
_INFLIGHT_EMAILS: Dict[str, Dict[str, Any]] = {}
_BATCH_REUSE_POOLS: Dict[str, List[Dict[str, Any]]] = {}
_BATCH_REUSE_PREPARED: Set[str] = set()
_BATCH_PURCHASE_GATES: Dict[str, threading.Lock] = {}
_BATCH_PURCHASE_NEXT_ALLOWED_AT: Dict[str, float] = {}
_NO_STOCK_BREAKERS: Dict[str, Dict[str, Any]] = {}
# 申诉硬编码开关（临时）：False=关闭申诉提交；True=开启申诉提交。
LUCKMAIL_APPEAL_ENABLED = False


def _clear_luckmail_modules() -> None:
    stale_names = [name for name in list(sys.modules.keys()) if name == "luckmail" or name.startswith("luckmail.")]
    for name in stale_names:
        sys.modules.pop(name, None)


def _iter_luckmail_python_sdk_paths() -> List[Dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[2]
    candidates: List[Dict[str, str]] = []

    local_python_dir = repo_root / "LuckMailSdk-Python"
    if local_python_dir.is_dir():
        candidates.append({
            "source": "python_sdk_dir",
            "path": str(local_python_dir),
        })

    local_python_zip = repo_root / "LuckMailSdk-Python.zip"
    if local_python_zip.is_file():
        candidates.append({
            "source": "python_sdk_zip",
            "path": f"{local_python_zip}/LuckMailSdk-Python",
        })

    vendored_dir = repo_root / "luckmail"
    if vendored_dir.is_dir():
        candidates.append({
            "source": "vendored_dir",
            "path": str(vendored_dir),
        })

    tools_dir = Path(__file__).resolve().parents[3] / "tools" / "luckmail"
    if tools_dir.is_dir():
        candidates.append({
            "source": "tools_dir",
            "path": str(tools_dir),
        })

    return candidates


def _has_luckmail_rust_sdk_assets() -> bool:
    repo_root = Path(__file__).resolve().parents[2]
    return any(
        candidate.exists()
        for candidate in (
            repo_root / "LuckMailSdk-Rust",
            repo_root / "LuckMailSdk-Rust.zip",
        )
    )



def _normalize_batch_key(value: Any) -> str:
    return str(value or "").strip()

def _get_batch_purchase_gate(batch_key: str) -> threading.Lock:
    key = _normalize_batch_key(batch_key)
    if not key:
        raise ValueError("batch_key 不能为空")
    with _STATE_LOCK:
        gate = _BATCH_PURCHASE_GATES.get(key)
        if gate is None:
            gate = threading.Lock()
            _BATCH_PURCHASE_GATES[key] = gate
        return gate


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0

    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _load_luckmail_client_class():
    """
    兼容多种来源：
    1) 根目录最新 Python SDK（目录 / zip）
    2) 本地 vendored 目录
    3) 环境已安装 luckmail 包
    """
    for candidate in _iter_luckmail_python_sdk_paths():
        path_str = candidate["path"]
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
        try:
            _clear_luckmail_modules()
            from luckmail import LuckMailClient  # type: ignore

            return LuckMailClient, f"python:{candidate['source']}"
        except Exception:
            continue

    try:
        _clear_luckmail_modules()
        from luckmail import LuckMailClient  # type: ignore

        return LuckMailClient, "python:site_package"
    except Exception:
        return None, ""


class LuckMailService(BaseEmailService):
    """LuckMail 接码邮箱服务"""

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        super().__init__(EmailServiceType.LUCKMAIL, name)

        default_config = {
            "base_url": "https://mails.luckyous.com/",
            "api_key": "",
            "project_code": "openai",
            "email_type": "ms_graph",
            "preferred_domain": "",
            # auto: 优先 Rust（若环境已有可执行适配器），否则回退 Python SDK。
            "sdk_preference": "auto",
            # purchase: 购买邮箱 + token 拉码（可多次）
            # order: 创建接码订单 + 订单拉码（通常一次）
            "inbox_mode": "purchase",
            # 任务开始时优先复用“未在账号库且不在本地黑名单”的已购邮箱
            "reuse_existing_purchases": True,
            "purchase_scan_pages": 5,
            "purchase_scan_page_size": 100,
            "reuse_purchase_candidate_limit": 3,
            "batch_reuse_probe_workers": 8,
            "batch_reuse_probe_limit": 24,
            "batch_reuse_probe_request_timeout_seconds": 2,
            "batch_reuse_probe_allow_python_fallback": False,
            # 已购邮箱在真正投入流程前先做 token alive 检查，避免复用失活邮箱。
            "ensure_purchase_ready": True,
            "token_alive_timeout": 20,
            "token_alive_request_timeout": 4,
            "token_alive_poll_interval": 2.0,
            "purchase_ready_retries": 2,
            # purchase 无库存时自动切到 order，避免整条链路直接失败。
            "fallback_to_order_on_no_stock": True,
            # 同一批次短时间连续无库存时，直接熔断并终止进程，避免继续空转。
            "no_stock_shutdown_enabled": True,
            "no_stock_shutdown_threshold": 5,
            "no_stock_shutdown_window_seconds": 60.0,
            "no_stock_shutdown_exit_delay_seconds": 0.5,
            # 当 token code 接口没有返回验证码时，退回邮件列表/详情接口再读一次。
            "token_mail_fallback": True,
            "timeout": 30,
            "max_retries": 3,
            "poll_interval": 3.0,
            "code_reuse_ttl": 600,
            "batch_purchase_min_interval_seconds": 1.0,
        }
        self.config = {**default_config, **(config or {})}

        self.config["base_url"] = str(self.config.get("base_url") or "").strip()
        if not self.config["base_url"]:
            raise ValueError("LuckMail 配置缺少 base_url")
        self.config["api_key"] = str(self.config.get("api_key") or "").strip()
        self.config["project_code"] = str(self.config.get("project_code") or "openai").strip()
        self.config["email_type"] = str(self.config.get("email_type") or "ms_graph").strip()
        self.config["preferred_domain"] = str(self.config.get("preferred_domain") or "").strip().lstrip("@")
        self.config["sdk_preference"] = str(self.config.get("sdk_preference") or "auto").strip().lower()
        self.config["rust_cli_path"] = str(self.config.get("rust_cli_path") or "").strip().strip('"')
        if self.config["sdk_preference"] not in {"auto", "rust", "python"}:
            self.config["sdk_preference"] = "auto"
        self.config["inbox_mode"] = self._normalize_inbox_mode(self.config.get("inbox_mode"))
        self.config["reuse_existing_purchases"] = _coerce_bool(self.config.get("reuse_existing_purchases", True), True)
        self.config["purchase_scan_pages"] = max(int(self.config.get("purchase_scan_pages") or 5), 1)
        self.config["purchase_scan_page_size"] = max(int(self.config.get("purchase_scan_page_size") or 100), 1)
        self.config["reuse_purchase_candidate_limit"] = max(int(self.config.get("reuse_purchase_candidate_limit") or 3), 1)
        self.config["batch_reuse_probe_workers"] = max(int(self.config.get("batch_reuse_probe_workers") or 8), 1)
        self.config["batch_reuse_probe_limit"] = max(int(self.config.get("batch_reuse_probe_limit") or 24), 1)
        self.config["batch_reuse_probe_request_timeout_seconds"] = max(
            int(self.config.get("batch_reuse_probe_request_timeout_seconds") or 2),
            1,
        )
        self.config["batch_reuse_probe_allow_python_fallback"] = _coerce_bool(
            self.config.get("batch_reuse_probe_allow_python_fallback", False),
            False,
        )
        self.config["ensure_purchase_ready"] = _coerce_bool(self.config.get("ensure_purchase_ready", True), True)
        self.config["token_alive_timeout"] = max(int(self.config.get("token_alive_timeout") or 20), 1)
        self.config["token_alive_request_timeout"] = max(int(self.config.get("token_alive_request_timeout") or 4), 1)
        self.config["token_alive_poll_interval"] = max(float(self.config.get("token_alive_poll_interval") or 2.0), 0.2)
        self.config["purchase_ready_retries"] = max(int(self.config.get("purchase_ready_retries") or 2), 1)
        self.config["fallback_to_order_on_no_stock"] = _coerce_bool(
            self.config.get("fallback_to_order_on_no_stock", True),
            True,
        )
        self.config["no_stock_shutdown_enabled"] = _coerce_bool(
            self.config.get("no_stock_shutdown_enabled", True),
            True,
        )
        self.config["no_stock_shutdown_threshold"] = max(int(self.config.get("no_stock_shutdown_threshold") or 5), 1)
        self.config["no_stock_shutdown_window_seconds"] = max(
            float(self.config.get("no_stock_shutdown_window_seconds") or 60.0),
            1.0,
        )
        self.config["no_stock_shutdown_exit_delay_seconds"] = max(
            float(self.config.get("no_stock_shutdown_exit_delay_seconds", 0.5)),
            0.0,
        )
        self.config["token_mail_fallback"] = _coerce_bool(self.config.get("token_mail_fallback", True), True)
        self.config["poll_interval"] = float(self.config.get("poll_interval") or 3.0)
        self.config["code_reuse_ttl"] = int(self.config.get("code_reuse_ttl") or 600)
        self.config["batch_purchase_min_interval_seconds"] = max(float(self.config.get("batch_purchase_min_interval_seconds") or 1.0), 0.0)
        self._no_stock_threshold_callback = (
            self.config.get("_no_stock_threshold_callback")
            if callable(self.config.get("_no_stock_threshold_callback"))
            else None
        )

        if not self.config["api_key"]:
            raise ValueError("LuckMail 配置缺少 api_key")
        if not self.config["project_code"]:
            raise ValueError("LuckMail 配置缺少 project_code")

        self._rust_sdk_assets_present = _has_luckmail_rust_sdk_assets()
        self._rust_cli_path = resolve_luckmail_rust_cli_path(self.config)
        self._rust_backend: Optional[LuckMailRustCliBackend] = None
        if self._rust_cli_path is not None:
            self._rust_backend = LuckMailRustCliBackend(
                binary_path=self._rust_cli_path,
                base_url=self.config["base_url"],
                api_key=self.config["api_key"],
                timeout_seconds=int(self.config.get("timeout") or 30),
            )

        self.client = None
        self._python_sdk_backend = ""
        client_cls, python_sdk_backend = _load_luckmail_client_class()
        if client_cls is not None:
            try:
                self.client = client_cls(
                    base_url=self.config["base_url"],
                    api_key=self.config["api_key"],
                )
                self._python_sdk_backend = python_sdk_backend or "python:unknown"
            except Exception as exc:
                if self._rust_backend is None or self.config.get("sdk_preference") == "python":
                    raise ValueError(f"初始化 LuckMail 客户端失败: {exc}")
                logger.warning(f"LuckMail Python SDK 初始化失败，暂时仅使用 Rust CLI: {exc}")
        elif self.config.get("sdk_preference") == "python":
            raise ValueError("LuckMail 配置要求使用 Python SDK，但未找到可用 Python SDK")

        if self._rust_backend is None and self.client is None:
            raise ValueError(
                "未找到 LuckMail SDK，请先在当前运行环境安装 luckmail-sdk，或确保系统已安装可执行的 luckmail-cli"
            )

        if self._rust_backend is not None and self.config.get("sdk_preference") in {"auto", "rust"}:
            self._sdk_backend = "rust:cli"
        else:
            self._sdk_backend = self._python_sdk_backend or "python:unknown"

        if (
            self._rust_sdk_assets_present
            and self.config.get("sdk_preference") in {"auto", "rust"}
            and self._rust_backend is None
        ):
            logger.info("LuckMail 检测到 Rust SDK 源码，但当前环境尚未编译出 Rust CLI，暂时回退 Python SDK")

        self._orders_by_no: Dict[str, Dict[str, Any]] = {}
        self._orders_by_email: Dict[str, Dict[str, Any]] = {}
        # 记录每个订单/Token 最近返回过的验证码，避免后续阶段反复拿到旧码。
        self._recent_codes_by_order: Dict[str, Dict[str, float]] = {}
        self._data_dir = Path(__file__).resolve().parents[2] / "data"
        self._registered_file = self._data_dir / "luckmail_registered_emails.json"
        self._failed_file = self._data_dir / "luckmail_failed_emails.json"

    def _normalize_inbox_mode(self, raw: Any) -> str:
        mode = str(raw or "").strip().lower()
        aliases = {
            "purchase": "purchase",
            "token": "purchase",
            "buy": "purchase",
            "purchased": "purchase",
            "order": "order",
            "code": "order",
        }
        return aliases.get(mode, "purchase")

    def _no_stock_breaker_key(self, batch_id: str = "") -> str:
        return _normalize_batch_key(batch_id)

    def _reset_no_stock_breaker(self, batch_id: str = "") -> None:
        key = self._no_stock_breaker_key(batch_id)
        if not key:
            return
        with _STATE_LOCK:
            _NO_STOCK_BREAKERS.pop(key, None)

    def _raise_if_no_stock_shutdown_requested(self, batch_id: str = "") -> None:
        if not bool(self.config.get("no_stock_shutdown_enabled", True)):
            return
        key = self._no_stock_breaker_key(batch_id)
        if not key:
            return
        with _STATE_LOCK:
            requested = bool((_NO_STOCK_BREAKERS.get(key) or {}).get("stop_requested"))
        if requested:
            raise EmailServiceError("LuckMail 连续无库存触发批次熔断，当前批次已停止")

    def _register_no_stock_failure(self, batch_id: str, action: str, error: Any) -> bool:
        if not bool(self.config.get("no_stock_shutdown_enabled", True)):
            return False
        if not self._is_no_stock_error(error):
            return False

        key = self._no_stock_breaker_key(batch_id)
        if not key:
            return False
        threshold = max(int(self.config.get("no_stock_shutdown_threshold") or 5), 1)
        window_seconds = max(float(self.config.get("no_stock_shutdown_window_seconds") or 60.0), 1.0)
        now_ts = time.time()
        with _STATE_LOCK:
            current = dict(_NO_STOCK_BREAKERS.get(key) or {})
            last_at = float(current.get("last_at") or 0.0)
            first_at = float(current.get("first_at") or 0.0)
            count = int(current.get("count") or 0)
            stop_requested = bool(current.get("stop_requested"))
            if last_at <= 0 or (now_ts - last_at) > window_seconds:
                count = 0
                first_at = now_ts
                stop_requested = False
            count += 1
            _NO_STOCK_BREAKERS[key] = {
                "count": count,
                "first_at": first_at or now_ts,
                "last_at": now_ts,
                "stop_requested": stop_requested,
            }
            should_shutdown = count >= threshold and not stop_requested
            if should_shutdown:
                _NO_STOCK_BREAKERS[key]["stop_requested"] = True

        logger.warning(
            "LuckMail 无库存熔断计数: key=%s action=%s count=%s/%s window=%.1fs",
            key,
            action,
            count,
            threshold,
            window_seconds,
        )
        if should_shutdown:
            reason = (
                f"key={key}, action={action}, count={count}, threshold={threshold}, "
                f"error={str(error or "").strip()[:240]}"
            )
            logger.critical("LuckMail 连续无库存触发批次熔断: %s", reason)
            if self._no_stock_threshold_callback:
                try:
                    self._no_stock_threshold_callback(batch_id, reason)
                except Exception as exc:
                    logger.warning(f"LuckMail 批次熔断回调执行失败: {exc}")
        return should_shutdown

    def _is_no_stock_error(self, error: Any) -> bool:
        text = str(error or "").strip().lower()
        if not text:
            return False
        return any(
            marker in text
            for marker in (
                "无库存",
                "库存不足",
                "no stock",
                "code: 2003",
                'code": 2003',
                "api { code: 2003",
            )
        )

    def _extract_field(self, obj: Any, *keys: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            for k in keys:
                if k in obj:
                    return obj.get(k)
            return None
        for k in keys:
            if hasattr(obj, k):
                return getattr(obj, k)
        return None

    def _extract_list(self, obj: Any, *keys: str) -> List[Any]:
        if obj is None:
            return []
        if isinstance(obj, list):
            return list(obj)
        if isinstance(obj, dict):
            for k in keys:
                value = obj.get(k)
                if isinstance(value, list):
                    return list(value)
            nested = obj.get("data")
            if isinstance(nested, dict):
                return self._extract_list(nested, *keys)
            return []
        for k in keys:
            value = getattr(obj, k, None)
            if isinstance(value, list):
                return list(value)
        return []

    def _prefers_rust_backend(self) -> bool:
        return self._rust_backend is not None and self.config.get("sdk_preference") in {"auto", "rust"}

    def _call_preferred_backend(self, action: str, rust_call=None, python_call=None):
        last_error: Optional[Exception] = None
        if self._prefers_rust_backend() and callable(rust_call):
            try:
                return rust_call()
            except Exception as exc:
                last_error = exc
                if callable(python_call):
                    logger.warning(f"LuckMail Rust backend {action} 失败，回退 Python SDK: {exc}")
                else:
                    logger.warning(f"LuckMail Rust backend {action} 失败，且当前运行环境未提供 Python SDK 回退: {exc}")
                if not callable(python_call):
                    raise

        if callable(python_call):
            return python_call()

        if last_error is not None:
            raise last_error
        raise EmailServiceError(f"LuckMail 后端不可用: {action}")

    def _backend_get_purchases(self, page: int, page_size: int, user_disabled: int = 0):
        rust_call = None
        if self._rust_backend is not None:
            rust_call = lambda: self._rust_backend.get_purchases(page=page, page_size=page_size, user_disabled=user_disabled)
        python_call = None
        if self.client is not None:
            python_call = lambda: self.client.user.get_purchases(page=page, page_size=page_size, user_disabled=user_disabled)
        return self._call_preferred_backend("get_purchases", rust_call=rust_call, python_call=python_call)

    def _backend_create_order(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        specified_email: Optional[str] = None,
        variant_mode: Optional[str] = None,
    ):
        rust_call = None
        if self._rust_backend is not None:
            rust_call = lambda: self._rust_backend.create_order(
                project_code=project_code,
                email_type=email_type,
                domain=preferred_domain or None,
                specified_email=specified_email,
                variant_mode=variant_mode,
            )
        python_call = None
        if self.client is not None:
            kwargs: Dict[str, Any] = {
                "project_code": project_code,
                "email_type": email_type,
            }
            if preferred_domain:
                kwargs["domain"] = preferred_domain
            if specified_email:
                kwargs["specified_email"] = specified_email
            if variant_mode:
                kwargs["variant_mode"] = variant_mode
            python_call = lambda: self.client.user.create_order(**kwargs)
        return self._call_preferred_backend("create_order", rust_call=rust_call, python_call=python_call)

    def _backend_purchase_emails(self, project_code: str, quantity: int, email_type: str, preferred_domain: str):
        rust_call = None
        if self._rust_backend is not None:
            rust_call = lambda: self._rust_backend.purchase_emails(
                project_code=project_code,
                quantity=quantity,
                email_type=email_type,
                domain=preferred_domain or None,
            )
        python_call = None
        if self.client is not None:
            kwargs: Dict[str, Any] = {
                "project_code": project_code,
                "quantity": quantity,
                "email_type": email_type,
            }
            if preferred_domain:
                kwargs["domain"] = preferred_domain
            python_call = lambda: self.client.user.purchase_emails(**kwargs)
        return self._call_preferred_backend("purchase_emails", rust_call=rust_call, python_call=python_call)

    def _backend_check_token_alive(
        self,
        token: str,
        request_timeout_seconds: Optional[int] = None,
        allow_python_fallback: bool = True,
    ):
        rust_call = None
        if self._rust_backend is not None:
            rust_call = lambda: self._rust_backend.check_token_alive(token, timeout_seconds=request_timeout_seconds)
        python_alive_method = getattr(getattr(self.client, "user", None), "check_token_alive", None)
        python_call = None
        if callable(python_alive_method) and (allow_python_fallback or self._rust_backend is None or self.config.get("sdk_preference") == "python"):
            python_call = lambda: python_alive_method(token)
        return self._call_preferred_backend("check_token_alive", rust_call=rust_call, python_call=python_call)

    def _backend_get_token_code(self, token: str):
        rust_call = (lambda: self._rust_backend.get_token_code(token)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.get_token_code(token)) if self.client is not None else None
        return self._call_preferred_backend("get_token_code", rust_call=rust_call, python_call=python_call)

    def _backend_get_order_code(self, order_no: str):
        rust_call = (lambda: self._rust_backend.get_order_code(order_no)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.get_order_code(order_no)) if self.client is not None else None
        return self._call_preferred_backend("get_order_code", rust_call=rust_call, python_call=python_call)

    def _backend_get_token_mails(self, token: str):
        rust_call = (lambda: self._rust_backend.get_token_mails(token)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.get_token_mails(token)) if self.client is not None else None
        return self._call_preferred_backend("get_token_mails", rust_call=rust_call, python_call=python_call)

    def _backend_get_token_mail_detail(self, token: str, message_id: str):
        rust_call = (lambda: self._rust_backend.get_token_mail_detail(token, message_id)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.get_token_mail_detail(token, message_id)) if self.client is not None else None
        return self._call_preferred_backend("get_token_mail_detail", rust_call=rust_call, python_call=python_call)

    def _backend_set_purchase_disabled(self, purchase_id: int, disabled: int):
        rust_call = (lambda: self._rust_backend.set_purchase_disabled(purchase_id, disabled)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.set_purchase_disabled(purchase_id, disabled)) if self.client is not None else None
        return self._call_preferred_backend("set_purchase_disabled", rust_call=rust_call, python_call=python_call)

    def _backend_cancel_order(self, order_no: str):
        rust_call = (lambda: self._rust_backend.cancel_order(order_no)) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.cancel_order(order_no)) if self.client is not None else None
        return self._call_preferred_backend("cancel_order", rust_call=rust_call, python_call=python_call)

    def _backend_get_balance(self):
        rust_call = (lambda: self._rust_backend.get_balance()) if self._rust_backend is not None else None
        python_call = (lambda: self.client.user.get_balance()) if self.client is not None else None
        return self._call_preferred_backend("get_balance", rust_call=rust_call, python_call=python_call)

    def _extract_code_from_text(self, text: Any, pattern: str = OTP_CODE_PATTERN) -> str:
        text_value = str(text or "").strip()
        if not text_value:
            return ""
        try:
            match = re.search(pattern or OTP_CODE_PATTERN, text_value)
        except re.error:
            return ""
        if not match:
            return ""
        groups = match.groups()
        if groups:
            for group in groups:
                group_text = str(group or "").strip()
                if group_text:
                    return group_text
        return str(match.group(0) or "").strip()

    def _parse_mail_timestamp(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        for candidate in (normalized, normalized.replace(" ", "T")):
            try:
                parsed = datetime.fromisoformat(candidate)
            except Exception:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        return None

    def _is_terminal_alive_status(self, status: str, message: str) -> bool:
        status_text = str(status or "").strip().lower()
        message_text = str(message or "").strip().lower()
        if status_text in {"failed", "invalid", "disabled", "expired", "deleted", "not_found"}:
            return True
        return any(
            marker in message_text
            for marker in (
                "invalid",
                "disabled",
                "expired",
                "not found",
                "不存在",
                "不可用",
                "失效",
                "禁用",
            )
        )

    def _is_transient_alive_text(self, text: str) -> bool:
        text_value = str(text or "").strip().lower()
        if not text_value:
            return False
        markers = (
            "timeout",
            "timed out",
            "deadline exceeded",
            "context deadline exceeded",
            "connection reset",
            "connection closed abruptly",
            "tls connect error",
            "network error",
            "awaiting headers",
            "read tcp",
            "operation timed out",
            "请求失败",
            "连接重置",
            "超时",
            "网络错误",
        )
        return any(marker in text_value for marker in markers)

    def _classify_alive_failure(
        self,
        status: str,
        message: str,
        error: Optional[Exception] = None,
    ) -> str:
        if error is not None and self._is_transient_alive_text(str(error)):
            return "transient"
        if self._is_terminal_alive_status(status, message):
            return "terminal"
        if self._is_transient_alive_text(status) or self._is_transient_alive_text(message):
            return "transient"
        return "unknown"

    def _record_unavailable_purchase(self, order_info: Dict[str, Any], reason: str) -> None:
        email = self._normalize_email(order_info.get("email"))
        if not email:
            return
        extra = {
            "service_id": order_info.get("service_id"),
            "token": order_info.get("token"),
            "purchase_id": order_info.get("purchase_id"),
            "source": order_info.get("source"),
            "project_code": order_info.get("project_code"),
            "email_type": order_info.get("email_type"),
        }
        self._mark_failed_email(email, reason=reason, extra=extra)

    def _ensure_purchase_inbox_ready(
        self,
        order_info: Dict[str, Any],
        request_timeout_seconds: Optional[int] = None,
        allow_python_fallback: bool = True,
    ) -> bool:
        if not bool(self.config.get("ensure_purchase_ready", True)):
            return True
        if self._normalize_inbox_mode(order_info.get("inbox_mode")) != "purchase":
            return True

        token = str(order_info.get("token") or "").strip()
        email = self._normalize_email(order_info.get("email"))
        if not token:
            return False

        python_alive_method = getattr(getattr(self.client, "user", None), "check_token_alive", None)
        if self._rust_backend is None and not callable(python_alive_method):
            logger.warning("LuckMail 当前 SDK 不支持 token alive 检查，跳过已购邮箱前置探活")
            return True

        source = str(order_info.get("source") or "").strip().lower()
        max_attempts = 1 if source in {"reuse_purchase", "resume_failed"} else 2
        deadline = time.time() + max(int(self.config.get("token_alive_timeout") or 20), 1)
        request_timeout = max(
            int(request_timeout_seconds or self.config.get("token_alive_request_timeout") or 4),
            1,
        )
        poll_interval = max(float(self.config.get("token_alive_poll_interval") or 2.0), 0.2)
        last_status = ""
        last_message = ""
        last_mail_count: Any = None
        last_error: Optional[Exception] = None
        order_info.pop("_alive_failure_kind", None)
        order_info.pop("_alive_failure_detail", None)

        for attempt in range(1, max_attempts + 1):
            remaining_seconds = max(int(deadline - time.time()), 1)
            effective_timeout = max(min(request_timeout, remaining_seconds), 1)
            try:
                result = self._backend_check_token_alive(
                    token,
                    request_timeout_seconds=effective_timeout,
                    allow_python_fallback=allow_python_fallback,
                )
                alive = bool(self._extract_field(result, "alive"))
                status = str(self._extract_field(result, "status") or "").strip().lower()
                message = str(self._extract_field(result, "message") or "").strip()
                mail_count = self._extract_field(result, "mail_count")
                last_status = status
                last_message = message
                last_mail_count = mail_count
                if alive or status in {"ok", "alive", "success", "ready"}:
                    order_info["alive_checked_at"] = self._now_iso()
                    if mail_count not in (None, ""):
                        order_info["mail_count"] = mail_count
                    return True
                if self._is_terminal_alive_status(status, message):
                    break
            except Exception as exc:
                last_error = exc

            if attempt >= max_attempts or time.time() >= deadline:
                break
            time.sleep(min(poll_interval, max(deadline - time.time(), 0.0)))

        if last_error is not None:
            failure_kind = self._classify_alive_failure(last_status, last_message, error=last_error)
            order_info["_alive_failure_kind"] = failure_kind
            order_info["_alive_failure_detail"] = str(last_error)
            logger.warning(f"LuckMail 邮箱可用性检查失败: email={email}, error={last_error}")
            self.update_status(False, last_error)
        else:
            failure_kind = self._classify_alive_failure(last_status, last_message)
            order_info["_alive_failure_kind"] = failure_kind
            order_info["_alive_failure_detail"] = last_message or last_status or ""
            logger.warning(
                "LuckMail 邮箱可用性检查未通过: "
                f"email={email}, status={last_status or '-'}, message={last_message or '-'}, "
                f"mail_count={last_mail_count if last_mail_count not in (None, '') else '-'}"
            )
        return False

    def _probe_reusable_purchase_candidate(
        self,
        info: Dict[str, Any],
        request_timeout_seconds: Optional[int] = None,
        allow_python_fallback: bool = False,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        email = self._normalize_email(info.get("email"))
        if not email:
            return "invalid", None
        if not self._claim_email(email, info):
            logger.info(f"LuckMail 复用邮箱已被当前批次占用，跳过: {email}")
            return "claimed", None

        try:
            if self._ensure_purchase_inbox_ready(
                info,
                request_timeout_seconds=request_timeout_seconds,
                allow_python_fallback=allow_python_fallback,
            ):
                return "ready", info
        except Exception as exc:
            logger.warning(f"LuckMail 复用邮箱探活异常，已跳过: {email}, error={exc}")

        failure_kind = str(info.get("_alive_failure_kind") or "").strip().lower()
        self._release_claimed_email(email)
        if failure_kind == "transient":
            logger.warning(f"LuckMail 复用邮箱探活暂时失败，保留待重试: {email}")
            return "transient", dict(info)

        reason = f"LuckMail 邮箱可用性检查失败: {email}"
        self._record_unavailable_purchase(info, reason)
        logger.warning(f"LuckMail 复用邮箱探活未通过，已跳过: {email}")
        return "failed", None

    def _create_ready_purchase_inbox(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        batch_id: str = "",
    ) -> Dict[str, Any]:
        max_attempts = max(int(self.config.get("purchase_ready_retries") or 2), 1)
        last_error: Optional[EmailServiceError] = None

        for attempt in range(1, max_attempts + 1):
            order_info = self._create_purchase_inbox(
                project_code=project_code,
                email_type=email_type,
                preferred_domain=preferred_domain,
                batch_id=batch_id,
            )
            email = self._normalize_email(order_info.get("email"))
            if not self._claim_email(email, order_info):
                logger.warning(f"LuckMail 邮箱已被并发任务占用，放弃本次分配: {email}")
                raise EmailServiceError(f"LuckMail 邮箱已被并发任务占用: {email}")

            if self._ensure_purchase_inbox_ready(order_info):
                return order_info

            reason = f"LuckMail 邮箱可用性检查失败: {email}"
            failure_kind = str(order_info.get("_alive_failure_kind") or "").strip().lower()
            if failure_kind != "transient":
                self._record_unavailable_purchase(order_info, reason)
            self._release_claimed_email(email)
            last_error = EmailServiceError(reason)
            if attempt < max_attempts:
                if failure_kind == "transient":
                    logger.warning(f"LuckMail 新购邮箱探活暂时失败，准备重试购买: {email} ({attempt}/{max_attempts})")
                else:
                    logger.warning(f"LuckMail 新购邮箱探活未通过，准备重试购买: {email} ({attempt}/{max_attempts})")

        if last_error is not None:
            raise last_error
        raise EmailServiceError("LuckMail 购买邮箱失败：未获取到可用邮箱")

    def _fetch_token_code_from_mail_fallback(
        self,
        token: str,
        pattern: str,
        otp_sent_at: Optional[float] = None,
    ) -> str:
        if not bool(self.config.get("token_mail_fallback", True)):
            return ""

        list_method = None
        detail_method = None
        if self.client is not None:
            user_client = getattr(self.client, "user", None)
            list_method = getattr(user_client, "get_token_mails", None)
            detail_method = getattr(user_client, "get_token_mail_detail", None)
        if self._rust_backend is None and not callable(list_method):
            return ""
        try:
            mails_result = self._backend_get_token_mails(token)
            mails = self._extract_list(mails_result, "mails", "list", "items")
        except Exception as exc:
            logger.warning(f"LuckMail 拉取邮件列表失败: {exc}")
            return ""

        for item in mails:
            received_at = self._parse_mail_timestamp(
                self._extract_field(item, "received_at", "created_at", "createdAt")
            )
            if otp_sent_at and received_at is not None and received_at + 1 < float(otp_sent_at):
                continue

            detail_payload = item
            message_id = str(self._extract_field(item, "message_id", "id") or "").strip()
            if callable(detail_method) and message_id:
                try:
                    detail_payload = self._backend_get_token_mail_detail(token, message_id)
                except Exception as exc:
                    logger.warning(f"LuckMail 拉取邮件详情失败: token={token}, message_id={message_id}, error={exc}")
                    detail_payload = item

            direct_code = str(self._extract_field(detail_payload, "verification_code") or "").strip()
            if direct_code:
                return direct_code

            for text_field in (
                self._extract_field(detail_payload, "body_text", "text", "body"),
                self._extract_field(detail_payload, "body_html", "html_body", "html"),
                self._extract_field(detail_payload, "subject", "mail_subject"),
            ):
                code = self._extract_code_from_text(text_field, pattern)
                if code:
                    return code

        return ""

    def _cache_order(self, info: Dict[str, Any]) -> None:
        order_key = str(info.get("order_no") or info.get("service_id") or "").strip()
        email = str(info.get("email") or "").strip().lower()
        if order_key:
            self._orders_by_no[order_key] = info
        if email:
            self._orders_by_email[email] = info

    def _find_order(self, email: Optional[str], email_id: Optional[str]) -> Optional[Dict[str, Any]]:
        if email_id:
            item = self._orders_by_no.get(str(email_id).strip())
            if item:
                return item
        if email:
            item = self._orders_by_email.get(str(email).strip().lower())
            if item:
                return item
        return None

    def _cleanup_inflight_email_claims(self, now_ts: Optional[float] = None) -> None:
        now_value = float(now_ts or time.time())
        stale_emails: List[str] = []
        for email, meta in list(_INFLIGHT_EMAILS.items()):
            claimed_at = float((meta or {}).get("claimed_at") or 0)
            if claimed_at and (now_value - claimed_at) <= _INFLIGHT_EMAIL_TTL_SECONDS:
                continue
            stale_emails.append(email)
        for email in stale_emails:
            _INFLIGHT_EMAILS.pop(email, None)

    def _claim_email(self, email: Optional[str], extra: Optional[Dict[str, Any]] = None) -> bool:
        email_norm = self._normalize_email(email)
        if not email_norm:
            return False
        with _STATE_LOCK:
            self._cleanup_inflight_email_claims()
            if email_norm in _INFLIGHT_EMAILS:
                return False
            record: Dict[str, Any] = {
                "email": email_norm,
                "claimed_at": time.time(),
                "updated_at": self._now_iso(),
                "service_name": self.name,
            }
            if extra:
                for key in ("service_id", "order_no", "token", "purchase_id", "source"):
                    value = extra.get(key)
                    if value not in (None, ""):
                        record[key] = value
            _INFLIGHT_EMAILS[email_norm] = record
            return True

    def _release_claimed_email(self, email: Optional[str]) -> None:
        email_norm = self._normalize_email(email)
        if not email_norm:
            return
        with _STATE_LOCK:
            _INFLIGHT_EMAILS.pop(email_norm, None)

    def _is_recent_code(self, order_key: str, code: str, now: Optional[float] = None) -> bool:
        if not order_key or not code:
            return False
        now_ts = now or time.time()
        ttl = max(int(self.config.get("code_reuse_ttl") or 600), 0)
        order_cache = self._recent_codes_by_order.get(order_key) or {}
        if ttl <= 0:
            return code in order_cache
        used_at = order_cache.get(code)
        if used_at is None:
            return False
        return (now_ts - used_at) <= ttl

    def _remember_code(self, order_key: str, code: str, now: Optional[float] = None) -> None:
        if not order_key or not code:
            return
        now_ts = now or time.time()
        ttl = max(int(self.config.get("code_reuse_ttl") or 600), 0)
        order_cache = self._recent_codes_by_order.setdefault(order_key, {})
        order_cache[code] = now_ts
        if ttl > 0:
            expire_before = now_ts - ttl
            stale = [k for k, v in order_cache.items() if v < expire_before]
            for key in stale:
                order_cache.pop(key, None)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _normalize_email(self, email: Optional[str]) -> str:
        return str(email or "").strip().lower()

    def _is_resumable_failure_reason(self, reason: str) -> bool:
        text = str(reason or "").strip().lower()
        if not text:
            return False
        keywords = (
            "该邮箱已存在 openai",
            "邮箱已存在 openai",
            "user_already_exists",
            "already exists",
            "创建用户账户失败",
        )
        return any(k in text for k in keywords)

    def _extract_password_from_task_logs(self, logs_text: str) -> str:
        if not logs_text:
            return ""
        matches = re.findall(r"生成密码[:：]\s*([^\s]+)", str(logs_text))
        if not matches:
            return ""
        return str(matches[-1] or "").strip()

    def _recover_password_from_recent_task_logs(self, email: str, max_tasks: int = 30) -> str:
        email_norm = self._normalize_email(email)
        if not email_norm:
            return ""
        try:
            from sqlalchemy import desc
            from ..database.models import RegistrationTask as RegistrationTaskModel
            from ..database.session import get_db

            with get_db() as db:
                tasks = (
                    db.query(RegistrationTaskModel)
                    .filter(RegistrationTaskModel.logs.isnot(None))
                    .order_by(desc(RegistrationTaskModel.created_at))
                    .limit(max_tasks)
                    .all()
                )

            for task in tasks:
                logs_text = str(getattr(task, "logs", "") or "")
                if email_norm not in logs_text.lower():
                    continue
                recovered = self._extract_password_from_task_logs(logs_text)
                if recovered:
                    return recovered
        except Exception as exc:
            logger.warning(f"LuckMail 从任务日志恢复密码失败: {exc}")
        return ""

    def _load_email_index(self, path: Path) -> Dict[str, Dict[str, Any]]:
        with _STATE_LOCK:
            try:
                if not path.exists():
                    return {}
                raw = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    return {
                        self._normalize_email(e): {"email": self._normalize_email(e), "updated_at": self._now_iso()}
                        for e in raw
                        if self._normalize_email(e)
                    }
                if not isinstance(raw, dict):
                    return {}
                payload = raw.get("emails", raw)
                if isinstance(payload, list):
                    return {
                        self._normalize_email(e): {"email": self._normalize_email(e), "updated_at": self._now_iso()}
                        for e in payload
                        if self._normalize_email(e)
                    }
                if isinstance(payload, dict):
                    result: Dict[str, Dict[str, Any]] = {}
                    for email_key, meta in payload.items():
                        email_norm = self._normalize_email(email_key)
                        if not email_norm:
                            continue
                        if isinstance(meta, dict):
                            record = meta.copy()
                        else:
                            record = {"value": meta}
                        record["email"] = email_norm
                        if "updated_at" not in record:
                            record["updated_at"] = self._now_iso()
                        result[email_norm] = record
                    return result
            except Exception as exc:
                logger.warning(f"LuckMail 读取状态文件失败: {path} - {exc}")
                return {}
        return {}

    def _save_email_index(self, path: Path, index: Dict[str, Dict[str, Any]]) -> None:
        with _STATE_LOCK:
            try:
                self._data_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "updated_at": self._now_iso(),
                    "count": len(index),
                    "emails": index,
                }
                tmp_path = path.with_suffix(path.suffix + ".tmp")
                tmp_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                tmp_path.replace(path)
            except Exception as exc:
                logger.warning(f"LuckMail 写入状态文件失败: {path} - {exc}")

    def _mark_registered_email(self, email: str, extra: Optional[Dict[str, Any]] = None) -> None:
        email_norm = self._normalize_email(email)
        if not email_norm:
            return
        with _STATE_LOCK:
            registered = self._load_email_index(self._registered_file)
            failed = self._load_email_index(self._failed_file)
            record = registered.get(email_norm, {"email": email_norm})
            record["updated_at"] = self._now_iso()
            if extra:
                for k, v in extra.items():
                    if v is not None and v != "":
                        record[k] = v
            registered[email_norm] = record
            failed.pop(email_norm, None)
            self._save_email_index(self._registered_file, registered)
            self._save_email_index(self._failed_file, failed)

    def _should_force_failed_record(self, reason: str) -> bool:
        text = str(reason or "").strip().lower()
        if not text:
            return False
        keywords = (
            "该邮箱已存在 openai",
            "邮箱已存在 openai",
            "user_already_exists",
            "already exists",
            "failed to register username",
            "用户名注册失败",
            "创建用户账户失败",
        )
        return any(k in text for k in keywords)

    def _reconcile_failed_over_registered(
        self,
        registered: Dict[str, Dict[str, Any]],
        failed: Dict[str, Dict[str, Any]],
    ) -> bool:
        changed = False
        for email, failed_meta in list(failed.items()):
            if email not in registered:
                continue
            failed_reason = str((failed_meta or {}).get("reason") or "")
            if self._should_force_failed_record(failed_reason):
                registered.pop(email, None)
                changed = True
        return changed

    def _mark_failed_email(
        self,
        email: str,
        reason: str = "",
        extra: Optional[Dict[str, Any]] = None,
        prefer_failed: bool = False,
    ) -> Dict[str, Any]:
        email_norm = self._normalize_email(email)
        if not email_norm:
            return {}

        with _STATE_LOCK:
            registered = self._load_email_index(self._registered_file)
            registered_record: Dict[str, Any] = {}
            if email_norm in registered:
                if not prefer_failed:
                    return registered.get(email_norm) or {}
                registered_record = dict(registered.get(email_norm) or {})
                registered.pop(email_norm, None)
                self._save_email_index(self._registered_file, registered)

            failed = self._load_email_index(self._failed_file)
            record = failed.get(email_norm, {"email": email_norm, "fail_count": 0})
            for k, v in registered_record.items():
                if k not in record and v not in (None, ""):
                    record[k] = v
            record["fail_count"] = int(record.get("fail_count") or 0) + 1
            record["updated_at"] = self._now_iso()
            if reason:
                record["reason"] = reason[:500]
            if extra:
                for k, v in extra.items():
                    if v is not None and v != "":
                        record[k] = v
            failed[email_norm] = record
            self._save_email_index(self._failed_file, failed)
            return record

    def mark_registration_outcome(
        self,
        email: str,
        success: bool,
        reason: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """供任务调度层调用：把注册结果落盘，避免后续重复尝试同邮箱。"""
        try:
            if success:
                self._mark_registered_email(email, extra=context)
            else:
                prefer_failed = self._should_force_failed_record(reason)
                context_copy = dict(context or {})
                password = str(context_copy.get("generated_password") or context_copy.get("password") or "").strip()
                if password:
                    context_copy["password"] = password
                record = self._mark_failed_email(
                    email,
                    reason=reason,
                    extra=context_copy,
                    prefer_failed=prefer_failed,
                )
                self._try_submit_appeal(email=email, reason=reason, context=context_copy, failed_record=record)
        finally:
            self._release_claimed_email(email)

    def _resolve_order_id_by_order_no(self, order_no: str, max_pages: int = 3, page_size: int = 50) -> Optional[int]:
        order_no_text = str(order_no or "").strip()
        if not order_no_text:
            return None
        try:
            for page in range(1, max_pages + 1):
                result = self.client.user.get_orders(page=page, page_size=page_size)
                items = list(getattr(result, "list", []) or [])
                if not items:
                    break
                for item in items:
                    current_order_no = str(self._extract_field(item, "order_no") or "").strip()
                    if current_order_no != order_no_text:
                        continue
                    order_id_raw = self._extract_field(item, "id", "order_id")
                    if order_id_raw in (None, ""):
                        continue
                    try:
                        return int(order_id_raw)
                    except Exception:
                        continue
                if len(items) < page_size:
                    break
        except Exception as exc:
            logger.warning(f"LuckMail 查询订单ID失败: {exc}")
        return None

    def _build_appeal_payload(
        self,
        reason: str,
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        reason_text = str(reason or "").strip()
        reason_lower = reason_text.lower()

        purchase_id_raw = context.get("purchase_id")
        order_id_raw = context.get("order_id")
        order_no = str(context.get("order_no") or "").strip()

        appeal_type = None
        order_id = None
        purchase_id = None

        if purchase_id_raw not in (None, ""):
            try:
                purchase_id = int(purchase_id_raw)
                appeal_type = 2
            except Exception:
                purchase_id = None

        if appeal_type is None and order_id_raw not in (None, ""):
            try:
                order_id = int(order_id_raw)
                appeal_type = 1
            except Exception:
                order_id = None

        if appeal_type is None and order_no:
            order_id = self._resolve_order_id_by_order_no(order_no)
            if order_id is not None:
                appeal_type = 1

        if appeal_type is None:
            return None

        if "429" in reason_lower or "limit" in reason_lower or "限流" in reason_text:
            appeal_reason = "no_code"
        elif "exists" in reason_lower or "already" in reason_lower or "已存在" in reason_text:
            appeal_reason = "email_invalid"
        elif "验证码" in reason_text or "otp" in reason_lower:
            appeal_reason = "wrong_code"
        else:
            appeal_reason = "no_code"

        desc = reason_text or "注册任务失败，申请人工核查并处理。"
        payload: Dict[str, Any] = {
            "appeal_type": appeal_type,
            "reason": appeal_reason,
            "description": desc[:300],
        }
        if appeal_type == 1 and order_id is not None:
            payload["order_id"] = int(order_id)
        if appeal_type == 2 and purchase_id is not None:
            payload["purchase_id"] = int(purchase_id)
        return payload

    def _try_submit_appeal(
        self,
        email: str,
        reason: str,
        context: Dict[str, Any],
        failed_record: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not LUCKMAIL_APPEAL_ENABLED:
            return

        reason_text = str(reason or "").strip()
        if not reason_text:
            return

        reason_lower = reason_text.lower()
        should_appeal = (
            "429" in reason_lower
            or "限流" in reason_text
            or "验证码" in reason_text
            or "otp" in reason_lower
            or "failed to register username" in reason_lower
            or "用户名注册失败" in reason_text
            or "创建用户账户失败" in reason_text
            or "该邮箱已存在 openai" in reason_lower
            or "user_already_exists" in reason_lower
            or "already exists" in reason_lower
        )
        if not should_appeal:
            return

        email_norm = self._normalize_email(email)
        if not email_norm:
            return

        failed_index = self._load_email_index(self._failed_file)
        current = failed_index.get(email_norm, {})
        if not current and failed_record:
            current = failed_record

        # 申诉不代表删除：仅记录状态，不从 failed 名单移除。
        last_appeal_status = str(current.get("appeal_status") or "").strip().lower()
        if last_appeal_status == "submitted":
            return

        payload = self._build_appeal_payload(reason_text, context)
        if not payload:
            return

        try:
            response = self.client.user.create_appeal(**payload)
            appeal_no = str(self._extract_field(response, "appeal_no") or "").strip()
            current["appeal_status"] = "submitted"
            current["appeal_at"] = self._now_iso()
            if appeal_no:
                current["appeal_no"] = appeal_no
            failed_index[email_norm] = current
            self._save_email_index(self._failed_file, failed_index)
            logger.info(f"LuckMail 已提交申诉: email={email_norm}, appeal_no={appeal_no or '-'}")
        except Exception as exc:
            current["appeal_status"] = "failed"
            current["appeal_error"] = str(exc)[:500]
            current["appeal_at"] = self._now_iso()
            failed_index[email_norm] = current
            self._save_email_index(self._failed_file, failed_index)
            logger.warning(f"LuckMail 提交申诉失败: email={email_norm}, error={exc}")

    def _query_existing_account_emails(self, emails: Set[str]) -> Set[str]:
        if not emails:
            return set()
        try:
            from sqlalchemy import func
            from ..database.models import Account as AccountModel
            from ..database.session import get_db

            normalized = [self._normalize_email(e) for e in emails if self._normalize_email(e)]
            if not normalized:
                return set()

            with get_db() as db:
                rows = (
                    db.query(func.lower(AccountModel.email))
                    .filter(func.lower(AccountModel.email).in_(normalized))
                    .all()
                )
            result = set()
            for row in rows:
                try:
                    value = row[0]
                except Exception:
                    value = ""
                email_norm = self._normalize_email(value)
                if email_norm:
                    result.add(email_norm)
            return result
        except Exception as exc:
            logger.warning(f"LuckMail 查询账号库邮箱失败: {exc}")
            return set()

    def _iter_purchase_items(self, scan_pages: int, page_size: int):
        for page in range(1, scan_pages + 1):
            try:
                page_result = self._backend_get_purchases(
                    page=page,
                    page_size=page_size,
                    user_disabled=0,
                )
            except Exception as exc:
                logger.warning(f"LuckMail 拉取已购邮箱失败: page={page}, error={exc}")
                break

            items = self._extract_list(page_result, "list", "items", "purchases")
            if not items:
                break

            for item in items:
                yield item

            if len(items) < page_size:
                break

    def _build_purchase_order_info(
        self,
        item: Any,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        source: str,
    ) -> Optional[Dict[str, Any]]:
        email = self._normalize_email(self._extract_field(item, "email_address", "address", "email"))
        token = str(self._extract_field(item, "token") or "").strip()
        purchase_id_raw = self._extract_field(item, "id", "purchase_id")
        purchase_id = str(purchase_id_raw).strip() if purchase_id_raw not in (None, "") else ""

        if not email or not token:
            return None

        if preferred_domain:
            domain = email.split("@", 1)[1] if "@" in email else ""
            if domain != preferred_domain.lower():
                return None

        return {
            "id": purchase_id or token,
            "service_id": token,
            "order_no": "",
            "email": email,
            "token": token,
            "purchase_id": purchase_id or None,
            "inbox_mode": "purchase",
            "project_code": project_code,
            "email_type": email_type,
            "preferred_domain": preferred_domain,
            "expired_at": "",
            "created_at": time.time(),
            "source": source,
        }

    def _build_reusable_purchase_candidates(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
    ) -> List[Dict[str, Any]]:
        registered = self._load_email_index(self._registered_file)
        failed = self._load_email_index(self._failed_file)
        if self._reconcile_failed_over_registered(registered, failed):
            self._save_email_index(self._registered_file, registered)

        candidates: List[Dict[str, Any]] = []
        for item in self._iter_purchase_items(
            scan_pages=int(self.config.get("purchase_scan_pages") or 5),
            page_size=int(self.config.get("purchase_scan_page_size") or 100),
        ):
            info = self._build_purchase_order_info(
                item=item,
                project_code=project_code,
                email_type=email_type,
                preferred_domain=preferred_domain,
                source="reuse_purchase",
            )
            if not info:
                continue
            email = self._normalize_email(info.get("email"))
            if not email:
                continue
            if email in registered:
                continue
            if email in failed:
                failed_meta = failed.get(email) or {}
                failed_reason = str(failed_meta.get("reason") or "")
                if not self._is_resumable_failure_reason(failed_reason):
                    continue

                resume_password = str(
                    failed_meta.get("password")
                    or failed_meta.get("generated_password")
                    or ""
                ).strip()
                if not resume_password:
                    resume_password = self._recover_password_from_recent_task_logs(email)
                if not resume_password:
                    continue

                info["resume_password"] = resume_password
                info["source"] = "resume_failed"
            candidates.append(info)

        return candidates

    def _reserve_reusable_purchase_inboxes_parallel(
        self,
        probe_items: List[Tuple[int, Dict[str, Any]]],
        desired_count: int,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> List[Dict[str, Any]]:
        worker_count = min(
            max(int(self.config.get("batch_reuse_probe_workers") or 8), 1),
            max(len(probe_items), 1),
        )
        probe_cap = max(int(self.config.get("batch_reuse_probe_limit") or 24), 1)
        probe_limit = min(
            len(probe_items),
            max(
                int(self.config.get("reuse_purchase_candidate_limit") or 3),
                min(max(int(desired_count or 1), 1) * 2, probe_cap),
            ),
        )
        request_timeout_seconds = max(
            int(self.config.get("batch_reuse_probe_request_timeout_seconds") or 2),
            1,
        )
        allow_python_fallback = bool(self.config.get("batch_reuse_probe_allow_python_fallback", False))
        selected_items = probe_items[:probe_limit]
        if not selected_items:
            return []
        total_units = len(selected_items)

        logger.info(
            "LuckMail 批量预扫描启用并发探活: "
            f"workers={worker_count}, probe_limit={probe_limit}, request_timeout={request_timeout_seconds}s, "
            f"allow_python_fallback={allow_python_fallback}, "
            f"desired={desired_count}, candidates={len(probe_items)}"
        )

        processed = 0
        failed_count = 0
        skipped_count = 0
        transient_items: List[Tuple[int, Dict[str, Any]]] = []
        prepared_by_index: Dict[int, Dict[str, Any]] = {}
        if progress_callback:
            progress_callback(
                {
                    "phase": "running",
                    "mode": "parallel",
                    "processed": 0,
                    "total": len(selected_items),
                    "prepared": 0,
                    "failed": 0,
                    "skipped": 0,
                    "workers": worker_count,
                }
            )

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="luckmail_reuse_probe",
        ) as executor:
            future_map = {
                executor.submit(
                    self._probe_reusable_purchase_candidate,
                    dict(info),
                    request_timeout_seconds,
                    allow_python_fallback,
                ): index
                for index, info in selected_items
            }
            for future in as_completed(future_map):
                status, ready_info = future.result()
                processed += 1
                if status == "ready" and ready_info:
                    prepared_by_index[future_map[future]] = ready_info
                elif status == "transient" and ready_info:
                    transient_items.append((future_map[future], ready_info))
                elif status == "failed":
                    failed_count += 1
                else:
                    skipped_count += 1
                if progress_callback:
                    progress_callback(
                        {
                            "phase": "running",
                            "mode": "parallel",
                            "processed": processed,
                            "total": len(selected_items),
                            "prepared": len(prepared_by_index),
                            "failed": failed_count,
                            "skipped": skipped_count,
                            "workers": worker_count,
                        }
                    )

        remaining_needed = max(desired_count - len(prepared_by_index), 0)
        if remaining_needed > 0 and transient_items:
            retry_timeout = max(
                int(self.config.get("token_alive_request_timeout") or 4),
                request_timeout_seconds * 3,
            )
            retry_worker_count = min(max(1, min(4, worker_count)), len(transient_items))
            retry_limit = min(len(transient_items), max(remaining_needed * 2, remaining_needed))
            total_units += retry_limit
            logger.info(
                "LuckMail 批量预扫描对暂时失败候选发起二次探活: "
                f"retry_limit={retry_limit}, retry_workers={retry_worker_count}, retry_timeout={retry_timeout}s"
            )
            if progress_callback:
                progress_callback(
                        {
                            "phase": "retrying",
                            "mode": "parallel_retry",
                            "processed": processed,
                            "total": total_units,
                            "prepared": len(prepared_by_index),
                            "failed": failed_count,
                            "skipped": skipped_count,
                            "workers": retry_worker_count,
                        }
                )
            with ThreadPoolExecutor(
                max_workers=retry_worker_count,
                thread_name_prefix="luckmail_reuse_retry",
            ) as executor:
                retry_futures = {
                    executor.submit(
                        self._probe_reusable_purchase_candidate,
                        dict(info),
                        retry_timeout,
                        True,
                    ): index
                    for index, info in transient_items[:retry_limit]
                }
                for future in as_completed(retry_futures):
                    status, ready_info = future.result()
                    processed += 1
                    if status == "ready" and ready_info:
                        prepared_by_index[retry_futures[future]] = ready_info
                    elif status == "failed":
                        failed_count += 1
                    else:
                        skipped_count += 1
                    if progress_callback:
                        progress_callback(
                        {
                            "phase": "retrying",
                            "mode": "parallel_retry",
                            "processed": processed,
                            "total": total_units,
                            "prepared": len(prepared_by_index),
                            "failed": failed_count,
                            "skipped": skipped_count,
                            "workers": retry_worker_count,
                            }
                        )

        prepared: List[Dict[str, Any]] = []
        for index in sorted(prepared_by_index.keys()):
            prepared.append(prepared_by_index[index])
            if len(prepared) >= desired_count:
                break

        if progress_callback:
            progress_callback(
                {
                    "phase": "done",
                    "mode": "parallel",
                    "processed": processed,
                    "total": total_units,
                    "prepared": len(prepared),
                    "failed": failed_count,
                    "skipped": skipped_count,
                    "workers": worker_count,
                }
            )
        return prepared

    def _reserve_reusable_purchase_inboxes(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        target_count: int = 1,
        log_when_empty: bool = True,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> List[Dict[str, Any]]:
        desired_count = max(int(target_count or 1), 1)
        candidates = self._build_reusable_purchase_candidates(
            project_code=project_code,
            email_type=email_type,
            preferred_domain=preferred_domain,
        )
        if not candidates:
            if log_when_empty:
                logger.info(
                    "LuckMail 未找到可复用已购邮箱候选，跳过复用阶段探活并直接尝试新购；新购后仍会继续执行邮箱可用性检查: "
                    f"project_code={project_code}, email_type={email_type}, domain={preferred_domain or '-'}"
                )
            return []

        existing_in_db = self._query_existing_account_emails({self._normalize_email(c.get("email")) for c in candidates})
        probe_items: List[Tuple[int, Dict[str, Any]]] = []
        for info in candidates:
            email = self._normalize_email(info.get("email"))
            if email in existing_in_db:
                self._mark_registered_email(
                    email,
                    extra={
                        "source": "accounts_db",
                        "token": info.get("token"),
                        "purchase_id": info.get("purchase_id"),
                    },
                )
                continue
            probe_items.append((len(probe_items), info))

        if not probe_items:
            return []

        if desired_count > 1 and int(self.config.get("batch_reuse_probe_workers") or 1) > 1:
            return self._reserve_reusable_purchase_inboxes_parallel(
                probe_items=probe_items,
                desired_count=desired_count,
                progress_callback=progress_callback,
            )

        probe_limit = max(
            int(self.config.get("reuse_purchase_candidate_limit") or 3),
            min(len(probe_items), desired_count * 2),
        )
        total_units = probe_limit
        request_timeout_seconds = max(
            int(self.config.get("batch_reuse_probe_request_timeout_seconds") or 2),
            1,
        )
        allow_python_fallback = bool(self.config.get("batch_reuse_probe_allow_python_fallback", False))
        prepared: List[Dict[str, Any]] = []
        failed_count = 0
        skipped_count = 0
        transient_items: List[Dict[str, Any]] = []
        if progress_callback:
            progress_callback(
                {
                    "phase": "running",
                    "mode": "sequential",
                    "processed": 0,
                    "total": probe_limit,
                    "prepared": 0,
                    "failed": 0,
                    "skipped": 0,
                    "workers": 1,
                }
            )
        for idx, info in probe_items[:probe_limit]:
            if len(prepared) >= desired_count:
                break
            status, ready_info = self._probe_reusable_purchase_candidate(
                info,
                request_timeout_seconds=request_timeout_seconds,
                allow_python_fallback=allow_python_fallback,
            )
            if status == "ready" and ready_info:
                prepared.append(ready_info)
            elif status == "transient" and ready_info:
                transient_items.append(ready_info)
            elif status == "failed":
                failed_count += 1
            else:
                skipped_count += 1
            if progress_callback:
                progress_callback(
                    {
                        "phase": "running",
                        "mode": "sequential",
                        "processed": min(idx + 1, probe_limit),
                        "total": probe_limit,
                        "prepared": len(prepared),
                        "failed": failed_count,
                        "skipped": skipped_count,
                        "workers": 1,
                    }
                )
        remaining_needed = max(desired_count - len(prepared), 0)
        if remaining_needed > 0 and transient_items:
            retry_timeout = max(
                int(self.config.get("token_alive_request_timeout") or 4),
                max(int(self.config.get("batch_reuse_probe_request_timeout_seconds") or 2), 1) * 3,
            )
            retry_limit = min(len(transient_items), max(remaining_needed * 2, remaining_needed))
            total_units += retry_limit
            logger.info(
                "LuckMail 顺序预扫描对暂时失败候选发起二次探活: "
                f"retry_limit={retry_limit}, retry_timeout={retry_timeout}s"
            )
            for retry_index, retry_info in enumerate(transient_items[:retry_limit], start=1):
                if len(prepared) >= desired_count:
                    break
                status, ready_info = self._probe_reusable_purchase_candidate(
                    retry_info,
                    request_timeout_seconds=retry_timeout,
                    allow_python_fallback=True,
                )
                if status == "ready" and ready_info:
                    prepared.append(ready_info)
                elif status == "failed":
                    failed_count += 1
                else:
                    skipped_count += 1
                if progress_callback:
                    progress_callback(
                        {
                            "phase": "retrying",
                            "mode": "sequential_retry",
                            "processed": min(probe_limit + retry_index, total_units),
                            "total": total_units,
                            "prepared": len(prepared),
                            "failed": failed_count,
                            "skipped": skipped_count,
                            "workers": 1,
                        }
                    )
        if probe_limit < len(probe_items):
            logger.info(
                f"LuckMail 复用邮箱候选已达到探测上限，优先检查前 {probe_limit} 个可用候选"
            )
        if progress_callback:
            progress_callback(
                {
                    "phase": "done",
                    "mode": "sequential",
                    "processed": min(total_units, max(probe_limit, len(prepared) + failed_count + skipped_count)),
                    "total": total_units,
                    "prepared": len(prepared),
                    "failed": failed_count,
                    "skipped": skipped_count,
                    "workers": 1,
                }
            )

        return prepared

    def prepare_batch_reusable_inboxes(
        self,
        batch_id: str,
        target_count: int,
        project_code: Optional[str] = None,
        email_type: Optional[str] = None,
        preferred_domain: Optional[str] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> int:
        batch_key = _normalize_batch_key(batch_id)
        if not batch_key:
            return 0
        with _STATE_LOCK:
            if batch_key in _BATCH_REUSE_PREPARED:
                return len(_BATCH_REUSE_POOLS.get(batch_key) or [])

        prepared = self._reserve_reusable_purchase_inboxes(
            project_code=str(project_code or self.config.get("project_code") or "openai").strip(),
            email_type=str(email_type or self.config.get("email_type") or "ms_graph").strip(),
            preferred_domain=str(preferred_domain or self.config.get("preferred_domain") or "").strip().lstrip("@"),
            target_count=max(int(target_count or 0), 0),
            log_when_empty=True,
            progress_callback=progress_callback,
        )

        with _STATE_LOCK:
            _BATCH_REUSE_POOLS[batch_key] = [dict(item) for item in prepared]
            _BATCH_REUSE_PREPARED.add(batch_key)

        logger.info(
            f"LuckMail 批量预扫描完成: batch={batch_key}, prepared={len(prepared)}, requested={max(int(target_count or 0), 0)}"
        )
        return len(prepared)

    @classmethod
    def has_prepared_batch_reuse_pool(cls, batch_id: str) -> bool:
        batch_key = _normalize_batch_key(batch_id)
        if not batch_key:
            return False
        with _STATE_LOCK:
            return batch_key in _BATCH_REUSE_PREPARED

    @classmethod
    def clear_batch_reuse_pool(cls, batch_id: str) -> int:
        batch_key = _normalize_batch_key(batch_id)
        if not batch_key:
            return 0
        released = 0
        with _STATE_LOCK:
            prepared = list(_BATCH_REUSE_POOLS.pop(batch_key, []))
            _BATCH_REUSE_PREPARED.discard(batch_key)
            for info in prepared:
                email = str((info or {}).get("email") or "").strip().lower()
                if not email:
                    continue
                _INFLIGHT_EMAILS.pop(email, None)
                released += 1
        return released

    def _take_prepared_batch_reuse_inbox(self, batch_id: str) -> Optional[Dict[str, Any]]:
        batch_key = _normalize_batch_key(batch_id)
        if not batch_key:
            return None
        with _STATE_LOCK:
            pool = _BATCH_REUSE_POOLS.get(batch_key) or []
            if not pool:
                return None
            order_info = dict(pool.pop(0))
            if not pool:
                _BATCH_REUSE_POOLS[batch_key] = []
        return order_info

    def _run_batch_purchase_action(self, batch_id: str, action: str, func):
        self._raise_if_no_stock_shutdown_requested(batch_id)
        batch_key = _normalize_batch_key(batch_id)
        if not batch_key:
            return func()
        gate = _get_batch_purchase_gate(batch_key)
        min_interval = max(float(self.config.get("batch_purchase_min_interval_seconds") or 1.0), 0.0)
        with gate:
            wait_seconds = 0.0
            with _STATE_LOCK:
                next_allowed_at = float(_BATCH_PURCHASE_NEXT_ALLOWED_AT.get(batch_key) or 0.0)
            now_ts = time.time()
            if next_allowed_at > now_ts:
                wait_seconds = next_allowed_at - now_ts
            if wait_seconds > 0:
                logger.info(
                    f"LuckMail 批量新购节流等待: batch={batch_key}, action={action}, wait={wait_seconds:.2f}s"
                )
                time.sleep(wait_seconds)
            try:
                return func()
            finally:
                with _STATE_LOCK:
                    _BATCH_PURCHASE_NEXT_ALLOWED_AT[batch_key] = time.time() + min_interval

    def _create_order_inbox_impl(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        specified_email: Optional[str] = None,
        batch_id: str = "",
    ) -> Dict[str, Any]:
        def _request_order():
            return self._backend_create_order(
                project_code=project_code,
                email_type=email_type,
                preferred_domain=preferred_domain,
                specified_email=specified_email,
            )

        try:
            order = self._run_batch_purchase_action(batch_id, "create_order", _request_order)
        except Exception as exc:
            self.update_status(False, exc)
            if self._register_no_stock_failure(batch_id, "create_order", exc):
                raise EmailServiceError(f"LuckMail 连续无库存触发批次熔断，当前批次已停止: {exc}")
            raise EmailServiceError(f"LuckMail 创建订单失败: {exc}")

        order_no = str(self._extract_field(order, "order_no") or "").strip()
        email = str(self._extract_field(order, "email_address", "email") or "").strip().lower()
        if not order_no or not email:
            raise EmailServiceError("LuckMail 返回订单信息不完整")

        return {
            "id": order_no,
            "service_id": order_no,
            "order_no": order_no,
            "email": email,
            "token": "",
            "purchase_id": None,
            "inbox_mode": "order",
            "project_code": project_code,
            "email_type": email_type,
            "preferred_domain": preferred_domain,
            "expired_at": str(self._extract_field(order, "expired_at") or "").strip(),
            "created_at": time.time(),
            "source": "new_order",
        }

    def _pick_reusable_purchase_inbox(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
    ) -> Optional[Dict[str, Any]]:
        prepared = self._reserve_reusable_purchase_inboxes(
            project_code=project_code,
            email_type=email_type,
            preferred_domain=preferred_domain,
            target_count=1,
            log_when_empty=True,
        )
        if not prepared:
            return None
        return prepared[0]

    def _create_order_inbox(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        specified_email: Optional[str] = None,
        batch_id: str = "",
    ) -> Dict[str, Any]:
        return self._create_order_inbox_impl(
            project_code=project_code,
            email_type=email_type,
            preferred_domain=preferred_domain,
            specified_email=specified_email,
            batch_id=batch_id,
        )

    def _extract_first_purchase_item(self, purchased: Any) -> Any:
        if purchased is None:
            return None

        if isinstance(purchased, list):
            return purchased[0] if purchased else None

        if isinstance(purchased, dict):
            for key in ("purchases", "list", "items"):
                arr = purchased.get(key)
                if isinstance(arr, list) and arr:
                    return arr[0]
            data = purchased.get("data")
            if isinstance(data, dict):
                for key in ("purchases", "list", "items"):
                    arr = data.get(key)
                    if isinstance(arr, list) and arr:
                        return arr[0]
            return None

        for key in ("purchases", "list", "items"):
            arr = getattr(purchased, key, None)
            if isinstance(arr, list) and arr:
                return arr[0]

        return None

    def _create_purchase_inbox(
        self,
        project_code: str,
        email_type: str,
        preferred_domain: str,
        batch_id: str = "",
    ) -> Dict[str, Any]:
        def _request_purchase():
            return self._backend_purchase_emails(
                project_code=project_code,
                quantity=1,
                email_type=email_type,
                preferred_domain=preferred_domain,
            )

        try:
            purchased = self._run_batch_purchase_action(batch_id, "purchase_emails", _request_purchase)
        except Exception as exc:
            self.update_status(False, exc)
            if self._register_no_stock_failure(batch_id, "purchase_emails", exc):
                raise EmailServiceError(f"LuckMail 连续无库存触发批次熔断，当前批次已停止: {exc}")
            raise EmailServiceError(f"LuckMail 购买邮箱失败: {exc}")

        item = self._extract_first_purchase_item(purchased)
        if item is None:
            raise EmailServiceError("LuckMail 购买邮箱返回为空")

        email = str(self._extract_field(item, "email_address", "address", "email") or "").strip().lower()
        token = str(self._extract_field(item, "token") or "").strip()
        purchase_id_raw = self._extract_field(item, "id", "purchase_id")
        purchase_id = str(purchase_id_raw).strip() if purchase_id_raw not in (None, "") else None

        if not email or not token:
            raise EmailServiceError("LuckMail 购买邮箱返回字段不完整（缺少 email/token）")

        return {
            "id": purchase_id or token,
            "service_id": token,
            "order_no": "",
            "email": email,
            "token": token,
            "purchase_id": purchase_id,
            "inbox_mode": "purchase",
            "project_code": project_code,
            "email_type": email_type,
            "preferred_domain": preferred_domain,
            "expired_at": "",
            "created_at": time.time(),
            "source": "new_purchase",
        }

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        request_config = config or {}
        project_code = str(request_config.get("project_code") or self.config["project_code"]).strip()
        email_type = str(request_config.get("email_type") or self.config["email_type"]).strip()
        preferred_domain = str(
            request_config.get("preferred_domain")
            or request_config.get("domain")
            or self.config.get("preferred_domain")
            or ""
        ).strip().lstrip("@")
        batch_id = _normalize_batch_key(request_config.get("batch_id") or self.config.get("batch_id"))
        self._raise_if_no_stock_shutdown_requested(batch_id)


        inbox_mode = self._normalize_inbox_mode(
            request_config.get("inbox_mode") or request_config.get("mode") or self.config.get("inbox_mode")
        )

        claimed_in_picker = False
        if inbox_mode == "order":
            order_info = self._create_order_inbox(
                project_code=project_code,
                email_type=email_type,
                preferred_domain=preferred_domain,
                batch_id=batch_id,
            )
        else:
            try:
                if bool(self.config.get("reuse_existing_purchases", True)):
                    reused = None
                    if batch_id and self.has_prepared_batch_reuse_pool(batch_id):
                        reused = self._take_prepared_batch_reuse_inbox(batch_id)
                        if reused:
                            logger.info(
                                f"LuckMail 批量预扫描分发复用邮箱: batch={batch_id}, email={reused.get('email') or '-'}"
                            )
                        else:
                            logger.info(
                                "LuckMail 批量预扫描池已空，跳过复用扫描并直接尝试新购: "
                                f"batch={batch_id}, project_code={project_code}, email_type={email_type}, domain={preferred_domain or '-'}"
                            )
                    else:
                        reused = self._pick_reusable_purchase_inbox(
                            project_code=project_code,
                            email_type=email_type,
                            preferred_domain=preferred_domain,
                        )
                    if reused:
                        order_info = reused
                        claimed_in_picker = True
                    else:
                        order_info = self._create_ready_purchase_inbox(
                            project_code=project_code,
                            email_type=email_type,
                            preferred_domain=preferred_domain,
                            batch_id=batch_id,
                        )
                        claimed_in_picker = True
                else:
                    order_info = self._create_ready_purchase_inbox(
                        project_code=project_code,
                        email_type=email_type,
                        preferred_domain=preferred_domain,
                        batch_id=batch_id,
                    )
                    claimed_in_picker = True
            except EmailServiceError as exc:
                if not (
                    bool(self.config.get("fallback_to_order_on_no_stock", True))
                    and self._is_no_stock_error(exc)
                ):
                    raise
                self._raise_if_no_stock_shutdown_requested(batch_id)
                logger.warning(
                    "LuckMail purchase 模式无库存，自动回退 order 模式: "
                    f"project_code={project_code}, email_type={email_type}, domain={preferred_domain or '-'}"
                )
                order_info = self._create_order_inbox(
                    project_code=project_code,
                    email_type=email_type,
                    preferred_domain=preferred_domain,
                    batch_id=batch_id,
                )
                order_info["source"] = "purchase_no_stock_fallback_order"
            else:
                if not claimed_in_picker and order_info.get("inbox_mode") == "purchase":
                    claimed_in_picker = True

        if not claimed_in_picker:
            email = self._normalize_email(order_info.get("email"))
            if not self._claim_email(email, order_info):
                logger.warning(f"LuckMail 邮箱已被并发任务占用，放弃本次分配: {email}")
                raise EmailServiceError(f"LuckMail 邮箱已被并发任务占用: {email}")

        self._reset_no_stock_breaker(batch_id)
        self._cache_order(order_info)
        self.update_status(True)
        return order_info

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        order_info = self._find_order(email=email, email_id=email_id)

        token = ""
        order_no = ""
        inbox_mode = self._normalize_inbox_mode(self.config.get("inbox_mode"))
        if order_info:
            token = str(order_info.get("token") or "").strip()
            order_no = str(order_info.get("order_no") or order_info.get("service_id") or "").strip()
            inbox_mode = self._normalize_inbox_mode(order_info.get("inbox_mode") or inbox_mode)

        if not token and email_id and str(email_id).strip().startswith("tok_"):
            token = str(email_id).strip()
            inbox_mode = "purchase"

        if not order_no and email_id and not token:
            order_no = str(email_id).strip()

        if inbox_mode == "purchase":
            if not token:
                logger.warning(f"LuckMail 未找到 token，无法拉取验证码: email={email}, email_id={email_id}")
                return None
            code_key = f"token:{token}"
        else:
            if not order_no:
                logger.warning(f"LuckMail 未找到订单号，无法拉取验证码: email={email}, email_id={email_id}")
                return None
            code_key = f"order:{order_no}"

        poll_interval = float(self.config.get("poll_interval") or 3.0)
        timeout_s = max(int(timeout or 120), 1)
        deadline = time.time() + timeout_s
        # OTP 刚发送后的短窗口内更容易读到旧码；配合“最近已用验证码”一起过滤。
        otp_guard_until = (float(otp_sent_at) + 1.5) if otp_sent_at else None

        while time.time() < deadline:
            result = None
            code = ""
            try:
                if inbox_mode == "purchase":
                    result = self._backend_get_token_code(token)
                    status = "success" if bool(self._extract_field(result, "has_new_mail")) else "pending"
                else:
                    result = self._backend_get_order_code(order_no)
                    status = str(self._extract_field(result, "status") or "").strip().lower()
            except Exception as exc:
                logger.warning(f"LuckMail 拉取验证码失败: {exc}")
                self.update_status(False, exc)
                if inbox_mode == "purchase":
                    code = self._fetch_token_code_from_mail_fallback(
                        token=token,
                        pattern=pattern,
                        otp_sent_at=otp_sent_at,
                    )
                    if code:
                        status = "success"
                    else:
                        time.sleep(min(poll_interval, 1.0))
                        continue
                else:
                    time.sleep(min(poll_interval, 1.0))
                    continue

            if result is not None:
                code = str(self._extract_field(result, "verification_code") or "").strip()
            if inbox_mode == "purchase" and not code:
                code = self._fetch_token_code_from_mail_fallback(
                    token=token,
                    pattern=pattern,
                    otp_sent_at=otp_sent_at,
                )

            # token 模式下，部分平台会在 has_new_mail=false 时也返回最近一次 code。
            # 这里以 code 为准，再配合“最近已用验证码”过滤旧码。
            if inbox_mode == "purchase" and code:
                status = "success"

            if status in ("timeout", "cancelled"):
                ref = token if inbox_mode == "purchase" else order_no
                logger.info(f"LuckMail 未拿到验证码: {ref}, status={status}")
                return None

            if status == "success" and code:
                if pattern and not re.search(pattern, code):
                    logger.warning(f"LuckMail 返回验证码格式不匹配: {code}")
                    return None

                now_ts = time.time()
                if otp_guard_until and now_ts < otp_guard_until and self._is_recent_code(code_key, code, now_ts):
                    time.sleep(poll_interval)
                    continue

                if self._is_recent_code(code_key, code, now_ts):
                    # 同一 token/订单在不同流程阶段会复用查询接口，这里阻断旧码重复返回。
                    time.sleep(poll_interval)
                    continue

                self._remember_code(code_key, code, now_ts)
                self.update_status(True)
                return code

            time.sleep(poll_interval)

        return None

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        _ = kwargs
        return list(self._orders_by_no.values())

    def delete_email(self, email_id: str) -> bool:
        order_info = self._find_order(email=email_id, email_id=email_id)
        token = str((order_info or {}).get("token") or "").strip()
        purchase_id = str((order_info or {}).get("purchase_id") or "").strip()
        order_no = str((order_info or {}).get("order_no") or "").strip()

        if not token and not order_no:
            raw_id = str(email_id or "").strip()
            if raw_id.startswith("tok_"):
                token = raw_id
            else:
                order_no = raw_id

        if not token and not order_no:
            return False

        try:
            if token and purchase_id.isdigit():
                # 购买邮箱通常不支持直接删除，标记禁用即可。
                try:
                    self._backend_set_purchase_disabled(int(purchase_id), 1)
                except Exception:
                    pass
            elif order_no:
                self._backend_cancel_order(order_no)

            key = token or order_no
            item = self._orders_by_no.pop(key, None)
            if item:
                email = str(item.get("email") or "").strip().lower()
                if email:
                    self._orders_by_email.pop(email, None)
                    self._release_claimed_email(email)
            if token:
                self._recent_codes_by_order.pop(f"token:{token}", None)
            if order_no:
                self._recent_codes_by_order.pop(f"order:{order_no}", None)
            self.update_status(True)
            return True
        except Exception as exc:
            logger.warning(f"LuckMail 删除邮箱失败: {exc}")
            self.update_status(False, exc)
            return False

    def check_health(self) -> bool:
        try:
            self._backend_get_balance()
            self.update_status(True)
            return True
        except Exception as exc:
            logger.warning(f"LuckMail 健康检查失败: {exc}")
            self.update_status(False, exc)
            return False

    def get_service_info(self) -> Dict[str, Any]:
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "base_url": self.config.get("base_url"),
            "project_code": self.config.get("project_code"),
            "email_type": self.config.get("email_type"),
            "preferred_domain": self.config.get("preferred_domain"),
            "sdk_preference": self.config.get("sdk_preference"),
            "sdk_backend": self._sdk_backend,
            "rust_cli_path": str(self._rust_cli_path or ""),
            "inbox_mode": self.config.get("inbox_mode"),
            "cached_orders": len(self._orders_by_no),
            "status": self.status.value,
        }

