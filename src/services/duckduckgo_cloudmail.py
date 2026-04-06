"""
DuckDuckGo CloudMail 邮箱服务实现
通过 duckduckgogo bridge 领取 Duck alias，并通过 CloudMail 精确匹配验证码
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from .cloud_mail import CloudMailService
from ..config.constants import OTP_CODE_PATTERN
from ..core.http_client import HTTPClient, RequestConfig


logger = logging.getLogger(__name__)

_EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_duckduckgo_cloudmail_config(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """兼容字段名，便于前后端和桥接层逐步对齐。"""
    normalized = dict(config or {})

    if normalized.get("api_url") and not normalized.get("bridge_base_url"):
        normalized["bridge_base_url"] = normalized.pop("api_url")

    if normalized.get("base_url") and not normalized.get("bridge_base_url") and not normalized.get("cloudmail_base_url"):
        normalized["bridge_base_url"] = normalized.pop("base_url")

    if normalized.get("cloudmail_api_url") and not normalized.get("cloudmail_base_url"):
        normalized["cloudmail_base_url"] = normalized.pop("cloudmail_api_url")

    if normalized.get("cloudmail_api_key") and not normalized.get("cloudmail_admin_password"):
        normalized["cloudmail_admin_password"] = normalized.pop("cloudmail_api_key")

    if normalized.get("forward_to") and not normalized.get("forward_to_email"):
        normalized["forward_to_email"] = normalized.pop("forward_to")

    if normalized.get("mailbox_email") and not normalized.get("forward_to_email"):
        normalized["forward_to_email"] = normalized.pop("mailbox_email")

    for key in (
        "bridge_base_url",
        "bridge_token",
        "forward_to_email",
        "consumer",
        "purpose",
        "cloudmail_base_url",
        "cloudmail_admin_email",
        "cloudmail_admin_password",
        "cloudmail_domain",
    ):
        if key in normalized and normalized.get(key) is not None:
            normalized[key] = str(normalized.get(key) or "").strip()

    if normalized.get("forward_to_email"):
        normalized["forward_to_email"] = normalized["forward_to_email"].lower()
    if normalized.get("cloudmail_admin_email"):
        normalized["cloudmail_admin_email"] = normalized["cloudmail_admin_email"].lower()

    for key, default in (("timeout", 30), ("max_retries", 3), ("lease_ttl_sec", 900), ("poll_interval", 3), ("list_size", 20)):
        if key in normalized or default is not None:
            normalized[key] = _coerce_int(normalized.get(key), default)

    if normalized.get("cloudmail_service_id") not in (None, ""):
        normalized["cloudmail_service_id"] = _coerce_int(normalized.get("cloudmail_service_id"), 0)

    return normalized


def build_duckduckgo_cloudmail_runtime_config(
    config: Optional[Dict[str, Any]],
    linked_cloudmail_config: Optional[Dict[str, Any]] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    """将 duckduckgo_cloudmail 配置扩展成可直接实例化的运行时配置。"""
    normalized = normalize_duckduckgo_cloudmail_config(config)
    linked = dict(linked_cloudmail_config or {})

    if linked:
        if linked.get("base_url") and not normalized.get("cloudmail_base_url"):
            normalized["cloudmail_base_url"] = str(linked.get("base_url") or "").strip()
        if linked.get("admin_email") and not normalized.get("cloudmail_admin_email"):
            normalized["cloudmail_admin_email"] = str(linked.get("admin_email") or "").strip().lower()
        linked_password = linked.get("admin_password") or linked.get("api_key")
        if linked_password and not normalized.get("cloudmail_admin_password"):
            normalized["cloudmail_admin_password"] = str(linked_password or "").strip()
        linked_domain = linked.get("domain") or linked.get("default_domain")
        if linked_domain and not normalized.get("cloudmail_domain"):
            normalized["cloudmail_domain"] = str(linked_domain or "").strip()

    if proxy_url and not normalized.get("proxy_url"):
        normalized["proxy_url"] = proxy_url

    return normalized


class DuckDuckGoCloudMailService(BaseEmailService):
    """DuckDuckGo alias bridge + CloudMail inbox 适配服务。"""

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        super().__init__(EmailServiceType.DUCKDUCKGO_CLOUDMAIL, name)

        default_config = {
            "bridge_base_url": "",
            "bridge_token": "",
            "forward_to_email": "",
            "consumer": "codex-console-back",
            "purpose": "openai_registration",
            "timeout": 30,
            "max_retries": 3,
            "lease_ttl_sec": 900,
            "poll_interval": 3,
            "list_size": 20,
            "proxy_url": None,
            "cloudmail_base_url": "",
            "cloudmail_admin_email": "",
            "cloudmail_admin_password": "",
            "cloudmail_domain": "",
            "cloudmail_service_id": 0,
        }
        self.config = {**default_config, **build_duckduckgo_cloudmail_runtime_config(config)}
        self.config["bridge_base_url"] = str(self.config.get("bridge_base_url") or "").rstrip("/")
        self.config["cloudmail_base_url"] = str(self.config.get("cloudmail_base_url") or "").rstrip("/")
        self.config["forward_to_email"] = str(self.config.get("forward_to_email") or "").strip().lower()

        missing_keys = [key for key in ("bridge_base_url", "forward_to_email") if not self.config.get(key)]
        if missing_keys:
            raise ValueError(f"缺少必需配置: {missing_keys}")

        missing_cloudmail_keys = [
            key for key in ("cloudmail_base_url", "cloudmail_admin_email", "cloudmail_admin_password")
            if not self.config.get(key)
        ]
        if missing_cloudmail_keys:
            raise ValueError(
                "缺少 CloudMail 运行配置，请提供 cloudmail_* 字段，或先在路由层通过 cloudmail_service_id 解析后再实例化: "
                f"{missing_cloudmail_keys}"
            )

        self.http_client = HTTPClient(
            proxy_url=self.config.get("proxy_url"),
            config=RequestConfig(
                timeout=self.config["timeout"],
                max_retries=self.config["max_retries"],
            ),
        )
        self._cloudmail_service: Optional[CloudMailService] = None
        self._leases_by_id: Dict[str, Dict[str, Any]] = {}
        self._leases_by_email: Dict[str, Dict[str, Any]] = {}

    def _build_bridge_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        bridge_token = str(self.config.get("bridge_token") or "").strip()
        if bridge_token:
            headers["Authorization"] = f"Bearer {bridge_token}"
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _bridge_request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.config['bridge_base_url']}{path}"
        kwargs["headers"] = self._build_bridge_headers(kwargs.get("headers"))

        try:
            response = self.http_client.request(method, url, **kwargs)
            if response.status_code >= 400:
                message = f"bridge 请求失败: {response.status_code}"
                try:
                    message = f"{message} - {response.json()}"
                except Exception:
                    message = f"{message} - {response.text[:200]}"
                raise EmailServiceError(message)

            try:
                return response.json()
            except Exception:
                return {"raw_response": response.text}
        except Exception as exc:
            self.update_status(False, exc)
            if isinstance(exc, EmailServiceError):
                raise
            raise EmailServiceError(f"bridge 请求失败: {method} {path} - {exc}")

    def _get_cloudmail_service(self) -> CloudMailService:
        if self._cloudmail_service is None:
            self._cloudmail_service = CloudMailService(
                {
                    "base_url": self.config["cloudmail_base_url"],
                    "admin_email": self.config["cloudmail_admin_email"],
                    "admin_password": self.config["cloudmail_admin_password"],
                    "domain": self.config.get("cloudmail_domain") or "",
                    "timeout": self.config["timeout"],
                    "max_retries": self.config["max_retries"],
                    "proxy_url": self.config.get("proxy_url"),
                },
                name=f"{self.name}_cloudmail",
            )
        return self._cloudmail_service

    def _cache_lease(self, lease_info: Dict[str, Any]) -> Dict[str, Any]:
        lease_id = str(lease_info.get("lease_id") or lease_info.get("service_id") or lease_info.get("id") or "").strip()
        alias_email = str(lease_info.get("alias_email") or lease_info.get("email") or "").strip().lower()
        cached = dict(lease_info)
        cached["lease_id"] = lease_id
        cached["service_id"] = lease_id
        cached["id"] = lease_id
        cached["alias_email"] = alias_email
        cached["email"] = alias_email
        cached["forward_to_email"] = str(cached.get("forward_to_email") or self.config["forward_to_email"]).strip().lower()

        if lease_id:
            self._leases_by_id[lease_id] = cached
        if alias_email:
            self._leases_by_email[alias_email] = cached
        return cached

    def _get_cached_lease(self, email: Optional[str] = None, email_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        lease_id = str(email_id or "").strip()
        alias_email = str(email or "").strip().lower()

        if lease_id and lease_id in self._leases_by_id:
            return self._leases_by_id[lease_id]
        if alias_email and alias_email in self._leases_by_email:
            return self._leases_by_email[alias_email]
        if lease_id or alias_email:
            return self._cache_lease(
                {
                    "lease_id": lease_id,
                    "service_id": lease_id,
                    "alias_email": alias_email,
                    "email": alias_email,
                    "forward_to_email": self.config["forward_to_email"],
                }
            )
        return None

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[float]:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None

        for fmt in (None, "%Y-%m-%d %H:%M:%S"):
            try:
                if fmt is None:
                    normalized = text.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(normalized)
                else:
                    dt = datetime.strptime(text, fmt)
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).timestamp()
            except Exception:
                continue
        return None

    @staticmethod
    def _extract_addresses(value: Any) -> List[str]:
        addresses: List[str] = []

        def _collect(raw: Any) -> None:
            if raw is None:
                return
            if isinstance(raw, dict):
                for key in ("address", "email", "value"):
                    if raw.get(key):
                        addresses.append(str(raw.get(key) or "").strip().lower())
                return
            if isinstance(raw, list):
                for item in raw:
                    _collect(item)
                return
            text = str(raw or "").strip()
            if not text:
                return
            if text.startswith("[") or text.startswith("{"):
                try:
                    parsed = json.loads(text)
                    _collect(parsed)
                    return
                except Exception:
                    pass
            addresses.extend(match.lower() for match in _EMAIL_PATTERN.findall(text))

        _collect(value)
        deduped: List[str] = []
        for item in addresses:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    @staticmethod
    def _contains_alias(value: Any, alias_email: str) -> bool:
        alias = str(alias_email or "").strip().lower()
        if not alias:
            return False
        text = str(value or "").strip().lower()
        if alias and alias in text:
            return True
        return alias in DuckDuckGoCloudMailService._extract_addresses(value)

    def _is_openai_message(self, message: Dict[str, Any]) -> bool:
        combined = "\n".join(
            str(message.get(key) or "")
            for key in ("sendEmail", "name", "subject", "messageId", "content")
        ).lower()
        return "openai" in combined

    def _is_alias_match(self, message: Dict[str, Any], alias_email: str, forward_to_email: str) -> bool:
        if not self._is_openai_message(message):
            return False

        to_email = str(message.get("toEmail") or "").strip().lower()
        if forward_to_email and to_email and to_email != forward_to_email.lower():
            return False

        if self._contains_alias(message.get("recipient"), alias_email):
            return True
        if self._contains_alias(message.get("sendEmail"), alias_email):
            return True
        if self._contains_alias(message.get("messageId"), alias_email):
            return True
        return self._contains_alias(json.dumps(message, ensure_ascii=False), alias_email)

    def _message_search_text(self, message: Dict[str, Any]) -> str:
        return "\n".join(
            str(message.get(key) or "")
            for key in ("subject", "content", "sendEmail", "name", "recipient", "toEmail", "messageId")
            if message.get(key) not in (None, "")
        )

    def _fetch_cloudmail_messages(self) -> List[Dict[str, Any]]:
        cloudmail_service = self._get_cloudmail_service()
        result = cloudmail_service._make_request(
            "GET",
            "/api/allEmail/list",
            params={
                "emailId": 0,
                "size": self.config.get("list_size", 20),
                "timeSort": 0,
                "type": "receive",
                "searchType": "name",
            },
        )
        if result.get("code") not in (None, 200):
            raise EmailServiceError(f"CloudMail 查询失败: {result}")
        data = result.get("data", [])
        if not isinstance(data, list):
            return []
        return data

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        request_config = normalize_duckduckgo_cloudmail_config(config)
        payload = {
            "consumer": str(request_config.get("consumer") or self.config.get("consumer") or self.name).strip(),
            "purpose": str(request_config.get("purpose") or self.config.get("purpose") or "openai_registration").strip(),
            "lease_ttl_sec": _coerce_int(request_config.get("lease_ttl_sec"), self.config.get("lease_ttl_sec", 900)),
        }
        response = self._bridge_request("POST", "/api/v1/aliases/lease", json=payload)

        alias_email = str(response.get("alias_email") or response.get("email") or "").strip().lower()
        lease_id = str(response.get("lease_id") or response.get("service_id") or response.get("id") or "").strip()
        if not alias_email or not lease_id:
            raise EmailServiceError(f"bridge lease 返回数据不完整: {response}")

        lease_info = self._cache_lease(
            {
                "lease_id": lease_id,
                "service_id": lease_id,
                "id": lease_id,
                "email": alias_email,
                "alias_email": alias_email,
                "forward_to_email": str(response.get("forward_to_email") or self.config["forward_to_email"]).strip().lower(),
                "alias_state": response.get("alias_state"),
                "generated_at": response.get("generated_at"),
                "created_at": time.time(),
                "source": self.service_type.value,
                "raw_response": response,
            }
        )
        self.update_status(True)
        return lease_info

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        lease_info = self._get_cached_lease(email=email, email_id=email_id)
        if not lease_info:
            logger.warning("duckduckgo_cloudmail 未找到租约信息: email=%s email_id=%s", email, email_id)
            return None

        alias_email = str(lease_info.get("alias_email") or lease_info.get("email") or email or "").strip().lower()
        forward_to_email = str(lease_info.get("forward_to_email") or self.config.get("forward_to_email") or "").strip().lower()
        start_time = time.time()
        seen_message_ids = set()
        poll_interval = max(1, _coerce_int(self.config.get("poll_interval"), 3))

        while time.time() - start_time < timeout:
            try:
                for message in self._fetch_cloudmail_messages():
                    message_id = str(message.get("emailId") or message.get("messageId") or "").strip()
                    if message_id and message_id in seen_message_ids:
                        continue
                    if message_id:
                        seen_message_ids.add(message_id)

                    message_ts = self._parse_timestamp(message.get("createTime") or message.get("createdAt"))
                    if otp_sent_at and message_ts and message_ts + 1 < float(otp_sent_at):
                        continue
                    if not self._is_alias_match(message, alias_email, forward_to_email):
                        continue

                    content = self._message_search_text(message)
                    match = re.search(pattern, content)
                    if match:
                        self.update_status(True)
                        return match.group(1)
            except Exception as exc:
                logger.debug("duckduckgo_cloudmail 轮询验证码失败: %s", exc)

            time.sleep(poll_interval)

        logger.warning("等待 DuckDuckGo CloudMail 验证码超时: alias=%s", alias_email)
        return None

    def mark_registration_outcome(
        self,
        email: str,
        success: bool,
        reason: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        context = dict(context or {})
        lease_info = self._get_cached_lease(email=email, email_id=context.get("service_id"))
        if not lease_info:
            logger.debug("duckduckgo_cloudmail 未找到需回写的租约: email=%s", email)
            return

        lease_id = str(lease_info.get("lease_id") or lease_info.get("service_id") or "").strip()
        if not lease_id:
            return

        payload = {
            "success": bool(success),
            "reason": str(reason or "").strip(),
            "email": str(email or lease_info.get("alias_email") or "").strip().lower(),
            "context": context,
        }
        self._bridge_request("POST", f"/api/v1/aliases/{lease_id}/mark-outcome", json=payload)

        alias_email = str(lease_info.get("alias_email") or "").strip().lower()
        if lease_id:
            self._leases_by_id.pop(lease_id, None)
        if alias_email:
            self._leases_by_email.pop(alias_email, None)
        self.update_status(True)

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        return list(self._leases_by_email.values())

    def delete_email(self, email_id: str) -> bool:
        lease_info = self._get_cached_lease(email_id=email_id)
        if not lease_info:
            return False
        try:
            self.mark_registration_outcome(
                email=str(lease_info.get("alias_email") or ""),
                success=False,
                reason="lease_deleted",
                context={"service_id": str(email_id or "")},
            )
            return True
        except Exception as exc:
            logger.warning("duckduckgo_cloudmail 删除租约失败: %s", exc)
            self.update_status(False, exc)
            return False

    def check_health(self) -> bool:
        bridge_ok = False
        cloudmail_ok = False

        try:
            self._bridge_request("GET", "/api/v1/pool/stats")
            bridge_ok = True
        except Exception as exc:
            logger.warning("duckduckgo_cloudmail bridge 健康检查失败: %s", exc)

        try:
            cloudmail_ok = self._get_cloudmail_service().check_health()
        except Exception as exc:
            logger.warning("duckduckgo_cloudmail CloudMail 健康检查失败: %s", exc)
            cloudmail_ok = False

        if bridge_ok and cloudmail_ok:
            self.update_status(True)
            return True

        self.update_status(False, EmailServiceError("bridge 或 CloudMail 不可用"))
        return False

    def get_service_info(self) -> Dict[str, Any]:
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "bridge_base_url": self.config.get("bridge_base_url"),
            "forward_to_email": self.config.get("forward_to_email"),
            "cloudmail_base_url": self.config.get("cloudmail_base_url"),
            "cloudmail_service_id": self.config.get("cloudmail_service_id") or None,
            "cached_leases": len(self._leases_by_email),
            "status": self.status.value,
        }
