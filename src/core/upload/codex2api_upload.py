"""
Codex2API 账号上传功能
"""

import json
import logging
from datetime import datetime, timezone
from typing import List, Tuple

from curl_cffi import CurlMime
from curl_cffi import requests as cffi_requests

from ...database.models import Account
from ...database.session import get_db

logger = logging.getLogger(__name__)


def normalize_codex2api_url(api_url: str) -> str:
    """规范化 codex2api 根地址。"""
    return (api_url or "").strip().rstrip("/")


def build_codex2api_import_entries(accounts: List[Account]) -> List[dict]:
    """构建 codex2api 导入条目。"""
    entries = []
    for account in accounts or []:
        refresh_token = getattr(account, "refresh_token", None) or ""
        access_token = getattr(account, "access_token", None) or ""
        email = getattr(account, "email", None) or ""
        if not refresh_token and not access_token:
            continue
        entries.append({
            "refresh_token": refresh_token,
            "access_token": access_token,
            "email": email,
        })
    return entries


def upload_to_codex2api(accounts: List[Account], api_url: str, admin_key: str) -> Tuple[bool, str]:
    """上传账号列表到 Codex2API 平台。"""
    if not accounts:
        return False, "无可上传的账号"
    if not api_url:
        return False, "Codex2API URL 未配置"
    if not admin_key:
        return False, "Codex2API Admin Key 未配置"

    entries = build_codex2api_import_entries(accounts)
    if not entries:
        return False, "所有账号均缺少 refresh_token / access_token，无法上传"

    payload = json.dumps(entries, ensure_ascii=False, indent=2).encode("utf-8")
    filename = f"codex2api_import_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    mime = CurlMime()
    mime.addpart(
        name="file",
        data=payload,
        filename=filename,
        content_type="application/json",
    )
    mime.addpart(name="format", data=b"json")

    url = normalize_codex2api_url(api_url) + "/api/admin/accounts/import"
    headers = {
        "X-Admin-Key": admin_key,
    }

    try:
        response = cffi_requests.post(
            url,
            multipart=mime,
            headers=headers,
            proxies=None,
            timeout=60,
            impersonate="chrome110",
        )
        if response.status_code in (200, 201):
            try:
                data = response.json()
            except Exception:
                data = None
            if isinstance(data, dict):
                message = data.get("message")
                if message:
                    return True, message
            return True, f"成功上传 {len(entries)} 个账号到 Codex2API"

        error_msg = f"上传失败: HTTP {response.status_code}"
        try:
            detail = response.json()
            if isinstance(detail, dict):
                error_msg = detail.get("error") or detail.get("message") or error_msg
        except Exception:
            error_msg = f"{error_msg} - {response.text[:200]}"
        return False, error_msg
    except Exception as e:
        logger.error(f"Codex2API 上传异常: {e}")
        return False, f"上传异常: {str(e)}"


def batch_upload_to_codex2api(account_ids: List[int], api_url: str, admin_key: str) -> dict:
    """批量上传指定 ID 的账号到 Codex2API 平台。"""
    results = {
        "success_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "details": [],
    }

    with get_db() as db:
        accounts = []
        for account_id in account_ids:
            account = db.query(Account).filter(Account.id == account_id).first()
            if not account:
                results["failed_count"] += 1
                results["details"].append({"id": account_id, "email": None, "success": False, "error": "账号不存在"})
                continue
            if not (account.refresh_token or account.access_token):
                results["skipped_count"] += 1
                results["details"].append({"id": account.id, "email": account.email, "success": False, "error": "缺少 refresh_token / access_token"})
                continue
            accounts.append(account)

        if not accounts:
            return results

        success, message = upload_to_codex2api(accounts, api_url, admin_key)
        if success:
            for account in accounts:
                results["success_count"] += 1
                results["details"].append({"id": account.id, "email": account.email, "success": True, "message": message})
        else:
            for account in accounts:
                results["failed_count"] += 1
                results["details"].append({"id": account.id, "email": account.email, "success": False, "error": message})

    return results


def test_codex2api_connection(api_url: str, admin_key: str) -> Tuple[bool, str]:
    """测试 Codex2API 连接。"""
    if not api_url:
        return False, "API URL 不能为空"
    if not admin_key:
        return False, "Admin Key 不能为空"

    url = normalize_codex2api_url(api_url) + "/api/admin/health"
    headers = {"X-Admin-Key": admin_key}

    try:
        response = cffi_requests.get(
            url,
            headers=headers,
            proxies=None,
            timeout=10,
            impersonate="chrome110",
        )
        if response.status_code in (200, 204):
            return True, "Codex2API 连接测试成功"
        if response.status_code == 401:
            return False, "连接成功，但 Admin Key 无效"
        if response.status_code == 403:
            return False, "连接成功，但权限不足"
        return False, f"服务器返回异常状态码: {response.status_code}"
    except cffi_requests.exceptions.ConnectionError as e:
        return False, f"无法连接到服务器: {str(e)}"
    except cffi_requests.exceptions.Timeout:
        return False, "连接超时，请检查网络配置"
    except Exception as e:
        return False, f"连接测试失败: {str(e)}"
