from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.database.models import EmailService as EmailServiceModel
from src.database.session import get_db, init_database
from src.services.luckmail_mail import LuckMailService


DEFAULT_REASON_CODE = "no_receive"
DEFAULT_DESCRIPTION = "等待超过 5 分钟，未收到任何验证码邮件，订单已超时失效"
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAIL_CHECK_RETRIES = 2
DEFAULT_EMPTY_CHECKS = 1
DEFAULT_MAIL_CHECK_INTERVAL_SECONDS = 1.0
DEFAULT_APPEAL_TIMEOUT_SECONDS = 15


logger = logging.getLogger("luckmail_batch_appeal")


@dataclass
class AppealCandidate:
    service_id: int
    service_name: str
    email: str
    token: str
    purchase_id: int


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量排查 LuckMail 已购启用邮箱，并对收件箱为空的邮箱提交申诉。",
    )
    parser.add_argument("--database-url", type=str, default="", help="可选：显式指定数据库 URL；为空时优先读取根目录 .env。")
    parser.add_argument("--service-id", type=int, action="append", default=[], help="只处理指定的 LuckMail 服务 ID，可重复传入。")
    parser.add_argument("--service-name", type=str, default="", help="只处理名称包含该关键词的 LuckMail 服务。")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="拉取已购邮箱列表时每页数量。")
    parser.add_argument("--max-pages", type=int, default=0, help="最多扫描多少页，0 表示直到无数据为止。")
    parser.add_argument("--mail-check-retries", type=int, default=DEFAULT_MAIL_CHECK_RETRIES, help="每个邮箱检查邮件列表的最大尝试次数。")
    parser.add_argument("--required-empty-checks", type=int, default=DEFAULT_EMPTY_CHECKS, help="至少成功确认几次“空邮箱”才会提交申诉。")
    parser.add_argument("--mail-check-interval-seconds", type=float, default=DEFAULT_MAIL_CHECK_INTERVAL_SECONDS, help="多次检查空邮箱之间的等待秒数。")
    parser.add_argument("--reason-code", type=str, default=DEFAULT_REASON_CODE, help="LuckMail 申诉 reason 代码，默认 no_receive。")
    parser.add_argument("--description", type=str, default=DEFAULT_DESCRIPTION, help="LuckMail 申诉 description 内容。")
    parser.add_argument("--appeal-timeout-seconds", type=int, default=DEFAULT_APPEAL_TIMEOUT_SECONDS, help="提交申诉接口超时秒数。")
    parser.add_argument("--appeal-interval-seconds", type=float, default=0.2, help="每次提交申诉后的等待秒数，避免过快。")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要申诉的邮箱，不真正提交。")
    parser.add_argument("--output", type=str, default="", help="可选：把结果写入 JSON 文件。")
    parser.add_argument("--verbose", action="store_true", help="输出更详细的调试日志。")
    return parser.parse_args()


def load_enabled_luckmail_services(args: argparse.Namespace) -> List[EmailServiceModel]:
    service_ids = {int(value) for value in (args.service_id or []) if int(value) > 0}
    name_filter = str(args.service_name or "").strip().lower()

    with get_db() as db:
        query = (
            db.query(EmailServiceModel)
            .filter(
                EmailServiceModel.service_type == "luckmail",
                EmailServiceModel.enabled == True,
            )
            .order_by(EmailServiceModel.priority.asc(), EmailServiceModel.id.asc())
        )
        services = list(query.all())

    filtered: List[EmailServiceModel] = []
    for service in services:
        if service_ids and int(service.id) not in service_ids:
            continue
        current_name = str(service.name or "").strip().lower()
        if name_filter and name_filter not in current_name:
            continue
        filtered.append(service)
    return filtered


def load_env_file(env_path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not env_path.exists():
        return result
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = str(key or "").strip()
        if not key:
            continue
        result[key] = str(value or "").strip().strip('"').strip("'")
    return result


def resolve_database_url(cli_value: str) -> str:
    cli_url = str(cli_value or "").strip()
    if cli_url:
        return cli_url

    env_url = str(os.environ.get("APP_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if env_url:
        return env_url

    env_file_values = load_env_file(ROOT / ".env")
    file_url = str(env_file_values.get("APP_DATABASE_URL") or env_file_values.get("DATABASE_URL") or "").strip()
    if file_url:
        os.environ.setdefault("APP_DATABASE_URL", file_url)
        return file_url
    return ""


def create_luckmail_service(service_model: EmailServiceModel) -> LuckMailService:
    config = dict(service_model.config or {})
    return LuckMailService(config=config, name=str(service_model.name or "luckmail").strip() or "luckmail")


def iter_enabled_purchases(service: LuckMailService, page_size: int, max_pages: int = 0) -> Iterable[AppealCandidate]:
    page = 1
    seen_purchase_ids = set()

    while True:
        if max_pages > 0 and page > max_pages:
            break
        page_result = service._backend_get_purchases(page=page, page_size=page_size, user_disabled=0)
        items = service._extract_list(page_result, "list", "items", "purchases")
        if not items:
            break

        for item in items:
            email = service._normalize_email(service._extract_field(item, "email_address", "address", "email"))
            token = str(service._extract_field(item, "token") or "").strip()
            purchase_id_raw = service._extract_field(item, "id", "purchase_id")
            try:
                purchase_id = int(purchase_id_raw)
            except Exception:
                purchase_id = 0
            if not email or not token or purchase_id <= 0:
                logger.debug("跳过字段不完整的已购邮箱: email=%s purchase_id=%s token=%s", email or "-", purchase_id_raw, bool(token))
                continue
            if purchase_id in seen_purchase_ids:
                continue
            seen_purchase_ids.add(purchase_id)
            yield AppealCandidate(
                service_id=int(getattr(service, "id", 0) or 0),
                service_name=str(service.name or "luckmail"),
                email=email,
                token=token,
                purchase_id=purchase_id,
            )

        if len(items) < page_size:
            break
        page += 1


def check_mailbox_empty(
    service: LuckMailService,
    token: str,
    retries: int,
    required_empty_checks: int,
    interval_seconds: float,
) -> Tuple[str, Dict[str, Any]]:
    successful_empty_checks = 0
    last_error = ""

    for attempt in range(1, max(int(retries or 1), 1) + 1):
        try:
            mails_result = service._backend_get_token_mails(token)
            mails = service._extract_list(mails_result, "mails", "list", "items")
            if mails:
                return "has_mail", {"attempt": attempt, "mail_count": len(mails)}
            successful_empty_checks += 1
            if successful_empty_checks >= max(int(required_empty_checks or 1), 1):
                return "empty", {"attempt": attempt, "mail_count": 0}
        except Exception as exc:
            last_error = str(exc).strip()
            logger.warning("检查邮箱邮件列表失败: token=%s, attempt=%s, error=%s", token, attempt, last_error)

        if attempt < max(int(retries or 1), 1):
            time.sleep(max(float(interval_seconds or 0.0), 0.0))

    if successful_empty_checks > 0:
        return "empty", {"attempt": retries, "mail_count": 0}
    return "error", {"attempt": retries, "error": last_error or "mail check failed"}


def submit_appeal(
    service: LuckMailService,
    purchase_id: int,
    reason_code: str,
    description: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    base_url = str(service.config.get("base_url") or "").strip().rstrip("/")
    api_key = str(service.config.get("api_key") or "").strip()
    if not base_url or not api_key:
        raise RuntimeError("LuckMail 服务缺少 base_url 或 api_key")

    url = f"{base_url}/api/v1/openapi/appeal/create"
    payload = {
        "appeal_type": 2,
        "purchase_id": int(purchase_id),
        "reason": str(reason_code or DEFAULT_REASON_CODE).strip(),
        "description": str(description or DEFAULT_DESCRIPTION).strip(),
    }
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
    }
    response = requests.post(url, headers=headers, json=payload, timeout=max(int(timeout_seconds or 15), 1))
    response.raise_for_status()
    data = response.json()
    if int(data.get("code", -1)) != 0:
        raise RuntimeError(f"LuckMail 申诉接口返回失败: code={data.get('code')} message={data.get('message')}")
    payload_data = data.get("data") or {}
    appeal_no = ""
    if isinstance(payload_data, dict):
        appeal_no = str(payload_data.get("appeal_no") or "").strip()
    return {
        "appeal_no": appeal_no,
        "response": data,
    }


def serialize_service(service: EmailServiceModel) -> Dict[str, Any]:
    return {
        "id": int(service.id),
        "name": str(service.name or ""),
        "service_type": str(service.service_type or ""),
        "enabled": bool(service.enabled),
    }


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    database_url = resolve_database_url(args.database_url)
    if database_url:
        logger.info("使用数据库 URL: %s", database_url)
        init_database(database_url)
    else:
        logger.warning("未提供数据库 URL，且根目录 .env 中也未找到 APP_DATABASE_URL / DATABASE_URL，将回退默认本地 SQLite。")
        init_database()

    services = load_enabled_luckmail_services(args)
    if not services:
        logger.error("未找到符合条件的启用 LuckMail 服务。")
        return 1

    logger.info("命中 %s 个启用 LuckMail 服务。", len(services))

    results: List[Dict[str, Any]] = []
    total_checked = 0
    total_empty = 0
    total_appealed = 0
    total_skipped = 0
    total_errors = 0

    for service_model in services:
        try:
            service = create_luckmail_service(service_model)
            logger.info("开始排查 LuckMail 服务: id=%s name=%s", service_model.id, service_model.name)

            for candidate in iter_enabled_purchases(service, page_size=max(int(args.page_size or 100), 1), max_pages=max(int(args.max_pages or 0), 0)):
                total_checked += 1
                record: Dict[str, Any] = {
                    "service": serialize_service(service_model),
                    "email": candidate.email,
                    "purchase_id": candidate.purchase_id,
                    "token": candidate.token,
                }

                status, detail = check_mailbox_empty(
                    service=service,
                    token=candidate.token,
                    retries=max(int(args.mail_check_retries or 1), 1),
                    required_empty_checks=max(int(args.required_empty_checks or 1), 1),
                    interval_seconds=max(float(args.mail_check_interval_seconds or 0.0), 0.0),
                )
                record["mail_check"] = {"status": status, **detail}

                if status == "has_mail":
                    total_skipped += 1
                    logger.info("跳过有邮件的邮箱: service=%s email=%s purchase_id=%s mail_count=%s", service_model.name, candidate.email, candidate.purchase_id, detail.get("mail_count", 0))
                    results.append(record)
                    continue

                if status == "error":
                    total_errors += 1
                    logger.warning("跳过检查失败的邮箱: service=%s email=%s purchase_id=%s error=%s", service_model.name, candidate.email, candidate.purchase_id, detail.get("error", "unknown"))
                    results.append(record)
                    continue

                total_empty += 1
                if args.dry_run:
                    record["appeal"] = {"status": "dry_run"}
                    total_appealed += 1
                    logger.info("DRY RUN：将申诉空邮箱: service=%s email=%s purchase_id=%s", service_model.name, candidate.email, candidate.purchase_id)
                    results.append(record)
                    continue

                try:
                    appeal_result = submit_appeal(
                        service=service,
                        purchase_id=candidate.purchase_id,
                        reason_code=args.reason_code,
                        description=args.description,
                        timeout_seconds=max(int(args.appeal_timeout_seconds or 15), 1),
                    )
                    record["appeal"] = {"status": "submitted", **appeal_result}
                    total_appealed += 1
                    logger.info("申诉已提交: service=%s email=%s purchase_id=%s appeal_no=%s", service_model.name, candidate.email, candidate.purchase_id, record["appeal"].get("appeal_no", ""))
                except Exception as exc:
                    record["appeal"] = {"status": "failed", "error": str(exc).strip()}
                    total_errors += 1
                    logger.warning("提交申诉失败: service=%s email=%s purchase_id=%s error=%s", service_model.name, candidate.email, candidate.purchase_id, record["appeal"]["error"])

                results.append(record)
                time.sleep(max(float(args.appeal_interval_seconds or 0.0), 0.0))
        except Exception as exc:
            total_errors += 1
            logger.warning("处理 LuckMail 服务失败: id=%s name=%s error=%s", service_model.id, service_model.name, str(exc).strip())
            results.append(
                {
                    "service": serialize_service(service_model),
                    "service_error": str(exc).strip(),
                }
            )

    summary = {
        "checked": total_checked,
        "empty_mailboxes": total_empty,
        "appealed": total_appealed,
        "skipped": total_skipped,
        "errors": total_errors,
        "dry_run": bool(args.dry_run),
        "reason_code": str(args.reason_code or ""),
        "description": str(args.description or ""),
    }
    logger.info("排查完成: %s", json.dumps(summary, ensure_ascii=False))

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.write_text(
            json.dumps({"summary": summary, "results": results}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("结果已写入: %s", output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
