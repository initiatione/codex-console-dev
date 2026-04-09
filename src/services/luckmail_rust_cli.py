"""LuckMail Rust CLI backend helpers."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


class LuckMailRustCliError(RuntimeError):
    """Raised when the LuckMail Rust CLI returns an error."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def iter_luckmail_rust_cli_paths(config: Optional[Dict[str, Any]] = None) -> List[Path]:
    config_map = config or {}
    repo_root = _repo_root()
    candidates: List[Path] = []

    configured_path = str(config_map.get("rust_cli_path") or "").strip().strip('"')
    if configured_path:
        candidate = Path(configured_path).expanduser()
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        candidates.append(candidate)

    binary_names = ["luckmail-cli.exe", "luckmail_cli.exe"] if sys.platform.startswith("win") else ["luckmail-cli", "luckmail_cli"]

    for binary_name in binary_names:
        which_path = shutil.which(binary_name)
        if which_path:
            candidates.append(Path(which_path))

    installed_base_dirs = [Path.home() / ".cargo" / "bin"]
    base_dirs = installed_base_dirs + [
        repo_root / "LuckMailSdk-Rust" / "target" / "release",
        repo_root / "LuckMailSdk-Rust" / "target" / "debug",
        repo_root / "LuckMailSdk-Rust",
        repo_root,
    ]
    for base_dir in base_dirs:
        for binary_name in binary_names:
            candidates.append(base_dir / binary_name)

    seen = set()
    resolved_candidates: List[Path] = []
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        resolved_candidates.append(candidate)
    return resolved_candidates


def resolve_luckmail_rust_cli_path(config: Optional[Dict[str, Any]] = None) -> Optional[Path]:
    for candidate in iter_luckmail_rust_cli_paths(config):
        try:
            if candidate.is_file():
                return candidate.resolve()
        except Exception:
            continue
    return None


class LuckMailRustCliBackend:
    """Thin subprocess wrapper around the local Rust CLI adapter."""

    def __init__(
        self,
        binary_path: Path,
        base_url: str,
        api_key: str,
        timeout_seconds: int = 30,
        proxy_url: Optional[str] = None,
    ):
        self.binary_path = str(Path(binary_path).resolve())
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = max(int(timeout_seconds or 30), 1)
        self.proxy_url = str(proxy_url or "").strip()

    def _base_command(self, timeout_seconds: Optional[int] = None) -> List[str]:
        effective_timeout = max(int(timeout_seconds or self.timeout_seconds), 1)
        command = [self.binary_path, "--base-url", self.base_url]
        if self.api_key:
            command.extend(["--api-key", self.api_key])
        command.extend(["--timeout-seconds", str(effective_timeout)])
        return command

    def _run_json(self, *args: str, timeout_seconds: Optional[int] = None) -> Any:
        command = self._base_command(timeout_seconds=timeout_seconds) + [str(arg) for arg in args if arg is not None]
        env = None
        if self.proxy_url:
            env = os.environ.copy()
            env["HTTP_PROXY"] = self.proxy_url
            env["HTTPS_PROXY"] = self.proxy_url
            env["http_proxy"] = self.proxy_url
            env["https_proxy"] = self.proxy_url
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            timeout=max(int(timeout_seconds or self.timeout_seconds), 1) + 5,
            check=False,
        )
        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if completed.returncode != 0:
            detail = stderr or stdout or f"exit code {completed.returncode}"
            raise LuckMailRustCliError(f"LuckMail Rust CLI 执行失败: {detail}")
        if not stdout:
            return {}
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise LuckMailRustCliError(f"LuckMail Rust CLI 返回了无效 JSON: {exc}") from exc

    def get_balance(self) -> Any:
        return self._run_json("get-balance")

    def create_order(
        self,
        project_code: str,
        email_type: Optional[str] = None,
        domain: Optional[str] = None,
        specified_email: Optional[str] = None,
        variant_mode: Optional[str] = None,
    ) -> Any:
        args = ["create-order", "--project-code", project_code]
        if email_type:
            args.extend(["--email-type", email_type])
        if domain:
            args.extend(["--domain", domain])
        if specified_email:
            args.extend(["--specified-email", specified_email])
        if variant_mode:
            args.extend(["--variant-mode", variant_mode])
        return self._run_json(*args)

    def purchase_emails(
        self,
        project_code: str,
        quantity: int,
        email_type: Optional[str] = None,
        domain: Optional[str] = None,
        variant_mode: Optional[str] = None,
    ) -> Any:
        args = ["purchase-emails", "--project-code", project_code, "--quantity", str(int(quantity))]
        if email_type:
            args.extend(["--email-type", email_type])
        if domain:
            args.extend(["--domain", domain])
        if variant_mode:
            args.extend(["--variant-mode", variant_mode])
        return self._run_json(*args)

    def get_purchases(self, page: int = 1, page_size: int = 100, user_disabled: int = 0) -> Any:
        return self._run_json(
            "get-purchases",
            "--page",
            str(int(page)),
            "--page-size",
            str(int(page_size)),
            "--user-disabled",
            str(int(user_disabled)),
        )

    def check_token_alive(self, token: str, timeout_seconds: Optional[int] = None) -> Any:
        return self._run_json("check-token-alive", "--token", token, timeout_seconds=timeout_seconds)

    def get_token_code(self, token: str) -> Any:
        return self._run_json("get-token-code", "--token", token)

    def get_token_mails(self, token: str) -> Any:
        return self._run_json("get-token-mails", "--token", token)

    def get_token_mail_detail(self, token: str, message_id: str) -> Any:
        return self._run_json("get-token-mail-detail", "--token", token, "--message-id", message_id)

    def get_order_code(self, order_no: str) -> Any:
        return self._run_json("get-order-code", "--order-no", order_no)

    def cancel_order(self, order_no: str) -> Any:
        return self._run_json("cancel-order", "--order-no", order_no)

    def set_purchase_disabled(self, purchase_id: int, disabled: int) -> Any:
        return self._run_json(
            "set-purchase-disabled",
            "--purchase-id",
            str(int(purchase_id)),
            "--disabled",
            str(int(disabled)),
        )
