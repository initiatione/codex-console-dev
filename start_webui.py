from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
LAUNCHER_LOG = LOGS_DIR / "start-webui-launcher.log"
DOCKER_DESKTOP_EXE = Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe")


class LaunchError(RuntimeError):
    pass


def log(message: str, level: str = "INFO") -> None:
    LOGS_DIR.mkdir(exist_ok=True)
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} [{level}] {message}"
    print(line, flush=True)
    with LAUNCHER_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动本地 PostgreSQL + Web UI")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=8000, help="监听端口，默认 8000")
    parser.add_argument("--skip-docker", action="store_true", help="跳过 docker compose up / 健康检查")
    parser.add_argument("--no-browser", action="store_true", help="启动后不自动打开浏览器")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不真正启动服务")
    parser.add_argument("--db-timeout", type=int, default=120, help="等待 PostgreSQL 就绪的超时秒数")
    parser.add_argument("--web-timeout", type=int, default=45, help="等待 Web UI 可访问并打开浏览器的超时秒数")
    parser.add_argument("--service", default="postgres", help="docker compose 中的数据库服务名，默认 postgres")
    return parser.parse_args()


def read_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def run_command(command: Iterable[str], *, capture_output: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=capture_output,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        joined = " ".join(command)
        raise LaunchError(f"命令执行失败: {joined}\n{detail}")
    return result


def docker_ready() -> bool:
    result = subprocess.run(
        ["docker", "info"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    return result.returncode == 0


def ensure_docker_cli() -> None:
    if shutil.which("docker") is None:
        raise LaunchError("未找到 docker 命令，请先安装 Docker Desktop 并确保 docker 在 PATH 中。")


def ensure_docker_daemon(timeout_seconds: int) -> None:
    ensure_docker_cli()
    if docker_ready():
        log("Docker daemon 已就绪")
        return

    if DOCKER_DESKTOP_EXE.exists():
        log("Docker daemon 尚未就绪，尝试启动 Docker Desktop", "WARN")
        subprocess.Popen([str(DOCKER_DESKTOP_EXE)], cwd=PROJECT_ROOT)
    else:
        raise LaunchError("Docker daemon 未就绪，且未找到 Docker Desktop.exe。")

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if docker_ready():
            log("Docker daemon 已恢复可用")
            return
        time.sleep(3)

    raise LaunchError("等待 Docker daemon 超时，请确认 Docker Desktop 已完全启动。")


def ensure_postgres_service(service_name: str, timeout_seconds: int, dry_run: bool) -> None:
    if dry_run:
        log(f"[dry-run] 将执行: docker compose up -d {service_name}")
        log(f"[dry-run] 将等待服务 {service_name} 进入 healthy/running")
        return

    run_command(["docker", "compose", "up", "-d", service_name], capture_output=True)
    container_id = run_command(["docker", "compose", "ps", "-q", service_name], capture_output=True).stdout.strip()
    if not container_id:
        raise LaunchError(f"未找到服务 {service_name} 对应的容器，请检查 docker-compose.yml。")

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = run_command(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                container_id,
            ],
            capture_output=True,
        ).stdout.strip()
        if status in {"healthy", "running"}:
            log(f"数据库容器已就绪: {service_name} ({status})")
            return
        time.sleep(2)

    raise LaunchError(f"等待数据库容器 {service_name} 就绪超时。")


def get_display_host(host: str) -> str:
    if not host or host in {"0.0.0.0", "::"}:
        return "127.0.0.1"
    return host


def port_is_available(host: str, port: int) -> bool:
    probe_host = host or "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((probe_host, port))
            return True
        except OSError:
            return False


def resolve_available_port(host: str, preferred_port: int, max_scan: int = 100) -> int:
    for offset in range(max_scan):
        candidate = preferred_port + offset
        if port_is_available(host, candidate):
            return candidate
    return preferred_port


def wait_for_http(url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status < 500:
                    return
        except Exception:
            time.sleep(1)
    raise LaunchError(f"等待 Web UI 可访问超时: {url}")


def maybe_open_browser(url: str, timeout_seconds: int, enabled: bool, dry_run: bool) -> threading.Thread | None:
    if not enabled:
        return None
    if dry_run:
        log(f"[dry-run] 将在服务可访问后打开浏览器: {url}")
        return None

    def _runner() -> None:
        try:
            wait_for_http(url, timeout_seconds)
        except Exception as exc:
            log(f"浏览器自动打开前的可访问性检测失败: {exc}", "WARN")
            return
        webbrowser.open(url)
        log(f"已尝试打开浏览器: {url}")

    thread = threading.Thread(target=_runner, name="webui-browser", daemon=True)
    thread.start()
    return thread


def launch_webui(host: str, port: int, dry_run: bool) -> int:
    os.environ["APP_HOST"] = host
    os.environ["WEBUI_HOST"] = host
    os.environ["APP_PORT"] = str(port)
    os.environ["WEBUI_PORT"] = str(port)

    if dry_run:
        log(f"[dry-run] 将执行: {sys.executable} webui.py")
        log(f"[dry-run] 环境覆盖: APP_HOST={host}, APP_PORT={port}")
        return 0

    log(f"使用 Python 解释器: {sys.executable}")
    log("开始启动 Web UI 主进程")

    from webui import start_webui as run_webui

    try:
        run_webui()
        return 0
    except KeyboardInterrupt:
        log("收到 Ctrl+C，Web UI 已请求停止", "WARN")
        return 0


def main() -> int:
    args = parse_args()
    os.chdir(PROJECT_ROOT)
    log("启动器已就位，开始检查环境")

    env_values = read_env_file(PROJECT_ROOT / ".env")
    database_url = env_values.get("APP_DATABASE_URL") or env_values.get("DATABASE_URL") or ""
    if not database_url:
        log(".env 中未发现 APP_DATABASE_URL / DATABASE_URL，应用可能回退到 SQLite", "WARN")
    elif not database_url.startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
        log(f".env 中数据库配置不是 PostgreSQL: {database_url}", "WARN")
    else:
        log("检测到 PostgreSQL 数据库配置")

    selected_port = resolve_available_port(args.host, args.port, max_scan=100)
    if selected_port != args.port:
        log(f"端口 {args.port} 已占用，自动改用 {selected_port}", "WARN")
    url = f"http://{get_display_host(args.host)}:{selected_port}"
    log(f"本次启动地址: {url}")

    if not args.skip_docker:
        ensure_docker_daemon(args.db_timeout)
        ensure_postgres_service(args.service, args.db_timeout, args.dry_run)
    else:
        log("已按参数跳过 Docker/PostgreSQL 自动启动", "WARN")

    maybe_open_browser(url, args.web_timeout, enabled=not args.no_browser, dry_run=args.dry_run)
    exit_code = launch_webui(args.host, selected_port, args.dry_run)
    if exit_code == 0:
        log("启动器退出，Web UI 进程已正常结束")
    else:
        log(f"Web UI 进程退出码: {exit_code}", "ERROR")
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LaunchError as exc:
        log(str(exc), "ERROR")
        raise SystemExit(1)