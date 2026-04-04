"""
Playwright helper for fetching Sentinel SDK tokens directly in browser context.
"""

from __future__ import annotations

import json
from typing import Callable, Optional
from urllib.parse import urlparse


def _flow_page_url(flow: str) -> str:
    flow_name = str(flow or "").strip().lower()
    mapping = {
        "authorize_continue": "https://auth.openai.com/create-account",
        "username_password_create": "https://auth.openai.com/create-account/password",
        "password_verify": "https://auth.openai.com/log-in/password",
        "email_otp_validate": "https://auth.openai.com/email-verification",
        "oauth_create_account": "https://auth.openai.com/about-you",
    }
    return mapping.get(flow_name, "https://auth.openai.com/about-you")


def _build_playwright_proxy_config(proxy: Optional[str]) -> Optional[dict]:
    proxy_value = str(proxy or "").strip()
    if not proxy_value:
        return None
    parsed = urlparse(proxy_value)
    if not parsed.scheme or not parsed.hostname:
        return {"server": proxy_value}

    server = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        server += f":{parsed.port}"
    config = {"server": server}
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    return config


def get_sentinel_token_via_browser(
    *,
    flow: str,
    proxy: Optional[str] = None,
    timeout_ms: int = 45000,
    page_url: Optional[str] = None,
    headless: bool = True,
    device_id: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[str]:
    """
    Fetch full openai-sentinel-token by invoking SentinelSDK.token(flow) in browser.
    """
    logger = log_fn or (lambda _msg: None)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        logger(f"Sentinel Browser 不可用: {exc}")
        return None

    target_url = str(page_url or _flow_page_url(flow)).strip() or _flow_page_url(flow)
    launch_args = {
        "headless": bool(headless),
        "args": [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
        ],
    }

    proxy_config = _build_playwright_proxy_config(proxy)
    if proxy_config:
        launch_args["proxy"] = proxy_config

    logger(f"Sentinel Browser 启动: flow={flow}, url={target_url}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**launch_args)
        try:
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.7103.92 Safari/537.36"
                ),
                ignore_https_errors=True,
            )

            if device_id:
                try:
                    context.add_cookies(
                        [
                            {
                                "name": "oai-did",
                                "value": str(device_id),
                                "url": "https://auth.openai.com/",
                                "secure": True,
                                "sameSite": "Lax",
                            }
                        ]
                    )
                except Exception:
                    pass

            page = context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)

            sdk_ready = False
            max_wait_ms = min(timeout_ms, 15000)
            poll_ms = 500
            for _ in range(max(1, max_wait_ms // poll_ms)):
                try:
                    sdk_ready = bool(
                        page.evaluate(
                            "() => typeof window.SentinelSDK !== 'undefined' && typeof window.SentinelSDK.token === 'function'"
                        )
                    )
                except Exception:
                    sdk_ready = False
                if sdk_ready:
                    break
                page.wait_for_timeout(poll_ms)

            if not sdk_ready:
                logger("Sentinel Browser 等待 SentinelSDK 超时")
                return None

            result = page.evaluate(
                """
                async ({ flow }) => {
                    try {
                        const token = await window.SentinelSDK.token(flow);
                        return { success: true, token };
                    } catch (e) {
                        return {
                            success: false,
                            error: (e && (e.message || String(e))) || "unknown",
                        };
                    }
                }
                """,
                {"flow": flow},
            )

            if not result or not result.get("success") or not result.get("token"):
                logger(
                    "Sentinel Browser 获取失败: "
                    + str((result or {}).get("error") or "no result")
                )
                return None

            token = str(result["token"] or "").strip()
            if not token:
                logger("Sentinel Browser 返回空 token")
                return None

            try:
                parsed = json.loads(token)
                logger(
                    "Sentinel Browser 成功: "
                    f"p={'Y' if parsed.get('p') else 'N'} "
                    f"t={'Y' if parsed.get('t') else 'N'} "
                    f"c={'Y' if parsed.get('c') else 'N'}"
                )
            except Exception:
                logger(f"Sentinel Browser 成功: len={len(token)}")

            return token
        except Exception as exc:
            logger(f"Sentinel Browser 异常: {exc}")
            return None
        finally:
            browser.close()
