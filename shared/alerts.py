"""Telegram alert dispatcher with thread-safe throttling and FIRING/RESOLVED formatting."""

from __future__ import annotations

import logging
import os
import threading
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)


class AlertThrottle:
    """Thread-safe cooldown tracker to avoid spamming Telegram on repeated checks."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sent: dict[str, float] = {}

    def should_send(self, key: str, cooldown_seconds: int = 300) -> bool:
        """Return True if key has not fired within cooldown_seconds."""
        now = time.time()
        with self._lock:
            last = self._sent.get(key, 0.0)
            if now - last >= cooldown_seconds:
                self._sent[key] = now
                return True
            return False


_throttle = AlertThrottle()

_LEVEL_EMOJI = {
    "CRITICAL": "\U0001f6a8",
    "WARNING": "\u26a0\ufe0f",
    "INFO": "\U0001f514",
    "RESOLVED": "\u2705",
}


def format_alert_message(
    message: str,
    level: str = "CRITICAL",
    alert_name: str | None = None,
    status: str = "FIRING",
    runbook: str | None = None,
) -> str:
    """Build a Markdown-formatted Telegram alert message."""
    normalized_status = status.upper()
    emoji = _LEVEL_EMOJI["RESOLVED"] if normalized_status == "RESOLVED" else _LEVEL_EMOJI.get(level.upper(), "\U0001f514")
    header_tag = "RESOLVED" if normalized_status == "RESOLVED" else level.upper()
    title = f" — {alert_name}" if alert_name else ""

    lines = [f"{emoji} *[{header_tag}] WMATA-Platform Alert{title}*", "", message]
    if runbook:
        lines.extend(["", f"Runbook: `{runbook}`"])
    return "\n".join(lines)


def send_alert(
    message: str,
    level: str = "CRITICAL",
    throttle_key: str | None = None,
    throttle_seconds: int = 300,
    alert_name: str | None = None,
    status: str = "FIRING",
    runbook: str | None = None,
) -> bool:
    """Send an alert notification to Telegram if configured and not throttled."""
    if throttle_key and status.upper() != "RESOLVED":
        if not _throttle.should_send(throttle_key, throttle_seconds):
            logger.debug("Alert throttled: %s", throttle_key)
            return False

    api_key = os.environ.get("TELEGRAM_BOT_API_KEY", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not api_key or not chat_id:
        logger.warning("Telegram alert not configured (missing API key or chat ID)")
        return False

    text = format_alert_message(
        message=message,
        level=level,
        alert_name=alert_name,
        status=status,
        runbook=runbook,
    )
    payload = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    ).encode("utf-8")

    url = f"https://api.telegram.org/bot{api_key}/sendMessage"
    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if 200 <= resp.status < 300:
                logger.info("Alert sent: [%s/%s] %s", status, level, message[:100])
                return True
            logger.error("Telegram API returned status %s", resp.status)
            return False
    except Exception as exc:
        logger.error("Failed to send Telegram alert: %s", exc)
        return False


def send_alert_if(
    condition: bool,
    message: str,
    level: str = "CRITICAL",
    throttle_key: str | None = None,
    throttle_seconds: int = 300,
    alert_name: str | None = None,
    runbook: str | None = None,
) -> None:
    """Send an alert only when condition is True."""
    if condition:
        send_alert(
            message=message,
            level=level,
            throttle_key=throttle_key,
            throttle_seconds=throttle_seconds,
            alert_name=alert_name,
            runbook=runbook,
        )
