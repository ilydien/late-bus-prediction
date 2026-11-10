import logging
import os
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

TELEGRAM_API_KEY = os.environ.get("TELEGRAM_BOT_API_KEY", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_API_KEY}/sendMessage"


class AlertThrottle:
    _sent: dict[str, float] = {}

    def should_send(self, key: str, cooldown_seconds: int = 300) -> bool:
        now = time.time()
        last = self._sent.get(key, 0.0)
        if now - last >= cooldown_seconds:
            self._sent[key] = now
            return True
        return False


_throttle = AlertThrottle()


def send_alert(
    message: str,
    level: str = "CRITICAL",
    throttle_key: str | None = None,
    throttle_seconds: int = 300,
) -> bool:
    if throttle_key and not _throttle.should_send(throttle_key, throttle_seconds):
        logger.debug("Alert throttled: %s", throttle_key)
        return False

    if not TELEGRAM_API_KEY or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram alert not configured (missing API key or chat ID)")
        return False

    emoji = {"CRITICAL": "\u26a0\ufe0f", "WARNING": "\u26a0\ufe0f", "INFO": "\U0001f514"}.get(level, "\U0001f514")
    text = f"{emoji} *[{level}]* VoltaicAlert\n\n{message}"

    payload = urllib.parse.urlencode(
        {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    ).encode()

    try:
        req = urllib.request.Request(
            TELEGRAM_API_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("Alert sent: [%s] %s", level, message[:100])
        return True
    except Exception as e:
        logger.error("Failed to send Telegram alert: %s", e)
        return False


def send_alert_if(
    condition: bool,
    message: str,
    level: str = "CRITICAL",
    throttle_key: str | None = None,
    throttle_seconds: int = 300,
) -> None:
    if condition:
        send_alert(message, level, throttle_key, throttle_seconds)
