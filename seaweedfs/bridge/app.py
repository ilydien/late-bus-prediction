"""SeaweedFS filer webhook -> NATS JetStream bridge.

SeaweedFS pushes one JSON envelope per filer event:

    {"key": "/buckets/wmata/...",
     "event_type": "create",
     "message_data": {...}}

This service maps the leading path to a bronze subject and publishes the raw
envelope to JetStream. It replies 5xx when NATS is unavailable so that
SeaweedFS retries (it already has retry/backoff and a dead-letter log).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
LISTEN_PORT = int(os.environ.get("BRIDGE_PORT", "8080"))

# Leading path -> base subject. The matching stream captures both "<subject>"
# and "<subject>.>", so sub-subjects added later are retained too.
ROUTES = {
    "/buckets/wmata/": "wmata.realtime.bronze_data",
    "/buckets/traffic/": "traffic.realtime.bronze_data",
}

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("seaweedfs-bridge")


class Publisher:
    """Lazy, thread-safe JetStream publisher."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._nc = None
        self._js = None

    def _connect(self) -> None:
        if self._nc is not None and not self._nc.is_closed:
            return
        self._nc = nats.connect(NATS_URL)
        self._js = self._nc.jetstream()
        log.info("connected to NATS at %s", NATS_URL)

    def publish(self, subject: str, payload: bytes, msg_id: str) -> None:
        with self._lock:
            self._connect()
            self._js.publish(subject, payload, headers={"Nats-Msg-Id": msg_id})
            self._nc.flush(timeout=5)


publisher = Publisher()


def route(key: str) -> str | None:
    for prefix, subject in ROUTES.items():
        if key.startswith(prefix):
            return subject
    return None


class Handler(BaseHTTPRequestHandler):
    server_version = "seaweedfs-bridge/1.0"

    def log_message(self, fmt, *args) -> None:  # route access logs via logging
        log.info("%s - %s", self.address_string(), fmt % args)

    def _reply(self, status: int, body: str) -> None:
        data = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._reply(200, "ok")
        else:
            self._reply(404, "not found")

    def do_POST(self) -> None:
        if self.path != "/events":
            self._reply(404, "not found")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._reply(400, "bad content-length")
            return

        raw = self.rfile.read(length)
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            self._reply(400, "invalid json")
            return

        key = event.get("key") or ""
        subject = route(key)
        if subject is None:
            # Not a bronze bucket we subscribe to: accept so SeaweedFS does not
            # keep retrying an event we intentionally drop.
            self._reply(200, "ignored")
            return

        # Retries replay identical bytes; hashing the payload lets JetStream
        # dedupe them inside the stream's duplicate window.
        msg_id = hashlib.sha256(raw).hexdigest()

        try:
            publisher.publish(subject, raw, msg_id)
        except Exception:
            log.exception("publish to %s failed for %s", subject, key)
            self._reply(503, "nats unavailable")
            return

        log.info("published %s -> %s", event.get("event_type", "?"), subject)
        self._reply(200, "ok")


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    server.daemon_threads = True

    def stop(signum, _frame) -> None:
        log.info("received signal %s, shutting down", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("listening on :%s, NATS=%s", LISTEN_PORT, NATS_URL)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
