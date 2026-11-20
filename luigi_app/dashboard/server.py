"""Luigi's local-only read-only dashboard. Inject a read-only broker snapshot capability."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import hmac
import json
import re
import secrets
import threading
import uuid


_ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/dashboard.css": ("dashboard.css", "text/css; charset=utf-8"),
    "/dashboard.js": ("dashboard.js", "text/javascript; charset=utf-8"),
}
_CSP = ("default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; object-src 'none'")
_QUEUE = re.compile(r"[a-z][a-z0-9_-]{0,31}", re.IGNORECASE | re.ASCII)
_CREDENTIAL = re.compile(r"(?:en_[a-zA-Z0-9_-]{22}|ag_[a-zA-Z0-9_-]{43})")
_REASONS = {
    "ready", "idle", "busy", "not_sampled", "no_sample", "stale", "unavailable",
    "blocked_by_fifo", "resource_busy", "waiting_for_other_queue", "queue_worker_locked",
    "interrupted_job_needs_operator_review", "load_telemetry_unavailable",
    "worker_error", "gpu_binding_unavailable", "gpu_mapping_stale", "gpu_mapping_ambiguous",
    "reserved_by_a_running_job", "gpu_identity_unverified", "gpu_identity_stale", "gpu_identity_ambiguous",
    "gpu_provider_not_configured", "gpu_monitor_disabled", "gpu_telemetry_unavailable",
    "gpu_mask_verification_failed", "no_target_engine_sample", "invalid_percentage",
}
_STATUS = {"queued", "starting", "running", "cancelling", "succeeded", "failed", "killed",
           "cancelled", "launch_failed", "interrupted", "timeout", "timed_out"}
_RECOVERY = re.compile(r"(?:(?:cpu|gpu)_)?recovering_[0-9]{1,3}_of_[0-9]{1,3}")
_UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")


def _objects(value, limit):
    return [item for item in value[:limit] if isinstance(item, dict)] if isinstance(value, list) else []


def _name(value):
    if isinstance(value, str) and _QUEUE.fullmatch(value) and not _CREDENTIAL.search(value):
        return value
    return None


def _receipt(value):
    if isinstance(value, str) and _UUID.fullmatch(value) and uuid.UUID(value).int:
        return value
    return None


def _date(value):
    if not isinstance(value, str) or not 20 <= len(value) <= 35:
        return None
    try:
        date = datetime.fromisoformat(value)
        if date.tzinfo is not None:
            return date.astimezone(timezone.utc).isoformat(timespec="seconds")
    except (ValueError, OverflowError):
        pass
    return None


def _reason(value):
    if not isinstance(value, str) or not 0 < len(value) <= 128:
        return "unknown"
    parts = value.lower().replace(" ", "_").split(",")
    if len(parts) > 4:
        return "unknown"
    for part in parts:
        short = part[4:] if part.startswith(("cpu_", "gpu_")) else part
        if part not in _REASONS and short not in _REASONS and not _RECOVERY.fullmatch(part):
            return "unknown"
    return ",".join(parts)


def _count(value):
    return value if type(value) is int and 0 <= value <= 2**31 - 1 else None


def _metric(obj):
    obj = obj if isinstance(obj, dict) else {}
    pct = obj.get("percent")
    pct = round(float(pct), 1) if type(pct) in (int, float) and 0 <= pct <= 100 else None
    stamp = _date(obj.get("observed_at"))
    # A percentage without a credible observation time is not current telemetry.
    if stamp is None:
        pct = None
    return {"percent": pct, "observed_at": stamp, "reason": _reason(obj.get("reason"))}


def _job(obj, stamp_key):
    identity = _receipt(obj.get("id"))
    if identity is None:
        return None
    status = obj.get("status")
    result = {
        "id": identity,
        "queues": [x for x in (_name(n) for n in obj.get("queues", [])[:8]) if x is not None]
        if isinstance(obj.get("queues"), list) else [],
        "status": status if isinstance(status, str) and status in _STATUS else "unknown",
        stamp_key: _date(obj.get(stamp_key)),
    }
    if stamp_key == "started_at":
        phase = obj.get("phase")
        result["phase"] = phase if isinstance(phase, str) and phase in {"launching", "executing", "draining"} else None
    else:
        result["never_started"] = obj.get("never_started") is True
    return result


def sanitize(raw):
    """Convert a validated provider snapshot into a deliberately narrow browser schema."""
    if not isinstance(raw, dict) or not any(k in raw for k in ("queues", "active", "recent", "telemetry")):
        raise ValueError("invalid snapshot")
    queues = []
    for item in _objects(raw.get("queues"), 32):
        name = _name(item.get("name"))
        if name is not None:
            held = item.get("held")
            queues.append({
                "name": name, "queued": _count(item.get("queued")),
                "active": _count(item.get("active")),
                "held": held if type(held) is bool else _count(held),
                "hold_reason": _reason(item.get("hold_reason")),
                "oldest_queued_at": _date(item.get("oldest_queued_at")),
                "hold_since": _date(item.get("hold_since")),
            })
    telemetry = raw.get("telemetry")
    telemetry = telemetry if isinstance(telemetry, dict) else {}
    gpu = []
    for item in _objects(telemetry.get("gpu"), 32):
        name = _name(item.get("queue"))
        if name is not None:
            gpu.append({"queue": name, **_metric(item)})
    return {
        "queues": queues,
        "active": [j for j in (_job(o, "started_at") for o in _objects(raw.get("active"), 32)) if j],
        "recent": [j for j in (_job(o, "finished_at") for o in _objects(raw.get("recent"), 32)) if j],
        "telemetry": {"cpu": _metric(telemetry.get("cpu")), "gpu": gpu},
    }


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    request_queue_size = 16

    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.token = secrets.token_urlsafe(32)
        self.slots = threading.BoundedSemaphore(8)
        self.assets = {
            route: ((Path(__file__).parent / "assets" / filename).read_bytes(), content_type)
            for route, (filename, content_type) in _ASSETS.items()
        }
        super().__init__(("127.0.0.1", 0), _Handler)
        self.expected_host = f"127.0.0.1:{self.server_port}"
        self.expected_origin = "http://" + self.expected_host

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(4)
        return sock, address

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def handle_error(self, request, client_address):
        # Never log URLs, auth header values, or provider exceptions.
        pass


class _Handler(BaseHTTPRequestHandler):
    server_version = "Luigi"
    sys_version = ""

    def log_message(self, *args):
        pass

    def _reply(self, status, value=b"", content_type="text/plain; charset=utf-8"):
        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(value)))
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Content-Security-Policy", _CSP)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Connection", "close")
        self.end_headers()
        if value and self.command != "HEAD":
            self.wfile.write(value)

    def send_error(self, code, message=None, explain=None):
        # Base parser errors should also avoid reflecting path or request data.
        self._reply(code, b"Request rejected.\n")

    def _valid_context(self):
        headers = self.headers
        origin = headers.get_all("Origin") or []
        return (
            self.client_address[0] == "127.0.0.1"
            and headers.get_all("Host") == [self.server.expected_host]
            and (not origin or origin == [self.server.expected_origin])
            and headers.get("Sec-Fetch-Site") not in ("cross-site", "same-site")
            and not headers.get_all("Transfer-Encoding")
            and (headers.get_all("Content-Length") or []) in ([], ["0"])
        )

    def _authorized(self):
        values = self.headers.get_all("Authorization") or []
        if len(values) != 1:
            return False
        value = values[0]
        if not isinstance(value, str) or not value.isascii() or len(value) != 50:
            return False
        return hmac.compare_digest(value, "Bearer " + self.server.token)

    def do_GET(self):
        if not self._valid_context():
            return self._reply(403, b"Forbidden.\n")
        if self.path in self.server.assets:
            body, content_type = self.server.assets[self.path]
            return self._reply(200, body, content_type)
        if self.path != "/api/snapshot":
            return self._reply(404, b"Not found.\n")
        if not self._authorized():
            return self._reply(401, b"Unauthorized.\n")
        try:
            value = sanitize(self.server.snapshot())
            body = json.dumps(value, ensure_ascii=True, allow_nan=False,
                              separators=(",", ":")).encode("ascii")
        except Exception:
            return self._reply(503, b'{"error":"disconnected"}', "application/json; charset=utf-8")
        return self._reply(200, body, "application/json; charset=utf-8")

    def _deny(self):
        if not self._valid_context():
            return self._reply(403, b"Forbidden.\n")
        return self._reply(405, b"Read only.\n")

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_HEAD = _deny


@dataclass(frozen=True)
class Session:
    # Exclude the capability from repr/logging. The launcher decides how to display it.
    url: str = field(repr=False)
    origin: str
    _server: _Server = field(repr=False)

    def close(self):
        self._server.shutdown()
        self._server.server_close()


def open_dashboard(snapshot):
    """Start a session. Snapshot callback must already be read-only and time bounded."""
    if not callable(snapshot):
        raise TypeError("snapshot provider must be callable")
    server = _Server(snapshot)
    try:
        thread = threading.Thread(target=server.serve_forever, daemon=True, name="luigi-dashboard")
        thread.start()
    except BaseException:
        server.server_close()
        raise
    return Session(server.expected_origin + "/#" + server.token, server.expected_origin, server)
