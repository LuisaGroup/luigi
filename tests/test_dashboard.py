import http.client
import json
import unittest
from urllib.parse import urlsplit

from luigi_app.dashboard.server import open_dashboard, sanitize


RECEIPT = "43215678-1234-4567-8765-123456789012"
SECRET = "my-private-credential-and-C:/work/secret"
DATE = "2026-11-20T08:05:00+08:00"


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.calls = 0
        self.error = False
        def snapshot():
            self.calls += 1
            if self.error:
                raise RuntimeError(SECRET)
            return {
                "queues": [{"name": "hip-rx", "queued": 3, "active": 1, "held": 3,
                            "hold_reason": "cpu_stale,gpu_busy", "path": SECRET}],
                "active": [{"id": RECEIPT, "queues": ["hip-rx"], "status": "running",
                            "started_at": DATE, "phase": "draining", "argv": [SECRET], "env": {"ADMIN": SECRET}}],
                "recent": [{"id": RECEIPT, "queues": ["hip-rx"], "status": "timed_out",
                            "finished_at": DATE}],
                "telemetry": {"cpu": {"percent": 40.2, "observed_at": DATE,
                                       "reason": "stale", "details": SECRET},
                              "gpu": [{"queue": "hip-rx", "percent": 0, "observed_at": DATE,
                                       "reason": "ready"}]},
                "raw_stdout": SECRET,
            }
        self.session = open_dashboard(snapshot)
        self.addCleanup(self.session.close)
        url = urlsplit(self.session.url)
        self.host, self.port = url.hostname, url.port
        self.token = url.fragment
        self.auth = {"Authorization": f"Bearer {self.token}"}

    def query(self, path, headers=None, method="GET"):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=3)
        try:
            conn.request(method, path, headers=headers or {})
            response = conn.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            conn.close()

    def test_only_authenticated_snapshot_invokes_provider(self):
        self.assertEqual(self.host, "127.0.0.1")
        self.assertNotIn(self.token, repr(self.session))
        for path, headers, method, status in (
            ("/api/snapshot", {}, "GET", 401),
            ("/api/snapshot", {"Authorization": "Bearer " + "a"*43}, "GET", 401),
            ("/api/snapshot", self.auth | {"Host": "attacker.example"}, "GET", 403),
            ("/api/snapshot", self.auth | {"Origin": "http://attacker.example"}, "GET", 403),
            ("/api/snapshot", self.auth | {"Sec-Fetch-Site": "cross-site"}, "GET", 403),
            ("/api/snapshot?token=" + self.token, {}, "GET", 404),
            ("/api/snapshot", self.auth, "POST", 405),
            ("/api/snapshot", self.auth, "OPTIONS", 405),
            ("/api/jobs/submit", self.auth, "POST", 405),
            ("/api/admin", self.auth, "GET", 404),
        ):
            with self.subTest(path=path, method=method, status=status):
                result, _, body = self.query(path, headers, method)
                self.assertEqual(result, status)
                self.assertNotIn(self.token.encode(), body)
        self.assertEqual(self.calls, 0)

    def test_static_assets_have_security_headers_and_no_session_secrets(self):
        for asset in ("/", "/dashboard.js", "/dashboard.css"):
            status, headers, body = self.query(asset)
            self.assertEqual(status, 200)
            self.assertIn("no-store", headers["Cache-Control"])
            self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
            self.assertEqual(headers["Referrer-Policy"], "no-referrer")
            self.assertEqual(headers["Cross-Origin-Resource-Policy"], "same-origin")
            self.assertNotIn("Access-Control-Allow-Origin", headers)
            self.assertNotIn(self.token.encode(), body)
            self.assertNotIn(SECRET.encode(), body)
            if asset.endswith(".js"):
                self.assertIn(b"textContent", body)
                self.assertNotIn(b"innerHTML", body)
                self.assertIn(b"history.replaceState", body)
        self.assertEqual(self.calls, 0)

    def test_authorized_snapshot_is_narrow_and_disconnection_is_generic(self):
        status, _, body = self.query("/api/snapshot", self.auth | {"Origin": self.session.origin})
        self.assertEqual(status, 200)
        self.assertEqual(self.calls, 1)
        self.assertNotIn(SECRET.encode(), body)
        self.assertNotIn(self.token.encode(), body)
        self.assertNotIn(b"argv", body)
        self.assertNotIn(b"env", body)
        data = json.loads(body)
        self.assertEqual(data["active"][0]["id"], RECEIPT)
        self.assertEqual(data["active"][0]["phase"], "draining")
        self.assertEqual(data["recent"][0]["status"], "timed_out")
        self.assertEqual(data["queues"][0]["hold_reason"], "cpu_stale,gpu_busy")
        self.assertEqual(data["telemetry"]["gpu"][0]["percent"], 0)
        self.assertEqual(data["telemetry"]["cpu"]["observed_at"], "2026-11-20T00:05:00+00:00")
        self.error = True
        status, _, body = self.query("/api/snapshot", self.auth)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "disconnected"})

    def test_sanitizer_denies_string_categories_and_untimed_measurements(self):
        section = sanitize({
            "queues": [{"name": "ok", "queued": True, "active": -3, "hold_reason": SECRET},
                       {"name": "en_" + "q"*22}, {"name": "C:/private"}],
            "active": [{"id": "bogus", "status": SECRET}],
            "telemetry": {"cpu": {"percent": 10, "observed_at": "not a date", "reason": SECRET}},
        })
        self.assertEqual(section["queues"], [{"name": "ok", "queued": None, "active": None,
                                              "held": None, "hold_reason": "unknown",
                                              "oldest_queued_at": None, "hold_since": None}])
        self.assertEqual(section["active"], [])
        self.assertEqual(section["telemetry"]["cpu"], {"percent": None, "observed_at": None, "reason": "unknown"})


if __name__ == "__main__":
    unittest.main()
