import os
from pathlib import Path
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from luigi_app.core.broker import Broker
import luigi_app.cli as luigi
import luigi_app.platforms.transport as transport

PASS = "synthetic linux admin fixture"


@unittest.skipUnless(sys.platform == "linux" and os.geteuid() != 0, "Linux non-root local runner")
class UnixBrokerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="luigi-linux-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "s"
        transport.private_root(self.root, initialize=True)
        self.path = self.root / "broker.sock"

    def need_unix(self):
        try:
            with socket.socket(socket.AF_UNIX):
                return
        except PermissionError:
            self.skipTest("runner policy disallows AF_UNIX")

    def test_root_is_rejected(self):
        from unittest.mock import patch
        with patch("os.geteuid", return_value=0):
            with self.assertRaisesRegex(ValueError, "regular, non-setuid"):
                transport.require_non_elevated()

    def test_live_and_stale_cleanup_never_unlinks_regular_file(self):
        self.need_unix()
        self.path.touch(mode=0o600)
        with self.assertRaises(ValueError):
            transport.listener(self.root)
        self.assertTrue(self.path.is_file())
        self.path.unlink()
        with socket.socket(socket.AF_UNIX) as abandoned:
            abandoned.bind(str(self.path))
            self.path.chmod(0o600)
        listener = transport.listener(self.root)
        try:
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
            with self.assertRaises(ValueError):
                transport.listener(self.root)
            self.assertTrue(self.path.is_socket())
        finally:
            listener.close()
        self.assertFalse(self.path.exists())

    def test_private_root_and_key_reject_link_or_permissions(self):
        public = self.root / "public"
        public.mkdir(mode=0o755)
        with self.assertRaises(ValueError):
            transport.private_root(public)
        link = self.root / "link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            transport.private_root(link)
        secret = self.root / "broker.identity"
        transport.identity(self.root, initialize=True)
        secret.chmod(0o644)
        with self.assertRaises(ValueError):
            transport.identity(self.root)
        secret.unlink()
        secret.symlink_to("/dev/null")
        with self.assertRaises(OSError):
            transport.identity(self.root)

    def test_partial_frame_respects_deadline_then_serves_cli_and_dashboard_snapshot(self):
        self.need_unix()
        Broker(self.root).auth.bootstrap_admin(PASS)
        transport.identity(self.root, initialize=True)
        stop = threading.Event()
        t = threading.Thread(target=luigi.serve, args=(self.root, None),
                             kwargs={"stop":stop, "prelaunch":lambda q:(True,"ready")})
        t.start()
        try:
            deadline = time.monotonic() + 5
            while not self.path.exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.025)
            with socket.socket(socket.AF_UNIX) as stalled:
                stalled.connect(str(self.path))
                stalled.sendall(struct.pack("!I", 32) + b"x")
                stalled.settimeout(3)
                self.assertFalse(stalled.recv(1))
            luigi.request(self.root, "create", PASS, lane="cpu")
            invite = luigi.request(self.root, "invite", PASS)["invitation"]
            token = luigi.request(self.root, "enroll", invite)["bearer"]
            f = self.root / "synthetic.token"
            fd = os.open(f, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as out:
                out.write(token)
            # Path to root shim also works from a completely unrelated cwd.
            entry = Path(__file__).resolve().parents[1] / "luigi.py"
            result = subprocess.check_output([sys.executable, str(entry), "--root", str(self.root),
                                              "list", "--token-file", str(f)], cwd=self.tmp.name, text=True)
            self.assertIn('"jobs": []', result)
            self.assertEqual(luigi.request(self.root, "snapshot", PASS)["queues"][0]["name"], "cpu")
        finally:
            stop.set()
            t.join(8)
        self.assertFalse(t.is_alive())
        self.assertFalse(self.path.exists())


class FrameTests(unittest.TestCase):
    def test_partial_payload_and_oversize_are_bounded(self):
        class Socket:
            def __init__(self, chunks):
                self.chunks, self.timeout = chunks, []
            def settimeout(self, value):
                self.timeout.append(value)
            def recv(self, size):
                # Model partial frame reads and ensure allocation is capped.
                return self.chunks.pop(0)
        def frame(chunks, limit):
            inner = Socket(chunks)
            conn = transport._UnixConnection(inner)
            conn.deadline = time.monotonic() + 1
            result = conn.recv_bytes(limit)
            self.assertTrue(all(0 < timeout <= 1 for timeout in inner.timeout))
            return result
        self.assertEqual(frame([b"\0", b"\0\0\3", b"a", b"bc"], 32), b"abc")
        with self.assertRaisesRegex(ValueError, "oversize"):
            frame([struct.pack("!I", 65536)], 32)
        with self.assertRaises(EOFError):
            frame([struct.pack("!I", 4), b"a", b""], 32)
