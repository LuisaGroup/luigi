import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from luigi_app.core.broker import Broker, decode, encode, MAX_FRAME, dispatcher, claim

TEST_PASSWORD = "local harness fake admin password"


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "spool"
        self.broker = Broker(self.root)
        self.broker.auth.bootstrap_admin(TEST_PASSWORD)
        self.a = self.agent()
        self.b = self.agent()

    def req(self, op, secret, **data):
        return decode(self.broker.handle(encode({"op":op,"credential":secret,"data":data})))

    def yes(self, op, secret, **data):
        response = self.req(op, secret, **data)
        self.assertTrue(response["ok"], response)
        return response["result"]

    def no(self, op, secret, **data):
        response = self.req(op, secret, **data)
        self.assertFalse(response["ok"], response)

    def agent(self):
        invite = self.yes("invite", TEST_PASSWORD, ttl=90)["invitation"]
        enrollment = self.yes("enroll", invite)
        self.no("enroll", invite)
        return enrollment

    def submit(self, credential, code="print('synthetic')", lane="gpu"):
        return self.yes("submit", credential, lane=lane,
                        argv=[str(Path(sys.executable).resolve()), "-c", code])["receipt"]

    def test_role_denials_and_owner_mutations(self):
        a, b = self.a["bearer"], self.b["bearer"]
        self.assertEqual(self.yes("queues", TEST_PASSWORD)["queues"], [])
        self.no("create", a, lane="gpu")
        self.no("reset", a, lane="gpu")
        self.no("backup", a)
        self.no("revoke", a, agent=self.b["agent"])
        self.no("submit", a, lane="gpu", argv=[sys.executable])
        self.yes("create", TEST_PASSWORD, lane="gpu")
        j = self.submit(a)
        self.assertEqual(str(uuid.UUID(j, version=4)), j)
        for op in ("kill", "cancel", "status", "wait", "log", "replay"):
            self.assertEqual(self.req(op, b, receipt=j)["error"], "not authorized")
        self.assertEqual(self.yes("list", b)["jobs"], [])
        self.no("status", TEST_PASSWORD, receipt=j)
        self.assertEqual(self.yes("status", a, receipt=j)["status"], "queued")
        self.yes("cancel", a, receipt=j)
        duplicate = self.yes("replay", a, receipt=j)["receipt"]
        self.assertNotEqual(j, duplicate)
        self.assertEqual(self.yes("status", a, receipt=duplicate)["replay_of_receipt"], j)
        self.assertEqual(self.yes("reset", TEST_PASSWORD, lane="gpu")["cancelled"], 1)
        backup_id = self.yes("backup", TEST_PASSWORD)["backup_id"]
        snapshot = (self.root / "backups" / backup_id / "snapshot.json").read_text()
        for secret in (a, b, TEST_PASSWORD):
            self.assertNotIn(secret, snapshot)
        self.assertEqual(json.loads(snapshot)["queues"], [{"name":"gpu","kind":"gpu","gpu_uuid":None,"gpu_provider":None}])

    def test_rotation_and_revocation(self):
        first = self.a["bearer"]
        second = self.yes("rotate", first)["bearer"]
        self.no("list", first)
        self.yes("list", second)
        self.yes("revoke", TEST_PASSWORD, agent=self.a["agent"])
        self.no("list", second)

    def test_strict_wire_and_secret_absence(self):
        for value in [b'x' * (MAX_FRAME+1), b'{"op":"x","op":"y"}',
                      b'{"a":NaN}', b'null', b'not pickle']:
            self.assertFalse(decode(self.broker.handle(value))["ok"])
        a = self.a["bearer"]
        self.yes("create", TEST_PASSWORD, lane="gpu")
        self.no("submit", a, lane="gpu", argv=[str(Path(sys.executable).resolve()), a])
        before = self.broker.handle(b'{"op":"whatever","credential":"raw","data":{}}')
        self.assertNotIn(b"raw", before)

    def test_guard_leaves_receipt_queued_and_preserves_fifo(self):
        a = self.a["bearer"]
        self.yes("create", TEST_PASSWORD, lane="gpu")
        first = self.submit(a, "import os; assert 'LUIGI_TEST_SECRET' not in os.environ; print('first')")
        second = self.submit(a, "print('second')")
        stop, green = threading.Event(), threading.Event()
        seen = []
        def prelaunch(lane):
            seen.append(lane["name"])
            return green.is_set(), "GPU busy" if not green.is_set() else "idle"
        worker = threading.Thread(target=dispatcher, args=(self.broker, stop, prelaunch))
        def until(fn):
            end = time.monotonic() + 12
            while time.monotonic() < end:
                if fn():
                    return
                time.sleep(.03)
            self.fail("worker did not reach expected state")
        with patch.dict(os.environ, {"LUIGI_TEST_SECRET": a}):
            worker.start()
            try:
                until(lambda: self.broker.hold("gpu")["reason"] == "GPU busy")
                for receipt in (first, second):
                    self.assertEqual(self.yes("status", a, receipt=receipt)["status"], "queued")
                green.set()
                until(lambda: self.yes("status", a, receipt=second)["status"] == "succeeded")
                self.assertEqual(self.yes("status", a, receipt=first)["status"], "succeeded")
                self.assertEqual(self.yes("log", a, receipt=first)["text"].strip(), "first")
                self.assertEqual(self.yes("log", a, receipt=second)["text"].strip(), "second")
            finally:
                stop.set()
                worker.join(15)
        self.assertTrue(seen)


    def test_multiqueue_atomic_heads_reset_recover_replay_backup(self):
        import luigi_app.core.gpufifo as fifo
        a = self.a["bearer"]
        program = [str(Path(sys.executable).resolve()), "-c", "pass"]
        for name in ("cpu", "gpu", "aux"):
            self.yes("create", TEST_PASSWORD, lane=name, kind="gpu" if name == "gpu" else "cpu")
        def submit(*names, timeout=1800):
            return self.yes("submit", a, queues=list(names), argv=program, timeout=timeout)["receipt"]
        self.no("submit", a, queues=["cpu", "missing"], argv=program)
        self.no("submit", a, queues=["gpu", "gpu"], argv=program)
        for invalid in (0, -1, 86401, True, "NaN"):
            self.no("submit", a, queues=["cpu"], argv=program, timeout=invalid)
        first, second, third, fourth = submit("cpu", "gpu", timeout=1), submit("cpu"), submit("gpu"), submit("aux")
        stop = threading.Event()
        held = lambda q: (q["kind"] != "gpu", "GPU gate closed")
        auxiliary = claim(self.broker, held, stop)
        self.assertEqual(auxiliary["receipt"], fourth)
        self.assertIsNone(claim(self.broker, held, stop))
        pending = self.yes("status", a, receipt=first)
        self.assertIsNone(pending["runtime_deadline"])
        self.assertEqual(pending["timeout"], 1)
        green = lambda _: (True, "available")
        joint = claim(self.broker, green, stop)
        self.assertEqual(joint["receipt"], first)
        self.assertEqual(joint["queues"], ["cpu", "gpu"])
        self.assertIsNone(claim(self.broker, green, stop))
        self.no("recover", TEST_PASSWORD, receipt=first, confirmed_stopped=True)
        self.no("backup", TEST_PASSWORD)
        # Restart has no in-memory workers; durable running memberships still block.
        reopened = Broker(self.root)
        self.assertIsNone(claim(reopened, green, stop))
        reopened.dispatch({"op":"recover","credential":TEST_PASSWORD,"data":{"receipt":first,"confirmed_stopped":True}})
        self.assertEqual(claim(reopened, green, stop)["receipt"], second)
        self.assertEqual(claim(reopened, green, stop)["receipt"], third)
        replay = self.yes("replay", a, receipt=first)["receipt"]
        self.assertEqual(self.yes("status", a, receipt=replay)["queues"], ["cpu", "gpu"])
        self.assertEqual(self.yes("status", a, receipt=replay)["timeout"], 1)
        self.assertEqual(self.yes("reset", TEST_PASSWORD, lane="gpu")["cancelled"], 1)
        self.assertEqual(self.yes("status", a, receipt=replay)["status"], "cancelled")
        for receipt in (second, third, fourth):
            fifo.finish(self.root, fifo.get_job(self.root, receipt)["id"], "succeeded")
        export = self.yes("backup", TEST_PASSWORD)["backup_id"]
        data = json.loads((self.root / "backups" / export / "snapshot.json").read_text())
        self.assertEqual(data["jobs"][0]["queues"], ["cpu", "gpu"])

    def test_cpu_head_waits_behind_older_gpu_job_while_unrelated_queue_progresses(self):
        a = self.a["bearer"]
        program = [str(Path(sys.executable).resolve()), "-c", "pass"]
        for name in ("cpu", "gpu", "other"):
            self.yes("create", TEST_PASSWORD, lane=name, kind="gpu" if name == "gpu" else "cpu")
        def submit(*names):
            return self.yes("submit", a, queues=list(names), argv=program)["receipt"]
        older, joint, later, unrelated = submit("gpu"), submit("gpu", "cpu"), submit("cpu"), submit("other")
        green, stop = lambda _: (True, "available"), threading.Event()
        self.assertEqual(claim(self.broker, green, stop)["receipt"], older)
        self.assertEqual(claim(self.broker, green, stop)["receipt"], unrelated)
        self.assertIsNone(claim(self.broker, green, stop))
        self.assertEqual(self.yes("status", a, receipt=later)["holds"]["cpu"]["reason"],
                         "older job at queue head")
        import luigi_app.core.gpufifo as fifo
        fifo.finish(self.root, fifo.get_job(self.root, older)["id"], "succeeded")
        self.assertEqual(claim(self.broker, green, stop)["receipt"], joint)
        self.assertIsNone(claim(self.broker, green, stop))

    def test_concurrent_submissions_preserve_order_on_every_shared_queue(self):
        import concurrent.futures
        import luigi_app.core.gpufifo as fifo
        a = self.a["bearer"]
        program = [str(Path(sys.executable).resolve()), "-c", "pass"]
        names = ["red", "blue", "green"]
        for name in names:
            self.yes("create", TEST_PASSWORD, lane=name, kind="cpu")
        requests = [names[k % 3:][:1] + ([names[(k+1) % 3]] if k % 2 else [])
                    for k in range(24)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            receipts = list(pool.map(lambda wanted: self.yes(
                "submit", a, queues=wanted, argv=program)["receipt"], requests))
        self.assertEqual(len(set(receipts)), 24)
        green, stop = lambda _: (True, "available"), threading.Event()
        order = {name: [] for name in names}
        for _ in range(24):
            row = claim(self.broker, green, stop)
            self.assertIsNotNone(row)
            for name in row["queues"]:
                order[name].append(row["id"])
            fifo.finish(self.root, row["id"], "succeeded")
        self.assertTrue(all(seq == sorted(seq) for seq in order.values()))

    def test_gpu_uuid_and_unbound_ambiguity(self):
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        self.yes("create", TEST_PASSWORD, lane="g-one", kind="gpu", gpu_uuid=id1)
        self.yes("create", TEST_PASSWORD, lane="g-two", kind="gpu", gpu_uuid=id2)
        self.no("create", TEST_PASSWORD, lane="clone", kind="gpu", gpu_uuid=id1.upper())
        self.no("create", TEST_PASSWORD, lane="gpu")
        self.no("create", TEST_PASSWORD, lane="cpu", gpu_uuid=id2)
        a = self.a["bearer"]
        program = [str(Path(sys.executable).resolve()), "-c", "pass"]
        self.no("submit", a, queues=["g-one", "g-two"], argv=program)
        first = self.yes("submit", a, queues=["g-one"], argv=program)["receipt"]
        second = self.yes("submit", a, queues=["g-two"], argv=program)["receipt"]
        stop = threading.Event()
        self.assertIsNone(claim(self.broker, lambda q: (False, "unmapped UUID"), stop))
        # Synthetic policy only: demonstrate independent queues with verified test resources.
        policy = lambda q: (q["gpu_uuid"] in (id1,id2), "synthetic resolved")
        self.assertEqual(claim(self.broker, policy, stop)["receipt"], first)
        self.assertEqual(claim(self.broker, policy, stop)["receipt"], second)

    @unittest.skipUnless(sys.platform == "win32", "Windows Job Object runtime timeout")
    def test_win_timeout_does_not_count_queue_wait_and_stops_children(self):
        a = self.a["bearer"]
        self.yes("create", TEST_PASSWORD, lane="gpu")
        self.yes("create", TEST_PASSWORD, lane="cpu")
        marker = Path(self.temp.name) / "late-child"
        child = "import time,pathlib,sys;time.sleep(3);pathlib.Path(sys.argv[1]).touch()"
        code = ("import subprocess,sys;subprocess.Popen([sys.executable,'-c',"
                + repr(child) + "," + repr(str(marker)) + "]);print('parent-exited')")
        receipt = self.yes("submit", a, queues=["cpu","gpu"], timeout=1,
                           argv=[str(Path(sys.executable).resolve()), "-c", code])["receipt"]
        stop, green = threading.Event(), threading.Event()
        t = threading.Thread(target=dispatcher, args=(self.broker, stop, lambda _: (green.is_set(), "test")))
        t.start()
        try:
            time.sleep(1.2)
            queued = self.yes("status", a, receipt=receipt)
            self.assertEqual(queued["status"], "queued")
            self.assertIsNone(queued["runtime_deadline"])
            green.set()
            deadline = time.monotonic() + 15
            while True:
                result = self.yes("status", a, receipt=receipt)
                if result["status"] not in {"queued", "running"}:
                    break
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.05)
            self.assertEqual(result["status"], "timed_out")
            self.assertIsNotNone(result["runtime_deadline"])
            self.assertEqual(self.yes("log", a, receipt=receipt)["text"].strip(), "parent-exited")
            time.sleep(2.1)
            self.assertFalse(marker.exists())
        finally:
            stop.set()
            t.join(15)

    def test_operator_snapshot_is_authenticated_and_redacted(self):
        a = self.a["bearer"]
        self.yes("create", TEST_PASSWORD, lane="cpu")
        marker = "SYNTHETIC-ARGUMENT-DO-NOT-DISPLAY"
        receipt = self.submit(a, repr(marker), lane="cpu")
        self.no("snapshot", a)
        doc = self.yes("snapshot", TEST_PASSWORD)
        self.assertEqual(doc["queues"][0]["queued"], 1)
        self.assertEqual(set(doc), {"queues","active","recent","telemetry"})
        import luigi_app.core.gpufifo as fifo
        row = claim(self.broker, lambda q: (True, "ready"), threading.Event())
        self.assertEqual(row["receipt"], receipt)
        doc = self.yes("snapshot", TEST_PASSWORD)
        self.assertEqual(doc["active"][0]["id"], receipt)
        self.assertEqual(doc["active"][0]["phase"], "launching")
        self.assertNotIn(marker, json.dumps(doc))
        self.assertNotIn(str(self.root), json.dumps(doc))
        self.assertNotIn(a, json.dumps(doc))
        fifo.finish(self.root, row["id"], "succeeded")

    def test_fake_server_never_receives_credential(self):
        import luigi_app.platforms.transport as transport
        class Impostor:
            def __init__(self):
                self.sent = []
            def send_bytes(self, payload):
                self.sent.append(payload)
            def poll(self, timeout):
                return True
            def recv_bytes(self, length):
                return b"X" * length
        fake = Impostor()
        with self.assertRaisesRegex(ValueError, "identity verification failed"):
            transport.exchange(fake, b"SYNTHETIC SECRET", b"K" * 32)
        self.assertEqual(len(fake.sent), 1)
        self.assertEqual(len(fake.sent[0]), 32)

    @unittest.skipUnless(sys.platform == "win32", "Windows paths")
    def test_windows_rejects_remote_or_device_before_component_io(self):
        from unittest.mock import patch
        import luigi_app.core.gpufifo as fifo
        with patch.object(Path, "lstat", side_effect=AssertionError("did caller path I/O")):
            for path in (r"\\localhost\C$\windows\notepad.exe",
                         r"\\?\C:\windows\notepad.exe",
                         r"\\.\C:\windows\notepad.exe",
                         r"C:\windows\..\notepad.exe",
                         r"C:\windows\notepad.exe:stream",
                         r"C:\windows\NUL.exe"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    fifo.validate_local_path(path)

    @unittest.skipUnless(sys.platform == "win32", "native Windows pipe required")
    def test_win_pipe_raw_json_admin_then_agent(self):
        import luigi_app.platforms.transport as transport
        key = transport.identity(self.root, initialize=True)
        listener = transport.windows_listener(self.root)
        # One bounded request per connection; synthetic credentials only.
        result = []
        def server():
            try:
                for _ in range(3):
                    transport.reply_one(listener.accept(), self.broker.handle, key)
            except Exception as exc:
                result.append(type(exc).__name__)
        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        try:
            def ask(op, secret, **data):
                return decode(transport.client(self.root, encode(
                    {"op":op,"credential":secret,"data":data})))
            self.assertFalse(ask("create", self.a["bearer"], lane="gpu")["ok"])
            self.assertTrue(ask("create", TEST_PASSWORD, lane="gpu")["ok"])
            self.assertTrue(ask("submit", self.a["bearer"], lane="gpu",
                                argv=[str(Path(sys.executable).resolve()), "-c", "print('synthetic')"])["ok"])
            thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result, [])
        finally:
            listener.close()

    @unittest.skipUnless(sys.platform == "win32", "native Windows Job Object required")
    def test_win_kill_is_enforced_by_owner_while_running(self):
        secret = self.a["bearer"]
        self.yes("create", TEST_PASSWORD, lane="gpu")
        receipt = self.submit(secret, "import time; time.sleep(20)")
        stop = threading.Event()
        worker = threading.Thread(target=dispatcher, args=(
            self.broker, stop, lambda _: (True, "synthetic idle")))
        worker.start()
        try:
            deadline = time.monotonic() + 15
            while self.yes("status", secret, receipt=receipt)["status"] != "running":
                if time.monotonic() > deadline:
                    self.fail("synthetic job did not start")
                time.sleep(.05)
            self.no("kill", self.b["bearer"], receipt=receipt)
            self.no("kill", TEST_PASSWORD, receipt=receipt)
            self.yes("kill", secret, receipt=receipt)
            deadline = time.monotonic() + 15
            while self.yes("status", secret, receipt=receipt)["status"] != "killed":
                if time.monotonic() > deadline:
                    self.fail("owner kill did not reach terminal state")
                time.sleep(.05)
        finally:
            stop.set()
            worker.join(25)


if __name__ == "__main__":
    unittest.main()
