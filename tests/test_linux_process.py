import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

import luigi_app.core.gpufifo as fifo
@unittest.skipUnless(sys.platform == "linux", "Linux process lifecycle")
class LinuxLifecycle(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.script = str(Path(fifo.__file__).resolve())
        self.child_marker = self.root / "child"
        self.done_marker = self.root / "finished"
        self.next_marker = self.root / "next"

    def insert(self, payload, timeout=None):
        with fifo.db(self.root) as db:
            columns = {r[1] for r in db.execute("PRAGMA table_info(jobs)")}
            if "timeout" not in columns:
                for name, datatype in (("timeout", "REAL"), ("runtime_phase", "TEXT"), ("runtime_deadline", "TEXT")):
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {datatype}")
            return db.execute("INSERT INTO jobs(receipt,argv,cwd,submitted,lane,timeout) VALUES(?,?,?,?,?,?)",
                              (fifo.uuid.uuid4().hex, json.dumps([sys.executable, "-c", payload]),
                               str(self.root), fifo.now(), "gpu", timeout)).lastrowid

    def spawn(self):
        proc = subprocess.Popen([sys.executable, self.script, "--root", str(self.root), "worker", "--drain"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self.stop, proc)
        return proc

    @staticmethod
    def stop(proc):
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=7)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def until(self, predicate):
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.03)
        self.fail("condition did not become true")

    def payload(self, *, delay, ignore_term=False, outlive_root=True, escape_group=False):
        child = "import os, signal, time; from pathlib import Path; "
        if ignore_term:
            child += "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        child += (f"Path({str(self.child_marker)!r}).write_text(str(os.getpid())); "
                  f"time.sleep({delay}); Path({str(self.done_marker)!r}).touch()")
        return (f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}], start_new_session={escape_group!r}); "
                + ("" if outlive_root else f"time.sleep({delay})"))

    def second(self):
        return self.insert(f"from pathlib import Path; Path({str(self.next_marker)!r}).touch()")

    def assert_clean(self, worker, job, state):
        self.assertEqual(worker.wait(timeout=8), 0)
        self.assertEqual(fifo.get_job(self.root, job)["status"], state)
        child = int(self.child_marker.read_text())
        self.assertFalse(Path(f"/proc/{child}").exists())

    def test_descendants_drain_before_release(self):
        active = self.insert(self.payload(delay=1.0), timeout=5)
        later = self.insert(f"from pathlib import Path; assert Path({str(self.done_marker)!r}).exists()")
        worker = self.spawn()
        self.until(self.child_marker.exists)
        self.until(lambda: fifo.get_job(self.root, active)["runtime_phase"] == "draining")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "queued")
        self.assert_clean(worker, active, "succeeded")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "succeeded")

    def test_timeout_while_draining_kills_group(self):
        active = self.insert(self.payload(delay=8, ignore_term=True), timeout=1)
        later = self.second()
        worker = self.spawn()
        self.until(self.child_marker.exists)
        self.assert_clean(worker, active, "timed_out")
        self.assertFalse(self.done_marker.exists())
        self.assertEqual(fifo.get_job(self.root, later)["status"], "succeeded")

    def test_kill_running_group_and_continue_queue(self):
        active = self.insert(self.payload(delay=8, ignore_term=True, outlive_root=False))
        later = self.second()
        worker = self.spawn()
        self.until(self.child_marker.exists)
        command = subprocess.run([sys.executable, self.script, "--root", str(self.root), "kill", str(active)],
                                 capture_output=True, text=True, timeout=5)
        self.assertEqual(command.returncode, 0, command.stderr)
        self.assert_clean(worker, active, "killed")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "succeeded")

    def test_ctrl_c_stops_group_and_leaves_next_job_queued(self):
        active = self.insert(self.payload(delay=8, ignore_term=True))
        later = self.second()
        worker = self.spawn()
        self.until(self.child_marker.exists)
        worker.send_signal(signal.SIGINT)
        self.assert_clean(worker, active, "killed")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "queued")

    def test_setsid_survives_group_cancellation_but_holds_the_queue(self):
        active = self.insert(self.payload(delay=1.8, escape_group=True), timeout=1)
        later = self.second()
        worker = self.spawn()
        self.until(self.child_marker.exists)
        time.sleep(1.15)
        self.assertEqual(fifo.get_job(self.root, active)["status"], "running")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "queued")
        self.assert_clean(worker, active, "timed_out")
        self.assertTrue(self.done_marker.exists())
        self.assertEqual(fifo.get_job(self.root, later)["status"], "succeeded")

    def test_worker_crash_cleans_group_but_preserves_stale_reservation(self):
        active = self.insert(self.payload(delay=8, ignore_term=True, outlive_root=False))
        later = self.second()
        worker = self.spawn()
        self.until(self.child_marker.exists)
        child = int(self.child_marker.read_text())
        worker.kill()
        worker.wait(timeout=5)
        self.until(lambda: not Path(f"/proc/{child}").exists())
        successor = self.spawn()
        self.assertEqual(successor.wait(timeout=8), 2)
        self.assertEqual(fifo.get_job(self.root, active)["status"], "running")
        self.assertEqual(fifo.get_job(self.root, later)["status"], "queued")


if __name__ == "__main__":
    unittest.main()
