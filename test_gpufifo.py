import concurrent.futures
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

CLI = Path(__file__).resolve().with_name("gpufifo.py")


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "spool with spaces"
        self.base = [sys.executable, str(CLI), "--root", str(self.root)]

    def cli(self, *args, check=True):
        return subprocess.run([*self.base, *args], capture_output=True, text=True,
                              timeout=10, check=check)

    def submit(self, code, *args, queue="gpu"):
        receipt = self.cli("submit", "--queue", queue, "--", sys.executable, "-c", code, *args).stdout.strip()
        self.assertEqual(str(uuid.UUID(receipt, version=4)), receipt)
        return receipt

    def status(self, job_id):
        return json.loads(self.cli("status", str(job_id)).stdout)

    def worker(self, *args):
        p = subprocess.Popen([*self.base, "worker", *args], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        self.addCleanup(self.stop_worker, p)
        return p

    @staticmethod
    def stop_worker(p):
        if p.poll() is None:
            p.kill()
        p.wait(timeout=5)

    def until(self, fn, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if result := fn():
                return result
            time.sleep(.04)
        self.fail("condition did not become true before timeout")

    def test_concurrent_submitters_atomic_fifo_and_no_overlap(self):
        trace = Path(self.tmp.name) / "trace"
        code = ("import sys,time; from pathlib import Path; "
                "p=Path(sys.argv[1]); tag=sys.argv[2]; "
                "f=p.open('a'); print('start',tag,file=f,flush=True); time.sleep(.02); "
                "print('end',tag,file=f,flush=True); f.close()")
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            pairs = list(pool.map(lambda i: (self.submit(code, str(trace), str(i)), i), range(10)))
        self.assertEqual(len({j for j, _ in pairs}), 10)
        self.cli("worker", "--drain")
        chronological = sorted(pairs, key=lambda pair: self.status(pair[0])["id"])
        expected = [f"{action} {i}" for _, i in chronological for action in ("start", "end")]
        self.assertEqual(trace.read_text().splitlines(), expected)
        self.assertTrue(all(self.status(j)["status"] == "succeeded" for j, _ in pairs))

    def test_failure_log_literal_args_cwd_and_cancel(self):
        text = "literal [argument] with spaces & no shell"
        job = self.submit("import sys; print(sys.argv[1]); print('oops',file=sys.stderr); sys.exit(7)", text)
        after = self.cli("submit", "--cwd", self.tmp.name, "--", sys.executable,
                         "-c", "import os; print(os.getcwd())").stdout.strip()
        cancelled = self.submit("raise AssertionError('not expected to execute')")
        self.cli("cancel", str(cancelled))
        self.cli("worker", "--drain")
        self.assertEqual(self.status(job)["code"], 7)
        self.assertEqual(self.status(job)["status"], "failed")
        log = self.cli("log", str(job)).stdout
        self.assertIn(text, log)
        self.assertNotIn("oops", log)
        self.assertIn("oops", self.cli("log", job, "--stream", "stderr").stdout)
        self.assertTrue(Path(self.cli("log", str(after)).stdout.strip()).samefile(self.tmp.name))
        self.assertEqual(self.status(cancelled)["status"], "cancelled")

    def win_process_waiter(self, pid):
        import ctypes as C
        from ctypes import wintypes as W
        k = C.WinDLL("kernel32", use_last_error=True)
        k.OpenProcess.argtypes = [W.DWORD, W.BOOL, W.DWORD]
        k.OpenProcess.restype = W.HANDLE
        k.WaitForSingleObject.argtypes = [W.HANDLE, W.DWORD]
        k.WaitForSingleObject.restype = W.DWORD
        k.CloseHandle.argtypes = [W.HANDLE]
        k.CloseHandle.restype = W.BOOL
        h = k.OpenProcess(0x00100000, False, pid)
        if not h:
            raise C.WinError(C.get_last_error())
        self.addCleanup(k.CloseHandle, h)
        return lambda timeout: k.WaitForSingleObject(h, timeout)

    def test_summary_reset_backup_and_replay(self):
        one = self.submit("print(101)")
        self.cli("worker", "--drain")
        pending = self.submit("print(202)")
        self.assertEqual([r["receipt"] for r in json.loads(self.cli("status").stdout)["queued"]], [pending])
        self.assertEqual(self.cli("replay", str(pending), check=False).returncode, 2)
        copied = self.cli("replay", one).stdout.strip()
        self.assertEqual(str(uuid.UUID(copied, version=4)), copied)
        self.assertNotEqual(copied, one)
        self.assertGreater(self.status(copied)["id"], self.status(pending)["id"])
        self.assertEqual(self.status(copied)["replay_of_receipt"], one)
        self.assertEqual([r["receipt"] for r in json.loads(self.cli("status").stdout)["queued"]], [pending, copied])
        self.cli("reset")
        self.assertEqual(self.status(pending)["status"], "cancelled")
        self.assertEqual(self.status(copied)["status"], "cancelled")
        self.assertEqual(self.status(one)["status"], "succeeded")
        dest = Path(self.tmp.name) / "backup"
        source = Path(self.status(one)["task_dir"])
        (source / "work" / "scratch.bin").write_bytes(b"work")
        (source / "tmp" / "scratch.bin").write_bytes(b"temporary")
        self.cli("backup", str(dest))
        self.assertFalse((dest / ".incomplete").exists())
        copied_task = dest / "tasks" / source.name
        for name in ("job.json", "stdout.log", "stderr.log"):
            self.assertEqual((copied_task / name).read_bytes(), (source / name).read_bytes())
        self.assertFalse((copied_task / "work").exists())
        self.assertFalse((copied_task / "tmp").exists())
        cmd = [sys.executable, str(CLI), "--root", str(dest), "status"]
        backup = json.loads(subprocess.check_output(cmd, text=True, timeout=5))
        self.assertEqual(backup["counts"], {"cancelled": 2, "succeeded": 1})
        self.assertEqual(self.cli("backup", str(dest), check=False).returncode, 2)

    def test_task_isolation_and_replay(self):
        code = ("import json,os,sys; from pathlib import Path; "
                "print(json.dumps([os.getcwd()]+[os.environ[k] for k in "
                "('TEMP','TMP','TMPDIR','GPU_FIFO_TASK_DIR','GPU_FIFO_TASK_RECEIPT')])); "
                "print('stderr-only marker',file=sys.stderr); "
                "Path('artifact.bin').write_bytes(b'scratch')")
        a = self.submit(code)
        self.cli("worker", "--drain")
        self.assertEqual(self.cli("wait", a).returncode, 0)
        b = self.cli("replay", a).stdout.strip()
        self.cli("worker", "--drain")
        infos = [self.status(receipt) for receipt in (a,b)]
        self.assertNotEqual(infos[0]["task_dir"], infos[1]["task_dir"])
        for receipt, info in zip((a,b),infos):
            path = Path(info["task_dir"])
            stdout = self.cli("log", receipt).stdout
            cwd, temp, tmp, tmpdir, path_env, receipt_env = json.loads(stdout)
            self.assertTrue(Path(cwd).samefile(path / "work"))
            self.assertTrue(all(Path(x).samefile(path / "tmp") for x in (temp,tmp,tmpdir)))
            self.assertTrue(Path(path_env).samefile(path))
            self.assertEqual(receipt_env, receipt)
            self.assertEqual((path / "work" / "artifact.bin").read_bytes(), b"scratch")
            self.assertNotIn("stderr-only", stdout)
            self.assertEqual(self.cli("log", receipt, "--stream", "stderr").stdout.strip(), "stderr-only marker")
            record = json.loads((path / "job.json").read_text(encoding="utf-8"))
            self.assertEqual(record["receipt"], receipt)
            self.assertEqual(record["replay_of_receipt"], a if receipt == b else None)

    def test_named_lanes_run_independently_and_reset_is_scoped(self):
        gate = Path(self.tmp.name) / "release-lanes"
        active_code = ("import os,sys,time; from pathlib import Path; "
                       "print(os.environ['GPU_FIFO_QUEUE'], flush=True); "
                       "g=Path(sys.argv[1]); end=time.monotonic()+15; "
                       "exec('while not g.exists() and time.monotonic()<end:\\n time.sleep(.02)')")
        gpu = self.submit(active_code, str(gate))
        cpu = self.submit(active_code, str(gate), queue="cpu")
        next_gpu = self.submit("print('gpu successor')")
        next_cpu = self.submit("print('cpu successor')", queue="cpu")
        pg = self.worker("--queue", "gpu", "--drain")
        pc = self.worker("--queue", "cpu", "--drain")
        self.until(lambda: self.status(gpu)["pid"] and self.status(cpu)["pid"])
        summary = json.loads(self.cli("status").stdout)["queues"]
        self.assertEqual(summary["gpu"]["running"]["receipt"], gpu)
        self.assertEqual(summary["cpu"]["running"]["receipt"], cpu)
        self.assertEqual(summary["gpu"]["queued"][0]["receipt"], next_gpu)
        self.assertEqual(summary["cpu"]["queued"][0]["receipt"], next_cpu)
        self.assertEqual(self.cli("backup", str(Path(self.tmp.name) / "busy"), check=False).returncode, 2)
        self.cli("reset")
        self.assertEqual(self.status(next_gpu)["status"], "cancelled")
        self.assertEqual(self.status(next_cpu)["status"], "queued")
        gate.touch()
        self.assertEqual(pg.wait(timeout=10), 0)
        self.assertEqual(pc.wait(timeout=10), 0)
        self.assertEqual(self.status(next_cpu)["status"], "succeeded")
        self.assertEqual(self.status(gpu)["status"], "succeeded")
        self.assertEqual(self.status(cpu)["status"], "succeeded")
        copied = self.cli("replay", cpu).stdout.strip()
        self.assertEqual(self.status(copied)["lane"], "cpu")
        self.cli("reset", "--all")
        self.assertEqual(self.status(copied)["status"], "cancelled")

    def test_v1_history_is_readable_but_not_executed(self):
        self.root.mkdir()
        db_path = self.root / "queue.sqlite3"
        conn = sqlite3.connect(db_path)
        try:
            with conn:
                conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, argv TEXT, cwd TEXT, "
                             "submitted TEXT, status TEXT, started TEXT, finished TEXT, "
                             "pid INTEGER, code INTEGER, note TEXT)")
                conn.execute("INSERT INTO jobs(id,argv,cwd,submitted,status) VALUES(1,'[]',?,"
                             "'relative old date','queued')", (self.tmp.name,))
        finally:
            conn.close()
        (self.root / "logs").mkdir()
        (self.root / "logs" / "1.log").write_text("legacy combined log")
        self.assertEqual(self.status(1)["cwd"], self.tmp.name)
        self.assertIn("legacy combined", self.cli("log", "1").stdout)
        self.assertIn("queued", self.cli("list").stdout)
        self.assertEqual(self.cli("worker", "--drain", check=False).returncode, 2)
        self.assertEqual(self.status(1)["status"], "queued")

    @unittest.skipUnless(sys.platform == "win32", "real Windows Job Object required")
    def test_windows_exited_parent_keeps_slot_for_descendant(self):
        pids, started, gate, ended = [Path(self.tmp.name) / x for x in
                                      ("launcher", "descendant", "release", "ended")]
        descendant = ("import sys,time; from pathlib import Path; "
                      "s,g,e=map(Path,sys.argv[1:]); s.touch(); end=time.monotonic()+15; "
                      "exec('while not g.exists() and time.monotonic()<end:\\n time.sleep(.02)'); e.touch()")
        launcher = ("import os,sys,time,subprocess; from pathlib import Path; "
                    "subprocess.Popen([sys.executable,'-c',sys.argv[1],*sys.argv[3:]]); "
                    "Path(sys.argv[2]).write_text(str(os.getpid())); "
                    "time.sleep(1)")
        first = self.submit(launcher, descendant, str(pids), str(started), str(gate), str(ended))
        second = self.submit("import sys; from pathlib import Path; assert Path(sys.argv[1]).exists()", str(ended))
        worker = self.worker("--drain")
        self.until(lambda: started.exists() and pids.exists() and pids.read_text().strip())
        parent_done = self.win_process_waiter(int(pids.read_text()))
        self.assertEqual(parent_done(5000), 0)
        self.assertEqual(self.status(first)["status"], "running")
        self.assertEqual(self.status(second)["status"], "queued")
        gate.touch()
        self.assertEqual(worker.wait(timeout=15), 0)
        self.assertEqual(self.status(first)["status"], "succeeded")
        self.assertEqual(self.status(second)["status"], "succeeded")

    @unittest.skipUnless(sys.platform == "win32", "real Windows Job Object required")
    def test_windows_kill_tree_before_next_job(self):
        pids = Path(self.tmp.name) / "pids"
        task = ("import os,sys,time,subprocess; from pathlib import Path; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(40)']); "
                "Path(sys.argv[1]).write_text(f'{os.getpid()} {p.pid}'); time.sleep(40)")
        active = self.submit(task, str(pids))
        next_id = self.submit("print('next')")
        worker = self.worker("--drain")
        self.until(lambda: pids.exists() and len(pids.read_text().split()) == 2)
        waits = [self.win_process_waiter(int(pid)) for pid in pids.read_text().split()]
        self.assertTrue(all(wait(0) == 258 for wait in waits))
        self.assertEqual(self.cli("kill", str(next_id), check=False).returncode, 2)
        # reset leaves the running job alone; queue a fresh successor afterward.
        self.cli("reset")
        self.assertEqual(self.status(active)["status"], "running")
        next_id = self.submit("print('actually next')")
        self.cli("kill", str(active))
        self.assertEqual(worker.wait(timeout=15), 0)
        self.assertTrue(all(wait(0) == 0 for wait in waits), "the tracked processes still exist")
        self.assertEqual(self.status(active)["status"], "killed")
        self.assertEqual(self.status(next_id)["status"], "succeeded")

    def test_exclusive_worker_and_crash_blocks_later_jobs(self):
        alive = Path(self.tmp.name) / "started"
        done = Path(self.tmp.name) / "done"
        release = Path(self.tmp.name) / "release"
        job = self.submit(
            "import sys,time; from pathlib import Path; "
            "a,b,c=map(Path,sys.argv[1:]); a.touch(); "
            "end=time.monotonic()+10; "
            "exec('while not c.exists() and time.monotonic()<end:\\n time.sleep(.02)'); b.touch()",
            str(alive), str(done), str(release))
        p = self.worker()
        self.until(alive.exists)
        if os.name == "nt":
            pid = self.until(lambda: self.status(job)["pid"])
            gate_exited = self.win_process_waiter(pid)
        later = self.submit("print('next')")
        second = self.cli("worker", "--drain", check=False)
        self.assertEqual(second.returncode, 2)
        self.assertIn("worker lock", second.stderr)
        self.assertEqual(self.cli("recover", str(job), "--confirmed-stopped", check=False).returncode, 2)
        p.kill()
        p.wait(timeout=5)
        self.assertEqual(self.cli("worker", "--drain", check=False).returncode, 2)
        self.assertEqual(self.status(later)["status"], "queued")
        unrelated = self.submit("print('independent CPU')", queue="cpu")
        self.cli("worker", "--drain", "--queue", "cpu")
        self.assertEqual(self.status(unrelated)["status"], "succeeded")
        if os.name == "nt":
            self.assertEqual(gate_exited(5000), 0)
            self.assertFalse(done.exists())
        else:
            release.touch()
            self.until(done.exists)
        self.cli("recover", str(job), "--confirmed-stopped")
        self.cli("worker", "--drain")
        self.assertEqual(self.status(job)["status"], "interrupted")
        self.assertEqual(self.status(later)["status"], "succeeded")

    @unittest.skipIf(sys.platform == "win32", "SIGINT simulation is POSIX-only")
    def test_graceful_stop_finishes_current_but_not_next(self):
        gate = Path(self.tmp.name) / "gate"
        first = self.submit("import time,sys; from pathlib import Path; "
                            "p=Path(sys.argv[1]); end=time.monotonic()+8; "
                            "exec('while not p.exists() and time.monotonic()<end:\\n time.sleep(.02)')", str(gate))
        second = self.submit("print('not processed yet')")
        p = self.worker()
        self.until(lambda: self.status(first)["pid"])
        p.send_signal(signal.SIGINT)
        time.sleep(.15)
        gate.touch()
        self.assertEqual(p.wait(timeout=8), 0)
        self.assertEqual(self.status(first)["status"], "succeeded")
        self.assertEqual(self.status(second)["status"], "queued")


if __name__ == "__main__":
    unittest.main()
