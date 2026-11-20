#!/usr/bin/env python3
"""Local, persistent, one-worker FIFO for trusted foreground commands."""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid

LANES = ("gpu", "cpu")


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def default_root():
    if os.name == "nt":
        return Path(os.environ["LOCALAPPDATA"]) / "GpuFifoV2"
    return Path.home() / ".local" / "state" / "gpu-fifo-v2"


@contextmanager
def db(root, *, allow_v1_read=False):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    conn = sqlite3.connect(root / "queue.sqlite3", timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, receipt TEXT NOT NULL UNIQUE,
                    argv TEXT NOT NULL, cwd TEXT NOT NULL, submitted TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    started TEXT, finished TEXT, pid INTEGER, code INTEGER, note TEXT,
                    kill_requested INTEGER NOT NULL DEFAULT 0,
                    replay_of INTEGER, replay_of_receipt TEXT,
                    lane TEXT NOT NULL DEFAULT 'gpu' CHECK(lane IN ('gpu', 'cpu'))
                )
            """)
            columns = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
            if not {"kill_requested", "replay_of", "receipt", "replay_of_receipt", "lane"} <= columns and not allow_v1_read:
                raise ValueError("this spool uses an older schema; choose a fresh v2 spool")
            yield conn
    finally:
        conn.close()


class WorkerLockBusy(ValueError):
    pass


@contextmanager
def worker_lock(root, lane):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Keep the same file/inode. The OS releases the lock even after a crash.
    with (root / f"worker-{lane}.lock").open("a+b", buffering=0) as f:
        if os.name == "nt":
            import msvcrt
            # Windows permits byte-range locks beyond end-of-file.
            f.seek(0)
            lock = lambda: msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            unlock = lambda: (f.seek(0), msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1))
        else:
            import fcntl
            lock = lambda: fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            unlock = lambda: fcntl.flock(f, fcntl.LOCK_UN)
        try:
            lock()
        except OSError as exc:
            raise WorkerLockBusy(f"cannot acquire worker lock for {lane}; another worker may already be active") from exc
        try:
            yield
        finally:
            unlock()


def get_job(root, reference):
    ref = str(reference)
    with db(root, allow_v1_read=True) as conn:
        if ref.isascii() and ref.isdecimal():
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (int(ref),)).fetchone()
        else:
            try:
                receipt = str(uuid.UUID(ref))
            except ValueError as exc:
                raise ValueError("invalid job reference (expected UUID or numeric ID)") from exc
            columns = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
            row = (conn.execute("SELECT * FROM jobs WHERE receipt=?", (receipt,)).fetchone()
                   if "receipt" in columns else None)
    if row is None:
        raise ValueError(f"job {reference} does not exist")
    return row


def resolve_argv(words, cwd):
    if words[:1] == ["--"]:
        words = words[1:]
    if not words:
        raise ValueError("specify a program and its arguments after --")
    first = Path(words[0])
    if first.is_absolute() or first.parent != Path(".") or words[0].startswith("."):
        name = str((cwd / first).resolve())
    else:
        local = cwd / first
        name = str(local.resolve()) if local.is_file() else shutil.which(words[0])
    if not name or not Path(name).is_file():
        raise ValueError(f"program not found: {words[0]}")
    name = str(Path(name).resolve())
    if os.name == "nt" and Path(name).suffix.lower() not in {".exe", ".com"}:
        raise ValueError("submit an .exe/.com directly; for PowerShell scripts submit powershell.exe -File explicitly")
    return [name, *words[1:]]


def validate_local_path(value, *, directory=False):
    """Check input form before I/O; do not follow Windows reparse points."""
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError("invalid local path")
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("an absolute local path is required")
    if os.name == "nt":
        import ctypes as C
        import re
        import stat
        from pathlib import PureWindowsPath
        is_reserved = getattr(os.path, "isreserved", None)
        if is_reserved is None:  # Python 3.10–3.12
            is_reserved = lambda part: PureWindowsPath(part).is_reserved()
        if (not re.fullmatch(r"[A-Za-z]:", path.drive) or
                any(part == ".." or part.endswith((".", " ")) or
                    any(ch in part for ch in ":<>|?*") or is_reserved(part)
                    for part in path.parts[1:])):
            raise ValueError("only ordinary absolute local drive paths are supported")
        drive_type = C.WinDLL("kernel32").GetDriveTypeW
        drive_type.argtypes, drive_type.restype = [C.c_wchar_p], C.c_uint
        if drive_type(path.anchor) not in {2, 3, 5, 6}:
            raise ValueError("remote or unavailable Windows drive")
        current = Path(path.anchor)
        for part in path.parts[1:]:
            current /= part
            info = current.lstat()
            if info.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                raise ValueError("use the physical local path, not a symlink or junction")
        if not directory and path.suffix.lower() not in {".exe", ".com"}:
            raise ValueError("submit a local .exe/.com directly")
    if not (path.is_dir() if directory else path.is_file()):
        raise ValueError("local path does not exist or has the wrong type")
    return str(path)


def finish(root, job_id, state, code=None, note=None):
    with db(root) as conn:
        conn.execute("UPDATE jobs SET status=?, code=?, finished=?, note=? WHERE id=?",
                     (state, code, now(), note, job_id))


class WindowsJob:
    """Own and contain a trusted gate and every descendant, with no breakaway."""

    def __init__(self):
        import ctypes as C
        from ctypes import wintypes as W

        class BasicLimits(C.Structure):
            _fields_ = [
                ("ProcessUserTime", C.c_int64), ("JobUserTime", C.c_int64),
                ("LimitFlags", W.DWORD), ("MinimumWorkingSetSize", C.c_size_t),
                ("MaximumWorkingSetSize", C.c_size_t), ("ActiveProcessLimit", W.DWORD),
                ("Affinity", C.c_size_t), ("PriorityClass", W.DWORD),
                ("SchedulingClass", W.DWORD),
            ]

        class IoCounters(C.Structure):
            _fields_ = [(name, C.c_uint64) for name in
                        ("ReadOperations", "WriteOperations", "OtherOperations",
                         "ReadBytes", "WriteBytes", "OtherBytes")]

        class ExtendedLimits(C.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimits), ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", C.c_size_t), ("JobMemoryLimit", C.c_size_t),
                ("PeakProcessMemoryUsed", C.c_size_t), ("PeakJobMemoryUsed", C.c_size_t),
            ]

        class Accounting(C.Structure):
            _fields_ = [(name, C.c_int64) for name in
                        ("TotalUserTime", "TotalKernelTime", "PeriodUserTime", "PeriodKernelTime")] + [
                ("TotalPageFaultCount", W.DWORD), ("TotalProcesses", W.DWORD),
                ("ActiveProcesses", W.DWORD), ("TotalTerminatedProcesses", W.DWORD),
            ]

        k = C.WinDLL("kernel32", use_last_error=True)

        def bind(name, args, result=W.BOOL):
            fn = getattr(k, name)
            fn.argtypes, fn.restype = args, result
            return fn

        self.C, self.Accounting = C, Accounting
        self.close_handle = bind("CloseHandle", [W.HANDLE])
        new_job = bind("CreateJobObjectW", [C.c_void_p, W.LPCWSTR], W.HANDLE)
        configure = bind("SetInformationJobObject", [W.HANDLE, C.c_int, C.c_void_p, W.DWORD])
        self.assign_process = bind("AssignProcessToJobObject", [W.HANDLE, W.HANDLE])
        self.query = bind("QueryInformationJobObject",
                          [W.HANDLE, C.c_int, C.c_void_p, W.DWORD, C.c_void_p])
        self.terminate_job = bind("TerminateJobObject", [W.HANDLE, W.UINT])
        self.handle = new_job(None, None)
        if not self.handle:
            raise C.WinError(C.get_last_error())
        limits = ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE; no breakaway
        if not configure(self.handle, 9, C.byref(limits), C.sizeof(limits)):  # ExtendedLimitInformation
            error = C.WinError(C.get_last_error())
            self.close()
            raise error

    def assign(self, proc):
        # CPython's Windows Popen retains the exact CreateProcess handle.
        # Never reopen by PID: a failed gate could exit and its PID be reused.
        handle = getattr(proc, "_handle", None)
        if handle is None:
            raise OSError("Windows worker requires CPython's subprocess process handle")
        if not self.assign_process(self.handle, int(handle)):
            raise self.C.WinError(self.C.get_last_error())

    def active(self):
        info = self.Accounting()
        if not self.query(self.handle, 1, self.C.byref(info), self.C.sizeof(info), None):
            raise self.C.WinError(self.C.get_last_error())
        return info.ActiveProcesses

    def terminate(self):
        if not self.terminate_job(self.handle, 125):
            raise self.C.WinError(self.C.get_last_error())

    def close(self):
        if self.handle:
            self.close_handle(self.handle)
            self.handle = None


def gate():
    # Internal trusted launcher: no user code executes before assignment.
    # Use -I in the worker so an untrusted cwd cannot shadow stdlib modules.
    if sys.stdin.buffer.read(1) != b"R":
        return 125
    argv = json.load(sys.stdin.buffer)
    try:
        child = subprocess.Popen(argv, shell=False, stdin=subprocess.DEVNULL)
    except (OSError, ValueError) as exc:
        print(f"gpufifo: could not launch job program: {exc}", file=sys.stderr)
        return 126
    code = child.wait()
    # Preserve Win32's unsigned 32-bit termination status.
    import ctypes as C
    exit_process = C.WinDLL("kernel32").ExitProcess
    exit_process.argtypes, exit_process.restype = [C.c_uint32], None
    exit_process(code)
    return code


def task_dir(root, job_id):
    return root / "tasks" / str(job_id)


def copy_task_records(root, dest):
    source = root / "tasks"
    if not source.is_dir():
        return
    for original in source.iterdir():
        if not original.name.isdecimal() or not original.is_dir():
            continue
        copied = dest / "tasks" / original.name
        copied.mkdir(parents=True)
        for name in ("job.json", "stdout.log", "stderr.log"):
            p = original / name
            if p.is_file():
                shutil.copy2(p, copied / name)


def run_job(root, row, *, base_env=None, validate_paths=False):
    row = dict(row)
    job_id = row["id"]
    # Commit running before spawn. If any uncertain failure follows spawning,
    # preserve that record so the next worker refuses to proceed.
    directory = task_dir(root, job_id)
    internal_work, temp = directory / "work", directory / "tmp"
    cwd = Path(row["cwd"]) if row["cwd"] else internal_work
    proc, job = None, None
    try:
        try:
            internal_work.mkdir(parents=True)
            temp.mkdir()
            argv = json.loads(row["argv"])
            if validate_paths:
                argv[0] = validate_local_path(argv[0])
                cwd = Path(validate_local_path(str(cwd), directory=True))
            record = dict(row)
            record.update(argv=argv, cwd=str(cwd), requested_cwd=row["cwd"] or None,
                          task_dir=str(directory))
            (directory / "job.json").write_text(
                json.dumps(record, indent=2) + "\n", encoding="utf-8")
            env = dict(os.environ if base_env is None else base_env)
            env.update(TEMP=str(temp), TMP=str(temp), TMPDIR=str(temp),
                       GPU_FIFO_TASK_ID=str(job_id), GPU_FIFO_TASK_DIR=str(directory),
                       GPU_FIFO_TASK_RECEIPT=row["receipt"], GPU_FIFO_QUEUE=row["lane"],
                       GPU_FIFO_QUEUES=json.dumps(row.get("queues", [row["lane"]])))
            with (directory / "stdout.log").open("xb", buffering=0) as out, \
                    (directory / "stderr.log").open("xb", buffering=0) as err:
                options = dict(cwd=cwd, env=env, shell=False, stdout=out, stderr=err)
                if os.name == "nt":
                    job = WindowsJob()
                    # Do not put the potentially long user argv on the gate's
                    # command line: the gate reads the JSON after the release byte.
                    proc = subprocess.Popen(
                        [sys.executable, "-I", str(Path(__file__).resolve()), "_gate"],
                        stdin=subprocess.PIPE, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                        **options)
                    try:
                        job.assign(proc)
                    except OSError:
                        proc.stdin.close()  # EOF: gate cannot start the program
                        proc.wait()
                        proc = None
                        raise
                else:
                    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
                                            start_new_session=True, **options)
        except (OSError, ValueError) as exc:
            if proc is not None:
                raise
            finish(root, job_id, "launch_failed", note=str(exc))
            return

        with db(root) as conn:
            conn.execute("UPDATE jobs SET pid=? WHERE id=?", (proc.pid, job_id))
        if job is None:
            code = proc.wait()
            finish(root, job_id, "succeeded" if code == 0 else "failed", code)
            return

        # Do not release the command if a kill was already accepted.
        with db(root) as conn:
            requested = conn.execute("SELECT kill_requested FROM jobs WHERE id=?",
                                     (job_id,)).fetchone()[0]
        killed = bool(requested)
        timed_out = False
        limit = row.get("timeout")
        cutoff = None
        if not killed:
            if limit is not None:
                utc_deadline = (datetime.now(timezone.utc) + timedelta(seconds=limit)).isoformat(timespec="milliseconds")
                with db(root) as conn:
                    conn.execute("UPDATE jobs SET runtime_deadline=?,runtime_phase='executing' WHERE id=?", (utc_deadline, job_id))
                cutoff = time.monotonic() + limit
            proc.stdin.write(b"R" + json.dumps(argv).encode("ascii"))
            proc.stdin.flush()
        proc.stdin.close()
        if killed:
            job.terminate()
        draining = False
        while job.active():
            if limit is not None and not draining and proc.poll() is not None:
                # Use the held root handle, not process-name/PID scans.
                with db(root) as conn:
                    conn.execute("UPDATE jobs SET runtime_phase='draining' WHERE id=?", (job_id,))
                draining = True
            if not killed:
                with db(root) as conn:
                    requested = conn.execute("SELECT kill_requested FROM jobs WHERE id=?",
                                             (job_id,)).fetchone()[0]
                if requested:
                    job.terminate()
                    killed = True
                elif cutoff is not None and time.monotonic() >= cutoff:
                    job.terminate()
                    killed = timed_out = True
            time.sleep(0.05)
        code = proc.wait()
        finish(root, job_id, "timed_out" if timed_out else "killed" if killed else ("succeeded" if code == 0 else "failed"), code)
    finally:
        if proc is not None and proc.stdin is not None and not proc.stdin.closed:
            proc.stdin.close()
        if job is not None:
            job.close()


def worker(root, once, lane):
    stop = False

    def request_stop(*_):
        nonlocal stop
        stop = True

    previous = signal.signal(signal.SIGINT, request_stop)
    try:
        with worker_lock(root, lane):
            with db(root) as conn:
                running = conn.execute("SELECT id,pid FROM jobs WHERE status='running' AND lane=?", (lane,)).fetchone()
            if running:
                raise ValueError(f"job {running['id']} may still be running (last PID: {running['pid']}); "
                                 "verify the process and any descendants have stopped, then run recover")
            while not stop:
                with db(root) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    row = conn.execute("SELECT * FROM jobs WHERE status='queued' AND lane=? ORDER BY id LIMIT 1",
                                       (lane,)).fetchone()
                    if row:
                        when = now()
                        conn.execute("UPDATE jobs SET status='running', started=? WHERE id=?",
                                     (when, row["id"]))
                        row = dict(row)
                        row.update(status="running", started=when)
                if row:
                    run_job(root, row)
                elif once:
                    return 0
                else:
                    time.sleep(0.25)
    finally:
        signal.signal(signal.SIGINT, previous)
    return 0


def main(prog=None):
    if sys.argv[1:] == ["_gate"] and os.name == "nt":
        return gate()
    p = argparse.ArgumentParser(prog=prog, description=__doc__)
    p.add_argument("--root", type=Path, help="alternative local spool (also GPU_FIFO_ROOT)")
    subs = p.add_subparsers(dest="cmd", required=True)
    help_command = subs.add_parser("help", help="show general or command help")
    help_command.add_argument("command", nargs="?")
    s = subs.add_parser("submit")
    s.add_argument("--queue", choices=LANES, default="gpu")
    s.add_argument("--cwd", type=Path, help="explicit working directory (default: new job work directory)")
    s.add_argument("argv", nargs=argparse.REMAINDER)
    d = subs.add_parser("worker")
    d.add_argument("--queue", choices=LANES, default="gpu")
    d.add_argument("--drain", action="store_true", help="stop when the queue first becomes empty")
    subs.add_parser("list")
    reset = subs.add_parser("reset", help="cancel queued jobs; retain running jobs, history and logs")
    selection = reset.add_mutually_exclusive_group()
    selection.add_argument("--queue", choices=LANES, default="gpu")
    selection.add_argument("--all", action="store_true")
    b = subs.add_parser("backup", help="copy a consistent spool snapshot while worker is stopped")
    b.add_argument("destination", type=Path)
    for name in ("status", "log", "wait", "cancel", "recover", "kill", "replay"):
        s = subs.add_parser(name)
        s.add_argument("id", **({"nargs": "?"} if name == "status" else {}))
        if name == "log":
            s.add_argument("--stream", choices=("stdout", "stderr"), default="stdout")
        if name == "wait":
            s.add_argument("--timeout", type=float)
        if name == "recover":
            s.add_argument("--confirmed-stopped", action="store_true", required=True)
    a = p.parse_args()
    if a.cmd == "help":
        if a.command is None:
            p.print_help()
        elif a.command in subs.choices:
            subs.choices[a.command].print_help()
        else:
            p.error(f"unknown help topic: {a.command}")
        return 0
    root = (a.root or (Path(os.environ["GPU_FIFO_ROOT"]) if os.environ.get("GPU_FIFO_ROOT")
                        else default_root())).expanduser().resolve()
    if (root / ".incomplete").exists():
        raise ValueError("this spool is an incomplete backup")
    if a.cmd == "submit":
        cwd = (a.cwd if a.cwd is not None else Path.cwd()).resolve(strict=True)
        if not cwd.is_dir():
            raise ValueError("working directory must exist and be a directory")
        argv = resolve_argv(a.argv, cwd)
        receipt = str(uuid.uuid4())
        with db(root) as conn:
            conn.execute("INSERT INTO jobs(receipt,argv,cwd,submitted,lane) VALUES(?,?,?,?,?)",
                         (receipt, json.dumps(argv), str(cwd) if a.cwd is not None else "", now(), a.queue))
        print(receipt)
    elif a.cmd == "worker":
        return worker(root, a.drain, a.queue)
    elif a.cmd == "list":
        with db(root, allow_v1_read=True) as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        columns = ("id", "receipt", "lane", "status", "submitted", "pid", "code", "argv")
        print("ID\tRECEIPT\tQUEUE\tSTATUS\tSUBMITTED (UTC)\tPID\tEXIT\tCOMMAND")
        for row in rows:
            r = dict(row)
            print("\t".join(str(r[k]) if r.get(k) is not None else "-" for k in columns))
    elif a.cmd == "status" and a.id is None:
        with db(root, allow_v1_read=True) as conn:
            conn.execute("BEGIN")
            counts = {r[0]: r[1] for r in conn.execute("SELECT status,COUNT(*) FROM jobs GROUP BY status")}
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued','running') ORDER BY id").fetchall()
        lanes = {lane: {"running": None, "queued": []} for lane in LANES}
        pending, running = [], []
        for r in map(dict, rows):
            lane = r.get("lane", "gpu")
            item = {"id": r["id"], "receipt": r.get("receipt"), "queue": lane}
            if r["status"] == "queued":
                lanes[lane]["queued"].append(item)
                pending.append(item)
            else:
                item.update(pid=r["pid"], started=r["started"], kill_requested=r.get("kill_requested", 0))
                lanes[lane]["running"] = item
                running.append(item)
        print(json.dumps({"queued": pending, "running": running,
                          "queues": lanes, "counts": counts}, indent=2))
    elif a.cmd == "reset":
        scope = "all queues" if a.all else f"queue {a.queue}"
        clause = "" if a.all else " AND lane=?"
        args = (now(), "queue reset") if a.all else (now(), "queue reset", a.queue)
        with db(root) as conn:
            result = conn.execute("UPDATE jobs SET status='cancelled',finished=?,note=? "
                                  "WHERE status='queued'" + clause, args).rowcount
        print(f"Cancelled {result} queued job(s) in {scope}; running jobs, history and logs retained.")
    elif a.cmd == "replay":
        original = get_job(root, a.id)["id"]
        receipt = str(uuid.uuid4())
        with db(root) as conn:
            job_id = conn.execute(
                "INSERT INTO jobs(receipt,argv,cwd,submitted,note,replay_of,replay_of_receipt,lane) "
                "SELECT ?,argv,cwd,?,?,id,receipt,lane FROM jobs WHERE id=? AND status NOT IN ('queued','running')",
                (receipt, now(), f"replay of job {original}", original))
            if job_id.rowcount != 1:
                raise ValueError("only a terminal job can be replayed")
        print(receipt)
    elif a.cmd == "kill":
        if os.name != "nt":
            raise ValueError("running-job kill requires Windows Job Objects; POSIX is test-only")
        row = dict(get_job(root, a.id))
        if row["status"] != "running" or "lane" not in row:
            raise ValueError("only an active job in a v2 queue may receive a kill request")
        try:
            with worker_lock(root, row["lane"]):
                raise ValueError("worker is not active; resolve any interrupted job with recover")
        except WorkerLockBusy:
            pass
        with db(root) as conn:
            changed = conn.execute("UPDATE jobs SET kill_requested=1 WHERE id=? AND status='running'",
                                   (row["id"],)).rowcount
        if changed != 1:
            raise ValueError("only an active job in this queue may receive a kill request")
        print(f"Kill requested for job {a.id}; see status or wait for the outcome.")
    elif a.cmd == "backup":
        destination = a.destination.expanduser().resolve()
        if destination == root or root in destination.parents:
            raise ValueError("backup destination must be outside the source spool")
        with worker_lock(root, "gpu"), worker_lock(root, "cpu"), db(root) as conn:
            if conn.execute("SELECT id FROM jobs WHERE status='running'").fetchone():
                raise ValueError("resolve the interrupted job before backing up")
            destination.mkdir(exist_ok=False)
            marker = destination / ".incomplete"
            marker.touch()
            with closing(sqlite3.connect(destination / "queue.sqlite3")) as target:
                conn.backup(target)
            logs = root / "logs"
            if logs.is_dir():
                shutil.copytree(logs, destination / "logs")
            copy_task_records(root, destination)
            marker.unlink()
        print(destination)
    elif a.cmd == "status":
        row = dict(get_job(root, a.id))
        row["argv"] = json.loads(row["argv"])
        if "replay_of" in row:
            directory = task_dir(root, row["id"])
            row["task_dir"] = str(directory)
            row["requested_cwd"] = row["cwd"] or None
            row["cwd"] = row["cwd"] or str(directory / "work")
            row["stdout"] = str(directory / "stdout.log")
            row["stderr"] = str(directory / "stderr.log")
        else:
            row["combined_log"] = str(root / "logs" / f"{row['id']}.log")
        print(json.dumps(row, ensure_ascii=True, indent=2))
    elif a.cmd == "log":
        job_id = get_job(root, a.id)["id"]
        path = task_dir(root, job_id) / f"{a.stream}.log"
        legacy = root / "logs" / f"{job_id}.log"
        if not path.is_file() and legacy.is_file():
            if a.stream == "stderr":
                raise ValueError("legacy job has only a combined log; use log without --stream")
            path = legacy
        if not path.is_file():
            raise ValueError("no log written yet")
        with path.open("rb") as f:
            shutil.copyfileobj(f, sys.stdout.buffer)
    elif a.cmd == "wait":
        start = time.monotonic()
        while True:
            row = get_job(root, a.id)
            if row["status"] not in {"queued", "running"}:
                print(f"{a.id}: {row['status']} (exit={row['code']})")
                return 0 if row["status"] == "succeeded" else 1
            if a.timeout is not None and time.monotonic() - start >= a.timeout:
                print(f"{a.id}: still {row['status']}", file=sys.stderr)
                return 3
            time.sleep(0.25)
    elif a.cmd == "cancel":
        job_id = get_job(root, a.id)["id"]
        with db(root) as conn:
            changed = conn.execute("UPDATE jobs SET status='cancelled',finished=? WHERE id=? AND status='queued'",
                                   (now(), job_id)).rowcount
            if changed != 1:
                raise ValueError("only queued jobs can be cancelled")
    elif a.cmd == "recover":
        row = dict(get_job(root, a.id))
        if "lane" not in row:
            raise ValueError("use a v1 CLI to recover a legacy job")
        with worker_lock(root, row["lane"]), db(root) as conn:
            changed = conn.execute(
                "UPDATE jobs SET status='interrupted',finished=?,note=? WHERE id=? AND status='running'",
                (now(), "operator confirmed earlier job processes stopped", row["id"])).rowcount
            if changed != 1:
                raise ValueError("only an interrupted running record may be recovered")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, OSError, sqlite3.Error) as exc:
        print(f"gpufifo: {exc}", file=sys.stderr)
        sys.exit(2)
