"""Private Linux supervisor for trusted cooperative workloads (not a sandbox)."""
import ctypes
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

_POLL = 0.05
_GRACE = 0.5


def _report(fd, message):
    # A vanished worker is detected through the control pipe, not a signal to
    # any PID loaded from disk. Continue cleanup if the status reader is gone.
    try:
        os.write(fd, (json.dumps(message) + "\n").encode("ascii"))
    except BrokenPipeError:
        pass


def _children():
    try:
        children = Path("/proc/thread-self/children").read_text()
    except FileNotFoundError:
        # CONFIG_CHECKPOINT_RESTORE is optional. Current children cannot
        # disappear while we scan: this single-threaded subreaper alone can
        # reap their zombies. An already-adopted parent stays visible even
        # if it exits and hands us new children during this pass.
        me = os.getpid()
        proc = Path("/proc")
        if (proc / "self").resolve() != proc / str(me):
            raise OSError("procfs is not mounted for the supervisor PID namespace")
        result = set()
        for path in proc.iterdir():
            if not path.name.isdecimal():
                continue
            try:
                stat = (path / "stat").read_text(errors="replace").rsplit(")", 1)[1].split()
            except (FileNotFoundError, ProcessLookupError):
                continue  # Unrelated processes can exit while we enumerate.
            if int(stat[1]) == me:
                result.add(int(path.name))
        return result
    return {int(pid) for pid in children.split()}


def _supervise(status):
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        _children()  # Fail before release if procfs is unavailable.
    except (OSError, ValueError) as exc:
        _report(status, ["launch_failed", f"Linux supervision unavailable: {exc}"[:512]])
        return 0
    _report(status, ["ready"])
    line = sys.stdin.buffer.readline()
    if not line:
        return 0  # Worker stopped before releasing a command.
    try:
        child = subprocess.Popen(json.loads(line), shell=False, stdin=subprocess.DEVNULL,
                                 start_new_session=True)
    except (OSError, ValueError) as exc:
        _report(status, ["launch_failed", str(exc)[:512]])
        return 0
    _report(status, ["started", child.pid, time.monotonic(), time.time()])
    with selectors.DefaultSelector() as selector:
        control_fd = sys.stdin.fileno()
        selector.register(control_fd, selectors.EVENT_READ)
        cutoff = None
        root_exited = False
        while True:
            if not root_exited:
                # Keep the direct child (even as a zombie) unreaped, anchoring
                # its PID/PGID until we have stopped sending group signals.
                root_exited = os.waitid(os.P_PID, child.pid,
                                        os.WEXITED | os.WNOWAIT | os.WNOHANG) is not None
                if root_exited:
                    _report(status, ["draining"])
            if root_exited:
                # All remaining descendants either descend from one of our
                # direct children or have been adopted here by subreaping.
                # Only this single-threaded supervisor reaps them; zombies
                # remain in the child list so concurrent exits do not remove
                # children behind a procfs traversal's back.
                adopted = _children() - {child.pid}
                if not adopted:
                    break
                for pid in adopted:
                    os.waitpid(pid, os.WNOHANG)
            if cutoff is not None and time.monotonic() >= cutoff:
                _signal_group(child.pid, signal.SIGKILL)
                cutoff = None
            if selector.select(_POLL):
                # A request OR EOF from an abruptly lost worker cancels the
                # exact held group. Never reopen a stale PID from the spool.
                os.read(control_fd, 1)
                selector.unregister(control_fd)
                _signal_group(child.pid, signal.SIGTERM)
                cutoff = time.monotonic() + _GRACE
    _report(status, ["done", child.wait()])
    return 0


def _signal_group(pgid, sig):
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


class LinuxJob:
    """Start and control one isolated trusted supervisor via private pipes."""

    def __init__(self, options):
        self.proc = None
        self.read_fd, write_fd = os.pipe()
        self.pending = b""
        try:
            self.proc = subprocess.Popen(
                [sys.executable, "-I", str(Path(__file__).resolve()), str(write_fd)],
                stdin=subprocess.PIPE, pass_fds=(write_fd,), start_new_session=True,
                **options)
            os.set_blocking(self.read_fd, False)
        except BaseException:
            self.close()
            raise
        finally:
            os.close(write_fd)

    def send(self, argv):
        self.proc.stdin.write(json.dumps(argv, ensure_ascii=True).encode("ascii") + b"\n")
        self.proc.stdin.flush()

    def cancel(self):
        # EOF cannot be misdirected and remains effective if the worker dies.
        self.proc.stdin.close()

    def events(self):
        with selectors.DefaultSelector() as sel:
            sel.register(self.read_fd, selectors.EVENT_READ)
            ready = sel.select(_POLL)
        if not ready:
            return []
        data = os.read(self.read_fd, 65536)
        if not data:
            raise OSError("Linux workload supervisor exited without confirmed drain")
        self.pending += data
        records = self.pending.split(b"\n")
        self.pending = records.pop()
        return [json.loads(line) for line in records]

    def close(self):
        if self.proc is not None:
            self.proc.stdin.close()
            # Always ask the supervisor to stop if unwinding, but do not claim
            # success or block forever on an escaped process if it cannot drain.
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        os.close(self.read_fd)


if __name__ == "__main__":
    # Popen closes all undeclared handles in the workload, including status.
    sys.exit(_supervise(int(sys.argv[1])))
