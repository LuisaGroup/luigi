"""Luigi trusted local broker and CLI. Windows and Linux local transports."""
import argparse
import getpass
import json
import os
from pathlib import Path
import sys
import stat
import signal
import threading
import time
import warnings

from luigi_app.core.broker import Broker, decode, encode, dispatcher
import luigi_app.platforms.transport as transport


def default_root():
    if sys.platform.startswith("linux"):
        state = Path(os.environ.get("XDG_STATE_HOME", ""))
        if not state.is_absolute():
            state = Path.home() / ".local" / "state"
        return state / "luigi"
    return Path(os.environ["LOCALAPPDATA"]) / "LuigiMultiQueue"


def configure_policy(broker, adapter, *, hip_dll=None, hip_workload=(), nvidia_smi=None, cuda_driver=None, cuda_workload=(), no_gpu_monitor=False, prelaunch=None):
    if prelaunch is None:
        if no_gpu_monitor:
            from luigi_app.platforms.linux_policy import NoGpuMonitorPolicy
            if adapter not in (None, "all") or hip_dll or hip_workload or nvidia_smi or cuda_driver or cuda_workload:
                raise ValueError("--no-gpu-monitor cannot be combined with device binding options")
            if any(q["gpu_uuid"] for q in broker.queue_configs()):
                raise ValueError("--no-gpu-monitor cannot serve a spool with GPU UUID queues")
            if sys.platform.startswith("linux"):
                from luigi_app.platforms.linux_load import linux_guard
                guard = linux_guard()
            else:
                from luigi_app.platforms.loadguard import LoadGuard
                guard = LoadGuard.windows()
            broker.policy = NoGpuMonitorPolicy(guard)
            print("WARNING: GPU monitoring disabled; generic GPU jobs can overlap external work.", file=sys.stderr)
        else:
            nvidia = None
            if nvidia_smi:
                from luigi_app.platforms.nvidia_probe import NvidiaProvider
                nvidia = NvidiaProvider(nvidia_smi, cuda_driver, cuda_workload)
            elif cuda_driver or cuda_workload:
                raise ValueError("CUDA options require a trusted --nvidia-smi path")
        if not no_gpu_monitor and sys.platform.startswith("linux"):
            from luigi_app.platforms.linux_policy import LinuxPolicy
            if adapter not in (None, "all") or hip_dll or hip_workload:
                raise ValueError("Windows adapter LUID and pinned HIP options are unavailable on Linux")
            if nvidia:
                from luigi_app.platforms.linux_load import LinuxPciAggregate, linux_guard
                probe = LinuxPciAggregate(nvidia_readings=lambda: {
                    row.pci_bus_id:row.percent for row in nvidia.inventory_load()})
                broker.policy = LinuxPolicy(linux_guard(gpu_probe=probe))
            else:
                broker.policy = LinuxPolicy()
        elif not no_gpu_monitor:
            from luigi_app.platforms.hip_policy import HipPolicy
            broker.policy = HipPolicy(None if adapter in (None, "all") else adapter, hip_dll, hip_workload)
        if not no_gpu_monitor:
            from luigi_app.platforms.gpu_routing import RoutedPolicy
            broker.policy = RoutedPolicy(broker.policy, nvidia)
        policy = broker.policy.decision
    else:
        policy = prelaunch
    return policy


def serve(root, adapter, *, hip_dll=None, hip_workload=(), nvidia_smi=None, cuda_driver=None, cuda_workload=(), no_gpu_monitor=False, prelaunch=None, stop=None):
    """Explicit foreground run under non-elevated user; no service/registry code."""
    import luigi_app.core.gpufifo as fifo
    transport.require_non_elevated()
    root = transport.private_root(root)
    stop = stop if stop is not None else threading.Event()
    broker = Broker(root)
    with broker.auth._db() as db:
        if not db.execute("SELECT 1 FROM admin WHERE id=1").fetchone():
            raise ValueError("run the interactive bootstrap command first")
    key = transport.identity(root)
    policy = configure_policy(broker, adapter, hip_dll=hip_dll, hip_workload=hip_workload,
                              nvidia_smi=nvidia_smi, cuda_driver=cuda_driver, cuda_workload=cuda_workload,
                              no_gpu_monitor=no_gpu_monitor, prelaunch=prelaunch)
    sampler = (threading.Thread(target=broker.policy.observe, args=(broker, stop), daemon=True)
               if broker.policy is not None else None)
    with fifo.worker_lock(root, "broker"):
        listener = transport.listener(root)
        worker = threading.Thread(target=dispatcher, args=(broker, stop, policy))

        def requests():
            try:
                while not stop.is_set():
                    try:
                        transport.reply_one(listener.accept(), broker.handle, key)
                    except (EOFError, ValueError, OSError):
                        continue
            finally:
                stop.set()

        thread = threading.Thread(target=requests, daemon=True)
        thread.start()
        if sampler:
            sampler.start()
        worker.start()
        print("Luigi foreground broker ready (cooperative, current user).", flush=True)
        try:
            while not stop.wait(.25):
                if not thread.is_alive() or not worker.is_alive():
                    break
        except KeyboardInterrupt:
            print("Stopping intake; waiting for active jobs.", file=sys.stderr)
        finally:
            stop.set()
            if sys.platform.startswith("linux"):
                listener.close()
                thread.join(4)
            # Complete current jobs normally; do not kill unrelated or queued work.
            worker.join()


def read_tty(prompt):
    if not sys.stdin.isatty():
        raise ValueError("a terminal is required for hidden credential entry")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        return getpass.getpass(prompt)


def request(root, op, credential, **data):
    response = decode(transport.client(root, encode({"op": op, "credential": credential, "data": data})))
    if not response["ok"]:
        raise ValueError(response["error"])
    return response["result"]



def run_dashboard(root):
    """Prompt once on a trusted console; route only the read-only snapshot RPC."""
    from luigi_app.dashboard.server import open_dashboard, sanitize

    if not sys.stdout.isatty():
        raise ValueError("a local terminal is required for the private dashboard link")
    password = read_tty("Luigi admin password: ")
    in_flight = threading.Lock()

    def snapshot():
        if not in_flight.acquire(blocking=False):
            raise TimeoutError("snapshot already in flight")
        try:
            return request(root, "snapshot", password)
        finally:
            in_flight.release()

    # Fail before starting HTTP if broker identity, password, or snapshot is invalid.
    sanitize(snapshot())
    session = open_dashboard(snapshot)
    try:
        # Do not auto-open via a browser process command line, where its viewing
        # capability could be logged. This link is for the local terminal only.
        print("Private viewing link (do not share): " + session.url, flush=True)
        input("Open it on this computer. Press Enter here to close the session. ")
    finally:
        session.close()
    return 0

def parse():
    p = argparse.ArgumentParser(prog="luigi", description=__doc__)
    p.add_argument("--root", type=Path, help="local spool (Linux default: ~/.local/state/luigi; Windows: LocalAppData/LuigiMultiQueue)")
    s = p.add_subparsers(dest="op", required=True)
    a = s.add_parser("bootstrap", help="one-time trusted local administrator password setup")
    a = s.add_parser("serve", help="start foreground broker (non-elevated)")
    a.add_argument("--gpu-adapter", default="all", help="Windows adapter LUID or all; Linux only supports all")
    a.add_argument("--no-gpu-monitor", action="store_true", help="explicitly allow generic GPU jobs without GPU telemetry (CPU gate applies)")
    a.add_argument("--hip-dll", help="trusted physical path to the pinned AMD HIP runtime")
    a.add_argument("--hip-workload", action="append", default=[], help="trusted physical executable; repeat to approve more")
    a.add_argument("--nvidia-smi", help="trusted absolute nvidia-smi path")
    a.add_argument("--cuda-driver", help="trusted absolute CUDA driver library path")
    a.add_argument("--cuda-workload", action="append", default=[], help="trusted physical CUDA executable; repeat to approve more")
    for name in ("create", "reset"):
        a = s.add_parser(name)
        a.add_argument("lane", metavar="NAME")
        if name == "create":
            a.add_argument("--kind", choices=("gpu", "cpu"))
            a.add_argument("--gpu-uuid")
            a.add_argument("--gpu-provider", choices=("amd","nvidia"))
    s.add_parser("queues")
    s.add_parser("dashboard", help="private local read-only queue dashboard (admin)")
    sup = s.add_parser("supervise", help="serve through stable local hot-update supervisor")
    sup.add_argument("--gpu-adapter", default="all", help="Windows adapter LUID or all; Linux only supports all")
    sup.add_argument("--no-gpu-monitor", action="store_true", help="explicitly allow generic GPU jobs without GPU telemetry (CPU gate applies)")
    sup.add_argument("--hip-dll", help="trusted physical path to the pinned AMD HIP runtime")
    sup.add_argument("--hip-workload", action="append", default=[], help="trusted physical executable; repeat to approve more")
    sup.add_argument("--nvidia-smi", help="trusted absolute nvidia-smi path")
    sup.add_argument("--cuda-driver", help="trusted absolute CUDA driver library path")
    sup.add_argument("--cuda-workload", action="append", default=[], help="trusted physical CUDA executable; repeat to approve more")
    upd = s.add_parser("hot-update", help="stage a trusted local release ZIP and drain into it (admin)")
    upd.add_argument("archive", type=Path)
    upd.add_argument("--sha256", required=True)
    upd.add_argument("--drain-timeout", type=int, default=3600)
    s.add_parser("hot-update-status", help="show the active release and update phase (admin)")
    s.add_parser("backup")
    a = s.add_parser("revoke")
    a.add_argument("agent")
    a = s.add_parser("recover")
    a.add_argument("receipt")
    a.add_argument("--confirmed-stopped", action="store_true", required=True)
    a = s.add_parser("invite")
    a.add_argument("--ttl", type=int, default=300)
    a.add_argument("--out", type=Path, required=True, help="fresh private file for invitation")
    a = s.add_parser("enroll")
    a.add_argument("--invite-file", type=Path)
    a.add_argument("--out", type=Path, required=True, help="fresh private file for bearer")
    for name in ("rotate", "submit", "status", "list", "wait", "log", "kill", "cancel", "replay"):
        a = s.add_parser(name)
        a.add_argument("--token-file", type=Path)
        if name == "rotate":
            a.add_argument("--out", type=Path, required=True)
        elif name == "submit":
            a.add_argument("--queue", dest="queues", metavar="NAME", action="append")
            a.add_argument("--cwd")
            a.add_argument("--timeout", type=int, help="task runtime in seconds (default 1800; max 86400)")
            a.add_argument("argv", nargs=argparse.REMAINDER)
        elif name == "list":
            a.add_argument("--limit", type=int, default=100)
        else:
            a.add_argument("receipt")
            if name == "wait":
                a.add_argument("--timeout", type=float, default=300)
            if name == "log":
                a.add_argument("--stream", choices=("stdout", "stderr"), default="stdout")
                a.add_argument("--offset", type=int, default=0)
    return p.parse_args()


def read_file(path):
    if sys.platform.startswith("linux"):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "r", encoding="ascii") as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError("credential file must be private and user-owned")
            raw = f.read(256)
    else:
        with path.open("r", encoding="ascii") as f:
            raw = f.read(256)
    if len(raw) == 256:
        raise ValueError("credential file exceeds limit")
    return raw.strip()


def secret_output(path, fetch, field):
    # Reserve the fresh path before token rotation/redeeming a single-use invite.
    if Path(path).exists():
        raise ValueError("credential output must be a fresh private path")
    def private_create(name, flags):
        return os.open(name, flags, 0o600)
    with open(path, "x", encoding="ascii", opener=private_create) as out:
        result = fetch()
        out.write(result[field] + "\n")
    return {k:v for k,v in result.items() if k != field}


def main():
    a = parse()
    root = Path(a.root) if a.root else default_root()
    if a.op == "bootstrap":
        root = transport.private_root(root, initialize=True)
        first = read_tty("New Luigi admin password (12+ characters): ")
        if first != read_tty("Repeat new password: "):
            raise ValueError("passwords differ")
        broker = Broker(root)
        transport.identity(root, initialize=True)
        broker.auth.bootstrap_admin(first)
        print("Luigi admin password initialized.")
        return 0
    if a.op == "serve":
        stop = threading.Event()
        previous = signal.signal(signal.SIGTERM, lambda *_: stop.set()) if sys.platform.startswith("linux") else None
        try:
            serve(root, a.gpu_adapter, hip_dll=a.hip_dll, hip_workload=a.hip_workload, nvidia_smi=a.nvidia_smi, cuda_driver=a.cuda_driver,
                  cuda_workload=a.cuda_workload, no_gpu_monitor=a.no_gpu_monitor, stop=stop)
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
        return 0

    if a.op == "dashboard":
        return run_dashboard(root)
    if a.op == "supervise":
        from luigi_app.hotupdate.supervisor import serve_supervised
        return serve_supervised(root, a.gpu_adapter,
                                hip_dll=str(a.hip_dll) if a.hip_dll is not None else None,
                                hip_workload=[str(p) for p in a.hip_workload],
                                nvidia_smi=str(a.nvidia_smi) if a.nvidia_smi is not None else None,
                                cuda_driver=str(a.cuda_driver) if a.cuda_driver is not None else None,
                                cuda_workload=[str(p) for p in a.cuda_workload],
                                no_gpu_monitor=a.no_gpu_monitor)
    if a.op in {"hot-update", "hot-update-status"}:
        password = read_tty("Luigi admin password: ")
        data = ({"archive": str(a.archive.absolute()), "sha256": a.sha256, "drain_timeout": a.drain_timeout}
                if a.op == "hot-update" else {})
        print(json.dumps(request(root, a.op, password, **data), ensure_ascii=True))
        return 0

    if a.op in {"create", "reset", "queues", "backup", "revoke", "recover", "invite"}:
        cred = read_tty("Luigi admin password: ")
    elif a.op == "enroll":
        cred = read_file(a.invite_file) if a.invite_file else read_tty("Luigi invitation: ")
    else:
        cred = read_file(a.token_file) if a.token_file else read_tty("Luigi agent token: ")

    data = {}
    for key in ("receipt", "agent", "lane", "cwd", "argv", "ttl", "limit", "stream",
                "offset", "confirmed_stopped", "queues", "kind", "gpu_uuid", "gpu_provider"):
        value = getattr(a, key, None)
        if value is not None:
            data[key] = value
    if a.op == "submit":
        if data["argv"][:1] == ["--"]:
            data["argv"] = data["argv"][1:]
        if a.timeout is not None:
            data["timeout"] = a.timeout

    if a.op == "wait":
        if not 0 <= a.timeout <= 86400:
            raise ValueError("invalid wait timeout")
        until = time.monotonic() + a.timeout
        while True:
            result = request(root, "status", cred, **data)
            if result["status"] not in {"queued", "running"}:
                print(json.dumps(result, ensure_ascii=True))
                return int(result["status"] != "succeeded")
            if time.monotonic() >= until:
                print(json.dumps(result, ensure_ascii=True))
                return 3
            time.sleep(.4)
    if a.op in {"invite", "enroll", "rotate"}:
        field = "invitation" if a.op == "invite" else "bearer"
        result = secret_output(a.out, lambda: request(root, a.op, cred, **data), field)
    else:
        result = request(root, a.op, cred, **data)
    if a.op in {"submit", "replay"}:
        print(result["receipt"])
    else:
        print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


def entry():
    try:
        return main()
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"luigi: {exc}", file=sys.stderr)
        return 2
    except Exception:
        print("luigi: operation rejected", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(entry())
