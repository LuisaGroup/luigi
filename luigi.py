"""Luigi trusted local broker and CLI. Windows pipe, no remote socket."""
import argparse
import getpass
import json
import os
from pathlib import Path
import sys
import threading
import time
import warnings

from broker import Broker, decode, encode, dispatcher
import transport


def default_root():
    return Path(os.environ["LOCALAPPDATA"]) / "LuigiMultiQueue"


def serve(root, adapter, *, hip_dll=None, hip_workload=(), prelaunch=None, stop=None):
    """Explicit foreground run under non-elevated user; no service/registry code."""
    import gpufifo as fifo

    transport.require_non_elevated()
    stop = stop if stop is not None else threading.Event()
    broker = Broker(root)
    with broker.auth._db() as db:
        if not db.execute("SELECT 1 FROM admin WHERE id=1").fetchone():
            raise ValueError("run the interactive bootstrap command first")
    key = transport.identity(root)
    sampler = None
    if prelaunch is None:
        from hip_policy import HipPolicy
        broker.policy = HipPolicy(None if adapter == "all" else adapter, hip_dll, hip_workload)
        sampler = threading.Thread(target=broker.policy.observe, args=(broker, stop), daemon=True)
        policy = broker.policy.decision
    else:
        policy = prelaunch
    with fifo.worker_lock(root, "broker"):
        listener = transport.windows_listener(root)
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
        print("Luigi foreground broker ready (cooperative, current Windows user).", flush=True)
        try:
            while not stop.wait(.25):
                if not thread.is_alive() or not worker.is_alive():
                    break
        except KeyboardInterrupt:
            print("Stopping intake; waiting for active jobs.", file=sys.stderr)
        finally:
            stop.set()
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
    from dashboard import open_dashboard, sanitize

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
    p.add_argument("--root", type=Path, help="local spool (default: LocalAppData/LuigiMultiQueue)")
    s = p.add_subparsers(dest="op", required=True)
    a = s.add_parser("bootstrap", help="one-time trusted local administrator password setup")
    a = s.add_parser("serve", help="start foreground broker (non-elevated)")
    a.add_argument("--gpu-adapter", required=True, help="adapter LUID, or all for conservative monitoring across adapters")
    a.add_argument("--hip-dll", help="trusted physical path to the pinned AMD HIP runtime")
    a.add_argument("--hip-workload", action="append", default=[], help="trusted physical executable; repeat to approve more")
    for name in ("create", "reset"):
        a = s.add_parser(name)
        a.add_argument("lane", metavar="NAME")
        if name == "create":
            a.add_argument("--kind", choices=("gpu", "cpu"))
            a.add_argument("--gpu-uuid")
    s.add_parser("queues")
    s.add_parser("dashboard", help="private local read-only queue dashboard (admin)")
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
    with path.open("r", encoding="ascii") as f:
        value = f.read(256).strip()
        if len(value) == 256:
            raise ValueError("credential file exceeds limit")
    return value


def secret_output(path, fetch, field):
    # Reserve the fresh path before token rotation/redeeming a single-use invite.
    if Path(path).exists():
        raise ValueError("credential output must be a fresh private path")
    with open(path, "x", encoding="ascii") as out:
        result = fetch()
        out.write(result[field] + "\n")
    return {k:v for k,v in result.items() if k != field}


def main():
    a = parse()
    root = Path(a.root) if a.root else default_root()
    if a.op == "bootstrap":
        first = read_tty("New Luigi admin password (12+ characters): ")
        if first != read_tty("Repeat new password: "):
            raise ValueError("passwords differ")
        broker = Broker(root)
        transport.identity(root, initialize=True)
        broker.auth.bootstrap_admin(first)
        print("Luigi admin password initialized.")
        return 0
    if a.op == "serve":
        serve(root, a.gpu_adapter, hip_dll=a.hip_dll, hip_workload=a.hip_workload)
        return 0

    if a.op == "dashboard":
        return run_dashboard(root)

    if a.op in {"create", "reset", "queues", "backup", "revoke", "recover", "invite"}:
        cred = read_tty("Luigi admin password: ")
    elif a.op == "enroll":
        cred = read_file(a.invite_file) if a.invite_file else read_tty("Luigi invitation: ")
    else:
        cred = read_file(a.token_file) if a.token_file else read_tty("Luigi agent token: ")

    data = {}
    for key in ("receipt", "agent", "lane", "cwd", "argv", "ttl", "limit", "stream",
                "offset", "confirmed_stopped", "queues", "kind", "gpu_uuid"):
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


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"luigi: {exc}", file=sys.stderr)
        sys.exit(2)
    except Exception:
        print("luigi: operation rejected", file=sys.stderr)
        sys.exit(2)
