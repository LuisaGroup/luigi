"""Private child endpoint for the hot-update supervisor."""
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time

from luigi_app.cli import configure_policy
from luigi_app.core.broker import Broker, decode, dispatcher, encode
from luigi_app.platforms import transport

ABI = 1


def running(root):
    with sqlite3.connect((root / "queue.sqlite3").as_uri() + "?mode=ro", uri=True, timeout=2) as db:
        return db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0]


def run(root, endpoint, config):
    transport.require_non_elevated()
    root = transport.private_root(root)
    broker = Broker(root)
    policy = configure_policy(broker, **config)
    if any(q["gpu_uuid"] for q in broker.queue_configs()) and not (config.get("hip_dll") or config.get("nvidia_smi")):
        raise ValueError("pinned GPU queues require an explicitly configured provider")
    boot = transport.listener(endpoint)
    key = transport.identity(endpoint)
    stop = threading.Event()
    paused = threading.Event()
    paused.set()
    worker = None
    worker_stop = None
    ready = threading.Event()

    def handler(raw):
        request = decode(raw)
        if request == {"local_hotupdate_probe": ABI}:
            return encode({"abi": ABI, "worker": bool(worker and worker.is_alive() and ready.is_set())})
        return broker.handle(raw)

    def listen():
        while not stop.is_set():
            try:
                transport.reply_one(boot.accept(), handler, key)
            except (OSError, ValueError, EOFError, TimeoutError):
                continue

    endpoint_thread = threading.Thread(target=listen, name="private-broker-listener", daemon=True)
    endpoint_thread.start()
    observer = threading.Thread(target=broker.policy.observe, args=(broker, stop), daemon=True)
    observer.start()

    def mark(name):
        (endpoint / name).touch()

    def start_worker():
        nonlocal worker, worker_stop, ready
        if worker is not None:
            raise ValueError("dispatcher already exists")
        worker_stop = threading.Event()
        ready = threading.Event()
        worker = threading.Thread(target=dispatcher, args=(broker, worker_stop, policy),
                                  kwargs={"drain": paused, "ready": ready})
        worker.start()
        if not ready.wait(3) or not worker.is_alive():
            raise RuntimeError("dispatcher failed to start")

    def freeze():
        with broker.claim_lock:
            paused.set()

    def finish_worker():
        nonlocal worker
        if worker is None:
            return
        if not paused.is_set() or running(root):
            raise RuntimeError("cannot retire a dispatcher with active jobs")
        worker_stop.set()
        worker.join(3)
        if worker.is_alive():
            raise RuntimeError("dispatcher did not retire")
        worker = None

    try:
        mark("ready")
        while True:
            command = sys.stdin.buffer.readline(32).strip()
            if command == b"start":
                start_worker()
                mark("started")
            elif command == b"drain":
                freeze()
                mark("draining")
            elif command == b"halt":
                finish_worker()
                mark("halted")
            elif command == b"resume":
                if not worker or not worker.is_alive():
                    raise RuntimeError("cannot resume missing dispatcher")
                paused.clear()
                mark("resumed")
            elif command in (b"exit", b""):
                return
            else:
                raise ValueError("unsupported supervisor command")
    finally:
        # Neither a disappearing supervisor nor a failed control command is
        # permission to interrupt work. Keep the lease until descendants exit.
        try:
            freeze()
            if worker is not None:
                while running(root) and worker.is_alive():
                    time.sleep(.2)
                if not running(root):
                    finish_worker()
        finally:
            stop.set()
            if sys.platform.startswith("linux"):
                boot.close()



def main():
    root, endpoint, config = sys.argv[1:]
    run(Path(root),Path(endpoint),json.loads(config))
