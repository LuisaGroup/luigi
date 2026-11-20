"""Manual browser QA fixture. Uses only invented queue data and no broker."""
import argparse
from datetime import datetime, timedelta, timezone
import sys

from dashboard import open_dashboard


def fixture_snapshot(mode):
    if mode == "disconnected":
        raise RuntimeError("synthetic offline provider")
    now = datetime.now(timezone.utc)
    stamp = (now - timedelta(seconds=3)).isoformat(timespec="seconds")
    oldest = (now - timedelta(minutes=2)).isoformat(timespec="seconds")
    held = mode == "held"
    return {
        "queues": [
            {"name": "cpu", "queued": 2 if held else 0, "active": 0,
             "held": held, "hold_reason": "blocked_by_fifo" if held else "ready",
             "oldest_queued_at": oldest if held else None},
            {"name": "gpu-fixture", "queued": 3 if held else 0, "active": 1,
             "held": held, "hold_reason": "waiting_for_other_queue" if held else "ready",
             "oldest_queued_at": oldest if held else None},
        ],
        "telemetry": {
            "cpu": {"percent": 0, "observed_at": stamp, "reason": "ready"},
            "gpu": [{"queue": "gpu-fixture", "percent": None if held else 27,
                     "observed_at": None if held else stamp, "reason": "no_sample" if held else "ready"}],
        },
        "active": [{"id": "01234567-0123-4567-8123-010203040506", "queues": ["gpu-fixture"],
                    "status": "running", "phase": "draining" if held else "executing", "started_at": stamp}],
        "recent": [{"id": "87654321-4321-4765-8765-060504030201", "queues": ["cpu", "gpu-fixture"],
                    "status": "timed_out" if held else "succeeded", "finished_at": stamp}],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", default="healthy", choices=("healthy", "held", "disconnected"))
    args = parser.parse_args()
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        parser.error("fixture requires a local terminal for its viewing link")
    mode = args.mode
    session = open_dashboard(lambda: fixture_snapshot(mode))
    try:
        print("Synthetic data only. Local viewing link: " + session.url, flush=True)
        while True:
            mode_requested = input("Choose healthy, held, disconnected, or Enter to exit: ").strip()
            if not mode_requested:
                break
            if mode_requested in ("healthy", "held", "disconnected"):
                mode = mode_requested
            else:
                print("Unknown fixture mode.")
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
