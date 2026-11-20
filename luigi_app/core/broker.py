"""Trusted same-SID Luigi cooperative broker; not a Windows security boundary."""
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
import uuid

import luigi_app.core.gpufifo as fifo
from luigi_app.core.auth import AuthCore, Rejected, _digest
from luigi_app.platforms.hip_policy import DeferredLaunch, workload_environment

MAX_FRAME = 65536
MAX_LOG_BYTES = 8000


def encode(obj):
    packed = json.dumps(obj, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    if len(packed) > MAX_FRAME:
        raise ValueError("frame exceeds limit")
    return packed


def decode(data):
    if not isinstance(data, bytes) or len(data) > MAX_FRAME:
        raise ValueError("frame exceeds limit")

    def unique(pairs):
        out = {}
        for k, v in pairs:
            if k in out:
                raise ValueError("duplicate field")
            out[k] = v
        return out

    def reject_constant(_):
        raise ValueError("non-finite constant")

    return json.loads(data.decode("utf-8"), object_pairs_hook=unique, parse_constant=reject_constant)


def fields(obj, required, optional=()):
    if not isinstance(obj, dict) or not set(required) <= obj.keys() or obj.keys() - set(required) - set(optional):
        raise ValueError("invalid fields")


def job_reference(value):
    if not isinstance(value, str) or len(value) != 36 or str(uuid.UUID(value)) != value:
        raise ValueError("expected canonical job UUID")
    return value


def queue(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", value):
        raise ValueError("invalid queue name")
    return value


def memberships(db, job):
    return [r[0] for r in db.execute("SELECT name FROM job_queues WHERE job=? ORDER BY name", (job,))]


def queue_rows(db):
    return [dict(r) for r in db.execute("SELECT name,kind,gpu_uuid,gpu_provider FROM named_queues ORDER BY name")]


def narrow_job(row, root):
    path = fifo.task_dir(root, row["id"])
    return {k: row[k] for k in ("receipt", "lane", "status", "submitted", "started", "finished", "code",
                                "replay_of_receipt", "timeout", "runtime_deadline", "runtime_phase")} | {
        "argv": json.loads(row["argv"]), "cwd": row["cwd"] or str(path / "work"),
        "stdout": str(path / "stdout.log"), "stderr": str(path / "stderr.log"),
    }


class Broker:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.holds = {}
        self.hold_lock = threading.Lock()
        self.active = set()
        self.claim_lock = threading.Lock()
        self.policy = None
        with fifo.db(self.root):
            pass
        self.auth = AuthCore(self.root / "queue.sqlite3")
        with self.auth._db(tx=True) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='queues'").fetchone():
                raise ValueError("use a fresh multiqueue spool; pilot spools are not migrated")
            db.execute("CREATE TABLE IF NOT EXISTS named_queues ("
                       "name TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('cpu','gpu')), "
                       "gpu_uuid TEXT UNIQUE)")
            queue_columns = {r[1] for r in db.execute("PRAGMA table_info(named_queues)")}
            if "gpu_provider" not in queue_columns:
                db.execute("ALTER TABLE named_queues ADD COLUMN gpu_provider TEXT")
                db.execute("UPDATE named_queues SET gpu_provider='amd' WHERE gpu_uuid IS NOT NULL")
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_unbound_gpu ON named_queues(kind) "
                       "WHERE kind='gpu' AND gpu_uuid IS NULL")
            db.execute("CREATE TABLE IF NOT EXISTS job_queues (job INTEGER NOT NULL REFERENCES jobs(id), "
                       "name TEXT NOT NULL REFERENCES named_queues(name), PRIMARY KEY(job,name))")
            db.execute("CREATE INDEX IF NOT EXISTS queue_job_index ON job_queues(name,job)")
            columns = {r[1] for r in db.execute("PRAGMA table_info(jobs)")}
            if "timeout" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN timeout INTEGER NOT NULL DEFAULT 1800")
            if "runtime_deadline" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN runtime_deadline TEXT")
            if "runtime_phase" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN runtime_phase TEXT")

    def queue_configs(self):
        with self.auth._db() as db:
            db.row_factory = sqlite3.Row
            return queue_rows(db)

    def snapshot(self):
        """Operator-only dashboard callback; an allowlist, not raw job rows."""
        with self.auth._db() as db:
            db.row_factory = sqlite3.Row
            configs = queue_rows(db)
            def listed(status, time):
                return [{"id": r["receipt"], "queues": memberships(db, r["id"]),
                         "status": r["status"], "phase": r["runtime_phase"], time + "_at": r[time]}
                        for r in db.execute("SELECT id,receipt,status,started,finished,runtime_phase FROM jobs "
                                            + status + " ORDER BY id DESC LIMIT 40")]
            queues = []
            for q in configs:
                name = q["name"]
                hold = self.hold(name)
                counts = dict(db.execute(
                    "SELECT j.status,count(*) FROM jobs j JOIN job_queues m ON m.job=j.id "
                    "WHERE m.name=? AND j.status IN('queued','running') GROUP BY j.status", (name,)))
                head = db.execute(
                    "SELECT j.id,j.submitted FROM jobs j JOIN job_queues m ON j.id=m.job "
                    "WHERE m.name=? AND j.status='queued' ORDER BY j.id LIMIT 1", (name,)).fetchone()
                if head and hold["allowed"]:
                    for other in memberships(db, head["id"]):
                        if other == name:
                            continue
                        prior = db.execute(
                            "SELECT 1 FROM job_queues m JOIN jobs j ON j.id=m.job WHERE m.name=? "
                            "AND (j.status='running' OR (j.status='queued' AND j.id<?))",
                            (other, head["id"])).fetchone()
                        if prior or not self.hold(other)["allowed"]:
                            hold = {"allowed": False, "reason": "waiting_for_other_queue"}
                            break
                queues.append({"name": name, "queued": counts.get("queued", 0), "active": counts.get("running", 0),
                               "held": bool(head) and not hold["allowed"],
                               "oldest_queued_at": head["submitted"] if head else None,
                               "hold_reason": str(hold["reason"]).replace(" ", "_")})
            telem = (self.policy.telemetry(configs) if self.policy else
                     {"cpu": {"percent": None, "observed_at": None, "reason": "no_sample"}, "gpu": []})
            return {"queues": queues, "active": listed("WHERE status='running'", "started"),
                    "recent": listed("WHERE status NOT IN ('queued','running')", "finished"), "telemetry": telem}

    def hold(self, lane):
        with self.hold_lock:
            return dict(self.holds.get(lane, {"allowed": False, "reason": "not sampled"}))

    def _agent(self, db, bearer):
        return self.auth._active_agent(db, _digest("ag", bearer), self.auth._now())

    def _admin(self, db, password):
        self.auth._require_admin(db, password)

    def _owned(self, db, owner, receipt):
        row = db.execute(
            "SELECT j.* FROM jobs j JOIN task_bindings t ON (j.receipt=t.id) "
            "WHERE t.owner=? AND j.receipt=?", (owner, job_reference(receipt))).fetchone()
        if row is None:
            raise Rejected()
        return row

    def _backup(self, password):
        # No caller-chosen path and no credential DB export; consistent drained metadata+logs.
        with self.auth._db() as db:
            self._admin(db, password)
        with self.auth._db(tx=True) as source:
            source.row_factory = sqlite3.Row
            self._admin(source, password)
            if source.execute("SELECT 1 FROM jobs WHERE status='running'").fetchone():
                raise ValueError("resolve interrupted/running jobs before backup")
            queues = queue_rows(source)
            jobs = [dict(r) for r in source.execute(
                "SELECT j.*, t.owner AS owner FROM jobs j JOIN task_bindings t ON (j.receipt=t.id) ORDER BY j.id")]
            for job in jobs:
                job["queues"] = memberships(source, job["id"])
            backups = self.root / "backups"
            backups.mkdir(exist_ok=True)
            destination = backups / str(uuid.uuid4())
            destination.mkdir()
            marker = destination / ".incomplete"
            marker.touch()
            with (destination / "snapshot.json").open("x", encoding="utf-8") as snapshot:
                json.dump({"format": "luigi-metadata-v2", "queues": queues, "jobs": jobs}, snapshot, indent=2)
            fifo.copy_task_records(self.root, destination)
            marker.unlink()
            return {"backup_id": destination.name}

    def dispatch(self, req):
        fields(req, ("op", "credential", "data"))
        op, cred, data = req["op"], req["credential"], req["data"]
        if not isinstance(cred, str) or not isinstance(op, str) or not isinstance(data, dict):
            raise ValueError("invalid request")
        if op == "invite":
            fields(data, (), ("ttl",))
            return {"invitation": self.auth.issue_invite(cred, ttl=data.get("ttl", 300))}
        if op == "enroll":
            fields(data, ())
            agent, bearer = self.auth.redeem_invite(cred)
            return {"agent": agent, "bearer": bearer}
        if op == "rotate":
            fields(data, ())
            return {"bearer": self.auth.rotate_bearer(cred)}
        if op == "revoke":
            fields(data, ("agent",))
            if not isinstance(data["agent"], str) or not re.fullmatch("[0-9a-f]{32}", data["agent"]):
                raise ValueError("invalid agent ID")
            self.auth.revoke_agent(cred, data["agent"])
            return {}
        if op == "recover":
            fields(data, ("receipt", "confirmed_stopped"))
            if data["confirmed_stopped"] is not True:
                raise ValueError("confirm interrupted process tree has stopped")
            receipt = job_reference(data["receipt"])
            with self.claim_lock, self.auth._db(tx=True) as db:
                self._admin(db, cred)
                row = db.execute("SELECT id FROM jobs WHERE receipt=? AND status='running'", (receipt,)).fetchone()
                if row is None or row[0] in self.active:
                    raise Rejected()
                changed = db.execute("UPDATE jobs SET status='interrupted',finished=?,note=? "
                                     "WHERE receipt=? AND status='running'",
                                     (fifo.now(), "operator confirmed stopped", receipt)).rowcount
                if changed != 1:
                    raise Rejected()
                return {"recovered": True}
        if op == "snapshot":
            fields(data, ())
            with self.auth._db() as db:
                self._admin(db, cred)
            return self.snapshot()
        if op == "backup":
            fields(data, ())
            return self._backup(cred)
        if op == "submit":
            fields(data, ("argv",), ("cwd", "queues", "lane", "timeout"))
            if "queues" in data and "lane" in data:
                raise ValueError("specify queues only once")
            timeout = data.get("timeout", 1800)
            if type(timeout) is not int or not 1 <= timeout <= 86400:
                raise ValueError("task timeout must be 1–86400 seconds")
            names = data.get("queues", [data.get("lane", "gpu")])
            if not isinstance(names, list) or not 1 <= len(names) <= 16:
                raise ValueError("expected 1–16 queue names")
            names = sorted(queue(name) for name in names)
            if len(set(names)) != len(names):
                raise ValueError("duplicate queue name")
            with self.auth._db() as read_db:
                self._agent(read_db, cred)
            argv = data["argv"]
            if not isinstance(argv, list) or not 1 <= len(argv) <= 128 or any(
                not isinstance(a, str) or len(a) > 8192 or "\0" in a for a in argv):
                raise ValueError("invalid argv")
            supplied = data.get("cwd")
            if supplied is not None and (not isinstance(supplied, str) or not Path(supplied).is_absolute()):
                raise ValueError("cwd must be absolute")
            if len(encode(argv)) > MAX_FRAME // 2:
                raise ValueError("argv too large")
            if any(re.search(r"(?:ag_[A-Za-z0-9_-]{43}|en_[A-Za-z0-9_-]{22})(?![A-Za-z0-9_-])", value)
                   for value in [*argv, supplied or ""]):
                raise ValueError("credentials are not job inputs")
            argv = [fifo.validate_local_path(argv[0]), *argv[1:]]
            if supplied:
                supplied = fifo.validate_local_path(supplied, directory=True)
            if self.policy:
                for conf in self.queue_configs():
                    if conf["name"] in names and conf["gpu_uuid"]:
                        self.policy.validate_submission(conf, argv)
        with self.auth._db(tx=True) as db:
            db.row_factory = sqlite3.Row
            if op in {"create", "reset", "queues"}:
                self._admin(db, cred)
                if op == "queues":
                    fields(data, ())
                    return {"queues": [
                        q | {"hold": self.hold(q["name"]), "jobs": dict(db.execute(
                            "SELECT j.status,COUNT(*) FROM jobs j JOIN job_queues m ON j.id=m.job "
                            "WHERE m.name=? GROUP BY j.status", (q["name"],)))}
                        for q in queue_rows(db)]}
                fields(data, ("lane",), ("kind", "gpu_uuid", "gpu_provider") if op == "create" else ())
                name = queue(data["lane"])
                if op == "create":
                    kind = data.get("kind", name)
                    if kind not in ("cpu", "gpu"):
                        raise ValueError("choose cpu or gpu queue kind")
                    device = data.get("gpu_uuid")
                    provider = data.get("gpu_provider")
                    if device is None and provider is not None:
                        raise ValueError("GPU provider requires a stable GPU UUID")
                    if device is not None:
                        if getattr(self.policy, "gpu_monitor_disabled", False):
                            raise ValueError("GPU UUID bindings require enabled monitoring")
                        if kind != "gpu" or not isinstance(device, str):
                            raise ValueError("GPU UUID requires a GPU queue")
                        provider = provider or ("nvidia" if device.lower().startswith("gpu-") else "amd")
                        if provider not in ("amd", "nvidia"):
                            raise ValueError("GPU provider must be amd or nvidia")
                        if device.lower().startswith("gpu-"):
                            if provider != "nvidia":
                                raise ValueError("GPU-prefixed UUID is only valid for NVIDIA")
                            device = device[4:]
                        device = str(uuid.UUID(device))
                        if device == str(uuid.UUID(int=0)):
                            raise ValueError("nil GPU UUID")
                    existing = db.execute("SELECT kind,gpu_uuid,gpu_provider FROM named_queues WHERE name=?", (name,)).fetchone()
                    if existing is not None:
                        if tuple(existing) != (kind, device, provider):
                            raise ValueError("existing queue has a different configuration")
                        return {"created": False, "name": name}
                    if db.execute("SELECT count(*) FROM named_queues").fetchone()[0] >= 32:
                        raise ValueError("too many queues")
                    if kind == "gpu" and db.execute(
                        "SELECT 1 FROM named_queues WHERE kind='gpu' AND (gpu_uuid IS NULL OR ? IS NULL)",
                        (device,)).fetchone():
                        raise ValueError("an unbound GPU queue cannot coexist with other GPU queues")
                    db.execute("INSERT INTO named_queues(name,kind,gpu_uuid,gpu_provider) VALUES(?,?,?,?)", (name, kind, device, provider))
                    return {"created": True, "name": name}
                if not db.execute("SELECT 1 FROM named_queues WHERE name=?", (name,)).fetchone():
                    raise ValueError("queue has not been created")
                changed = db.execute("UPDATE jobs SET status='cancelled',finished=?,note='queue reset' "
                                     "WHERE status='queued' AND id IN(SELECT job FROM job_queues WHERE name=?)",
                                     (fifo.now(), name)).rowcount
                return {"cancelled": changed}
            owner = self._agent(db, cred)
            if op == "submit":
                known = {r["name"]: r["kind"] for r in queue_rows(db)}
                if any(name not in known for name in names):
                    raise ValueError("queue has not been created")
                if sum(known[name] == "gpu" for name in names) > 1:
                    raise ValueError("multiple GPU queues in one job are unsupported")
                receipt = str(uuid.uuid4())
                primary_kind = "gpu" if any(known[name] == "gpu" for name in names) else "cpu"
                job = db.execute("INSERT INTO jobs(receipt,argv,cwd,submitted,lane,timeout) VALUES(?,?,?,?,?,?)",
                                 (receipt, json.dumps(argv), supplied or "", fifo.now(), primary_kind, timeout)).lastrowid
                db.executemany("INSERT INTO job_queues VALUES(?,?)", [(job, name) for name in names])
                db.execute("INSERT INTO task_bindings VALUES (?,?)", (receipt, owner))
                return {"receipt": receipt}
            if op == "list":
                fields(data, (), ("limit",))
                limit = data.get("limit", 100)
                if type(limit) is not int or not 1 <= limit <= 100:
                    raise ValueError("limit must be 1 to 100")
                return {"jobs": [{k:r[k] for k in ("receipt","lane","status","code")} | {"queues": memberships(db, r["id"])} for r in db.execute(
                    "SELECT j.id,j.receipt,j.lane,j.status,j.code FROM jobs j "
                    "JOIN task_bindings t ON (j.receipt=t.id) WHERE t.owner=? "
                    "ORDER BY j.id DESC LIMIT ?", (owner, limit))]}
            if op in {"status", "wait", "log", "kill", "cancel", "replay"}:
                fields(data, ("receipt",), ("stream", "offset") if op == "log" else ())
                row = self._owned(db, owner, data["receipt"])
                if op in {"status", "wait"}:
                    result = narrow_job(row, self.root)
                    result["queues"] = memberships(db, row["id"])
                    if row["status"] == "queued":
                        result["holds"] = {}
                        for name in result["queues"]:
                            blocker = db.execute(
                                "SELECT 1 FROM jobs j JOIN job_queues m ON j.id=m.job "
                                "WHERE m.name=? AND j.status='queued' AND j.id<?", (name,row["id"])).fetchone()
                            result["holds"][name] = ({"allowed": False, "reason": "older job at queue head"}
                                                     if blocker else self.hold(name))
                    return result
                if op == "log":
                    stream, offset = data.get("stream", "stdout"), data.get("offset", 0)
                    if stream not in {"stdout", "stderr"} or type(offset) is not int or offset < 0:
                        raise ValueError("invalid log selector")
                    path = fifo.task_dir(self.root, row["id"]) / (stream + ".log")
                    try:
                        with path.open("rb") as f:
                            f.seek(offset)
                            chunk = f.read(MAX_LOG_BYTES)
                    except FileNotFoundError:
                        chunk = b""
                    return {"text": chunk.decode("utf-8", errors="replace"), "next_offset": offset + len(chunk)}
                if op == "kill":
                    if row["status"] != "running":
                        raise ValueError("only a running job can be killed")
                    if os.name != "nt" and sys.platform != "linux":
                        raise ValueError("job tree termination is unavailable on this platform")
                    db.execute("UPDATE jobs SET kill_requested=1 WHERE id=?", (row["id"],))
                    return {"requested": True}
                if op == "cancel":
                    if row["status"] != "queued":
                        raise ValueError("only a queued job can be cancelled")
                    db.execute("UPDATE jobs SET status='cancelled',finished=? WHERE id=?",
                               (fifo.now(), row["id"]))
                    return {"cancelled": True}
                if row["status"] in {"queued", "running"}:
                    raise ValueError("only a terminal job can be replayed")
                receipt = str(uuid.uuid4())
                job = db.execute("INSERT INTO jobs(receipt,argv,cwd,submitted,lane,replay_of,replay_of_receipt,timeout) "
                           "VALUES(?,?,?,?,?,?,?,?)", (receipt, row["argv"], row["cwd"], fifo.now(), row["lane"],
                                                    row["id"], row["receipt"], row["timeout"])).lastrowid
                db.execute("INSERT INTO job_queues SELECT ?,name FROM job_queues WHERE job=?", (job,row["id"]))
                db.execute("INSERT INTO task_bindings VALUES (?,?)", (receipt, owner))
                return {"receipt": receipt}
            raise ValueError("unsupported operation")

    def handle(self, raw):
        try:
            return encode({"ok": True, "result": self.dispatch(decode(raw))})
        except Rejected:
            return encode({"ok": False, "error": "not authorized"})
        except Exception:
            # Never put arbitrary input/credentials, paths or SQL exceptions in server logs/errors.
            return encode({"ok": False, "error": "request rejected"})


# A running row is the durable reservation for every related queue. No
# SQLite update may expose a partially allocated job.
_ELIGIBLE = """
    SELECT j.* FROM jobs j JOIN task_bindings t ON t.id=j.receipt
    WHERE j.status='queued' AND EXISTS (SELECT 1 FROM job_queues m WHERE m.job=j.id)
    AND NOT EXISTS (
        SELECT 1 FROM job_queues wanted
        JOIN job_queues held ON held.name=wanted.name
        JOIN jobs other ON other.id=held.job
        WHERE wanted.job=j.id AND
            (other.status='running' OR (other.status='queued' AND other.id<j.id))
    )
    ORDER BY j.id
"""


def _configurations(db):
    return {r["name"]: r for r in queue_rows(db)}


def claim(broker, prelaunch, stop):
    """One allocation transaction. prelaunch must only read cached telemetry."""
    with broker.claim_lock:
        with broker.auth._db(tx=True) as db:
            db.row_factory = sqlite3.Row
            config = _configurations(db)
            holds = {}
            for name, q in config.items():
                try:
                    allowed, reason = prelaunch(q)
                except Exception:
                    allowed, reason = False, "load telemetry unavailable"
                active = db.execute(
                    "SELECT j.id FROM jobs j JOIN job_queues m ON m.job=j.id "
                    "WHERE m.name=? AND j.status='running'", (name,)).fetchone()
                if active:
                    allowed = False
                    reason = ("reserved by a running job" if active[0] in broker.active
                              else "interrupted job needs operator review")
                holds[name] = {"allowed": bool(allowed), "reason": str(reason)[:512]}
            with broker.hold_lock:
                broker.holds = holds
            if stop.is_set():
                return None
            for candidate in db.execute(_ELIGIBLE):
                names = memberships(db, candidate["id"])
                if not all(holds[name]["allowed"] for name in names):
                    continue
                when = fifo.now()
                db.execute("UPDATE jobs SET status='running',started=?,runtime_phase='launching' WHERE id=?", (when, candidate["id"]))
                row = dict(candidate) | {"status": "running", "started": when, "queues": names}
                binding = next((config[n] for n in names if config[n]["gpu_uuid"]), None)
                break
            else:
                return None
        broker.active.add(row["id"])
        return row | {"_hip_binding": binding}


def dispatcher(broker, stop, prelaunch, execute=None):
    """Only one dispatcher serves this spool. Running jobs drain on stop."""
    if execute is None:
        def execute(row):
            binding = row.pop("_hip_binding")
            try:
                if binding:
                    if broker.policy is None:
                        raise DeferredLaunch("gpu_provider_not_configured")
                    env = broker.policy.launch_environment(binding, json.loads(row["argv"]))
                else:
                    env = workload_environment()
            except DeferredLaunch as exc:
                # No process was spawned. Release every queue atomically and
                # preserve its original sequence/receipt. No timeout starts.
                with broker.claim_lock, broker.auth._db(tx=True) as db:
                    pending_kill = db.execute("SELECT kill_requested FROM jobs WHERE id=?", (row["id"],)).fetchone()[0]
                    if pending_kill:
                        db.execute("UPDATE jobs SET status='killed',finished=?,note=? WHERE id=?",
                                   (fifo.now(), "stopped before launch", row["id"]))
                    else:
                        db.execute("UPDATE jobs SET status='queued',started=NULL,pid=NULL,runtime_phase=NULL,note=? WHERE id=?",
                                   (str(exc), row["id"]))
                return
            except (OSError, ValueError):
                fifo.finish(broker.root, row["id"], "launch_failed", note="pinned HIP workload validation failed")
                return
            fifo.run_job(broker.root, row, base_env=env, validate_paths=True, stop_requested=stop.is_set)

    def run(row):
        try:
            execute(row)
        finally:
            with broker.claim_lock:
                broker.active.discard(row["id"])

    workers = set()
    with fifo.worker_lock(broker.root, "dispatcher"):
        try:
            while not stop.is_set():
                workers = {t for t in workers if t.is_alive()}
                if len(workers) >= 16:
                    stop.wait(.25)
                    continue
                row = claim(broker, prelaunch, stop)
                if row is None:
                    stop.wait(.25)
                    continue
                worker = threading.Thread(target=run, args=(row,))
                worker.start()
                workers.add(worker)
        finally:
            for worker in workers:
                worker.join()
