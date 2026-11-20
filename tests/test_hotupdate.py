"""Native process/transport tests for online Luigi replacement."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

from luigi_app.core.broker import Broker, decode, encode
from luigi_app.platforms import transport

SRC=Path(__file__).resolve().parents[1]
PASSWORD="synthetic test password only"
PYTHON=str(Path(sys.executable).resolve())


def until(predicate, seconds=12):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            value=predicate()
            if value:
                return value
        except (FileNotFoundError,OSError):
            pass
        time.sleep(.045)
    raise AssertionError("condition did not occur before deadline")


class TestHotUpdate(unittest.TestCase):
    def setUp(self):
        if (not sys.platform.startswith("linux") and os.name!="nt") or (hasattr(os,"geteuid") and os.geteuid()==0):
            self.skipTest("requires native local transport under non-elevated account")
        self.temp=tempfile.TemporaryDirectory(prefix="luigi-")
        self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name)
        self.manifest=self.base/"spool"/".h"/"active.json"
        self.root=transport.private_root(self.base/"spool",initialize=True)
        broker=Broker(self.root)
        transport.identity(self.root,initialize=True)
        broker.auth.bootstrap_admin(PASSWORD)
        _,self.token=broker.auth.redeem_invite(broker.auth.issue_invite(PASSWORD))
        with broker.auth._db(tx=True) as db:
            db.execute("insert into named_queues(name,kind) values('cpu','cpu')")
            db.execute("insert into named_queues(name,kind) values('gpu','gpu')")
        self.initial=self.base/"initial"
        self.initial.mkdir()
        for f in ("luigi.py","worker_entry.py"):
            shutil.copyfile(SRC/f,self.initial/f)
        shutil.copytree(SRC/"luigi_app",self.initial/"luigi_app",ignore=shutil.ignore_patterns("__pycache__"))
        # Pin deterministic telemetry only in the isolated fixture releases.
        p=self.initial/"luigi_app"/"hotupdate"/"worker.py"
        s=p.read_text(encoding="utf-8-sig")
        anchor="    policy = configure_policy(broker, **config)"
        assert anchor in s
        s=s.replace(anchor,'''    class FixturePolicy:
        def observe(self, broker, stop):
            stop.wait()
        def decision(self, queue):
            return True, "ready"
    broker.policy=FixturePolicy()
    policy=broker.policy.decision''')
        p.write_text(s,encoding="utf-8")
        self.exit_gate=self.base/"release"
        self.addCleanup(self.exit_gate.touch, exist_ok=True)
        self.start()

    def start(self):
        self.output=(self.base/"supervisor.log").open("ab")
        harness=self.base/"fixture_serve.py"
        harness.write_text("""import sys,threading
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from luigi_app.hotupdate.supervisor import serve_supervised
stop=threading.Event()
def control():
    sys.stdin.buffer.readline()
    stop.set()
threading.Thread(target=control,daemon=True).start()
serve_supervised(Path(sys.argv[2]),stop=stop)
""",encoding="utf-8")
        self.proc=subprocess.Popen([PYTHON,"-B",str(harness),str(self.initial),str(self.root)],
                                   cwd=self.initial,stdin=subprocess.PIPE,stdout=self.output,stderr=self.output)
        self.addCleanup(self.stop)
        try:
            until(lambda:self.rpc("hot-update-status",admin=True)["phase"]=="idle")
        except Exception:
            self.output.flush()
            raise RuntimeError((self.base/"supervisor.log").read_text(encoding="utf-8",errors="replace"))
    def stop(self):
        if not hasattr(self,"proc"):
            return
        self.exit_gate.touch()
        if self.proc.poll() is None:
            self.proc.stdin.write(b"\n")
            self.proc.stdin.flush()
            self.proc.wait(10)
        self.proc.stdin.close()
        self.output.close()

    def rpc(self,op,admin=False,**data):
        response=decode(transport.client(self.root,encode({"op":op,"credential":PASSWORD if admin else self.token,"data":data})))
        if not response["ok"]:
            raise ValueError(response["error"])
        return response["result"]

    def submit(self,tag,blocked=False):
        seq=self.base/"sequence"
        program=("from pathlib import Path; import sys,time;"
                 "p=Path(sys.argv[1]);"
                 + ("exec('while not Path(sys.argv[3]).exists(): time.sleep(0.025)');" if blocked else "")
                 + "open(p,'a').write(sys.argv[2]+'\\n')")
        return self.rpc("submit",queues=["cpu","gpu"],timeout=40,
                        argv=[PYTHON,"-c",program,str(seq),tag,str(self.exit_gate)])["receipt"]

    def status(self,receipt):
        return self.rpc("status",receipt=receipt)["status"]

    def package(self,name,break_start=False,break_schema=False,break_resume=False):
        dest=self.base/(name+".zip")
        with zipfile.ZipFile(dest,"w",zipfile.ZIP_DEFLATED) as z:
            for f in self.initial.rglob("*"):
                if not f.is_file() or f.suffix not in {".py",".js",".css",".html"}:
                    continue
                rel=f.relative_to(self.initial).as_posix()
                value=f.read_bytes()
                if rel=="luigi_app/hotupdate/worker.py" and break_start:
                    value=value.replace(b"        if worker is not None:", b"        raise RuntimeError('fixture broken candidate')\n        if worker is not None:",1)
                if rel=="luigi_app/hotupdate/worker.py" and break_resume:
                    value=value.replace(b'            elif command == b"resume":', b'            elif command == b"resume":\n                raise RuntimeError("fixture broken activation")',1)
                if rel=="luigi_app/core/broker.py" and break_schema:
                    value=value.replace(b'            queue_columns = ',b'            db.execute("CREATE TABLE user_incompatible(x)")\n            queue_columns = ',1)
                z.writestr(rel,value)
        return str(dest),hashlib.sha256(dest.read_bytes()).hexdigest()

    def update(self,release,seconds=10):
        archive,digest=release
        self.rpc("hot-update",admin=True,archive=archive,sha256=digest,drain_timeout=seconds)
        return digest

    def completion(self):
        state=self.rpc("hot-update-status",admin=True)
        return state if state["phase"] in {"idle","recovery_required"} and state["result"] else None

    def test_drain_then_switch_and_keep_receipts(self):
        a=self.submit("A",True)
        until(lambda:self.status(a)=="running")
        b=self.submit("B")
        digest=self.update(self.package("ok"))
        until(lambda:self.rpc("hot-update-status",admin=True)["phase"]=="draining")
        c=self.submit("C") # same pipe and token while A drains
        self.assertEqual(self.status(b),"queued")
        self.assertEqual(self.status(c),"queued")
        self.exit_gate.touch()
        end=until(self.completion)
        self.assertEqual((end["result"],end["active_sha256"]),("success",digest))
        until(lambda:self.status(c)=="succeeded")
        self.assertEqual([self.status(x) for x in (a,b,c)],["succeeded"]*3)
        self.assertEqual((self.base/"sequence").read_text().splitlines(),["A","B","C"])
        self.assertEqual(json.loads(self.manifest.read_text())["sha256"],digest)
        self.assertEqual(self.rpc("log",receipt=a)["text"],"")
        self.stop()
        self.start()
        self.assertEqual(self.rpc("hot-update-status",admin=True)["active_sha256"],digest)
        self.assertEqual(self.status(c),"succeeded")

    def test_failed_candidate_restores_old_dispatcher(self):
        a=self.submit("A",True)
        until(lambda:self.status(a)=="running")
        b=self.submit("B")
        self.update(self.package("bad",break_start=True))
        until(lambda:self.rpc("hot-update-status",admin=True)["phase"]=="draining")
        self.exit_gate.touch()
        end=until(self.completion)
        self.assertTrue(end["result"].startswith("failed:"),end)
        self.assertEqual(end["active_sha256"],"bundled")
        self.assertFalse(self.manifest.exists())
        until(lambda:self.status(b)=="succeeded")
        self.assertEqual((self.base/"sequence").read_text().splitlines(),["A","B"])

    def test_changed_schema_is_rejected_before_pause(self):
        a=self.submit("A",True)
        until(lambda:self.status(a)=="running")
        self.update(self.package("schema",break_schema=True))
        end=until(self.completion)
        self.assertTrue(end["result"].startswith("failed:"),end)
        self.assertEqual(self.status(a),"running")
        self.exit_gate.touch()
        until(lambda:self.status(a)=="succeeded")

    def test_drain_deadline_never_kills_existing_jobs(self):
        a=self.submit("A",True)
        until(lambda:self.status(a)=="running")
        b=self.submit("B")
        self.update(self.package("deadline"),seconds=1)
        end=until(self.completion)
        self.assertEqual((end["result"],end["active_sha256"]),("failed:TimeoutError","bundled"))
        self.assertEqual((self.status(a),self.status(b)),("running","queued"))
        self.exit_gate.touch()
        until(lambda:self.status(b)=="succeeded")
        self.assertEqual((self.base/"sequence").read_text().splitlines(),["A","B"])


    def test_activation_failure_rolls_back_pointer(self):
        a=self.submit("A",True)
        until(lambda:self.status(a)=="running")
        b=self.submit("B")
        self.update(self.package("bad-resume",break_resume=True))
        until(lambda:self.rpc("hot-update-status",admin=True)["phase"]=="draining")
        self.exit_gate.touch()
        end=until(self.completion)
        self.assertEqual((end["result"],end["active_sha256"]),("rolled_back:activation_failed","bundled"))
        until(lambda:self.status(b)=="succeeded")
        self.assertEqual(json.loads(self.manifest.read_text())["sha256"],"bundled")
        self.assertEqual((self.base/"sequence").read_text().splitlines(),["A","B"])


    def test_bad_digest_and_non_admin_are_rejected(self):
        archive,digest=self.package("bad-hash")
        with self.assertRaisesRegex(ValueError,"request rejected"):
            self.rpc("hot-update",archive=archive,sha256=digest,drain_timeout=1)
        self.update((archive,"0"*64))
        end=until(self.completion)
        self.assertEqual((end["result"],end["active_sha256"]),("failed:ValueError","bundled"))
        a=self.submit("A")
        until(lambda:self.status(a)=="succeeded")
