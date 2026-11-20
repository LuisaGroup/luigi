"""A stable local endpoint with versioned, smoothly drained worker children."""
from contextlib import closing
import hashlib
import hmac
import io
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile

from luigi_app.core import gpufifo
from luigi_app.core.auth import _password_key
from luigi_app.core.broker import decode, encode
from luigi_app.platforms import transport
from .worker import ABI, running

LOG = logging.getLogger("luigi.hotupdate")
SOURCE = Path(__file__).resolve().parents[2]
_HEX = re.compile(r"[a-f0-9]{64}")
_FILE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")
_ERROR = encode({"ok":False,"error":"request rejected"})
_OFFLINE = encode({"ok":False,"error":"Luigi worker unavailable; request not retried"})


def _private_dir(path):
    if os.name == "nt":
        path.mkdir(mode=0o700,parents=True,exist_ok=True)
    return transport.private_root(path,initialize=True)


def _read_only(root):
    return sqlite3.connect((root / "queue.sqlite3").as_uri()+"?mode=ro",uri=True,timeout=2)


def _schema(db):
    return db.execute("SELECT type,name,COALESCE(sql,'') FROM sqlite_master ORDER BY type,name").fetchall()


def _checkpoint(path, source_root):
    with closing(_read_only(source_root)) as source, closing(sqlite3.connect(path)) as target:
        source.backup(target)
        if target.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise RuntimeError("checkpoint integrity check failed")
        return _schema(target)


def _publish(path, data):
    tmp = path.with_name(path.name+".tmp")
    with tmp.open("w",encoding="ascii") as f:
        json.dump(data,f,separators=(",",":"))
        f.flush()
        os.fsync(f.fileno())
    if os.name=="nt":
        import ctypes
        from ctypes import wintypes
        replace=ctypes.WinDLL("kernel32",use_last_error=True).MoveFileExW
        replace.argtypes=[wintypes.LPCWSTR,wintypes.LPCWSTR,wintypes.DWORD]
        replace.restype=wintypes.BOOL
        # REPLACE_EXISTING | WRITE_THROUGH; temp and pointer are in one directory.
        if not replace(str(tmp),str(path),0x1 | 0x8):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        os.replace(tmp,path)
        fd=os.open(path.parent,os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)


def stage(archive, digest, releases):
    if not isinstance(digest,str) or not _HEX.fullmatch(digest):
        raise ValueError("SHA-256 must be 64 lowercase hexadecimal characters")
    path=Path(archive)
    if not path.is_absolute():
        raise ValueError("a trusted absolute local archive is required")
    if os.name == "nt" and not re.fullmatch(r"[A-Za-z]:",path.drive):
        raise ValueError("non-local releases are unsupported")
    for part in (path,*path.parents):
        info=part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info,"st_file_attributes",0)&getattr(stat,"FILE_ATTRIBUTE_REPARSE_POINT",0):
            raise ValueError("archive must use a physical local path")
    if not path.is_file():
        raise ValueError("release must be a regular file")
    with path.open("rb") as f:
        content=f.read(12*1024*1024+1)
    if len(content)>12*1024*1024 or not hmac.compare_digest(hashlib.sha256(content).hexdigest(),digest):
        raise ValueError("archive hash or size mismatch")
    dest=releases/uuid.uuid4().hex
    dest.mkdir(mode=0o700)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        total=0
        seen=set()
        entries=z.infolist()
        if len(entries)>1024:
            raise ValueError("too many release files")
        for entry in entries:
            if entry.is_dir():
                continue
            raw=entry.filename
            parts=raw.split("/")
            if (any(not _FILE.fullmatch(p) or (os.name=="nt" and getattr(os.path,"isreserved",lambda p:False)(p)) for p in parts)
                    or raw.casefold() in seen or stat.S_ISLNK(entry.external_attr >> 16)
                    or (raw not in {"luigi.py","worker_entry.py"} and (len(parts)<2 or parts[0]!="luigi_app"))
                    or PurePosixPath(raw).suffix not in {".py",".html",".css",".js"}):
                raise ValueError("invalid release entry")
            seen.add(raw.casefold())
            total+=entry.file_size
            if total>40*1024*1024:
                raise ValueError("expanded release is too large")
            out=dest.joinpath(*parts)
            out.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            with out.open("xb") as f:
                f.write(z.read(entry))
    if not {"worker_entry.py","luigi_app/hotupdate/worker.py","luigi_app/core/broker.py"} <= seen:
        raise ValueError("release lacks the supervised worker")
    return dest


class Child:
    def __init__(self, root, endpoint, release, config):
        self.endpoint=_private_dir(endpoint)
        transport.identity(self.endpoint,initialize=True)
        with (self.endpoint/"worker.log").open("ab") as out:
            self.proc=subprocess.Popen([sys.executable,"-E","-B",str(release/"worker_entry.py"),
                                         str(root),str(endpoint),json.dumps(config)],
                                       stdin=subprocess.PIPE,stdout=out,stderr=out,
                                       cwd=release,creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name=="nt" else 0),
                                       start_new_session=os.name!="nt")
    def cmd(self, word, marker=None, timeout=4):
        if marker:
            (self.endpoint/marker).unlink(missing_ok=True)
        self.proc.stdin.write(word.encode("ascii")+b"\n")
        self.proc.stdin.flush()
        if marker:
            self.wait(marker,timeout)
    def wait(self, marker, timeout):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            if self.proc.poll() is not None:
                raise RuntimeError("worker exited prematurely")
            if (self.endpoint/marker).is_file():
                return
            time.sleep(.025)
        raise TimeoutError("worker readiness timed out")
    def probe(self):
        response=decode(transport.client(self.endpoint,encode({"local_hotupdate_probe":ABI})))
        if response.get("abi") != ABI:
            raise ValueError("worker protocol mismatch")
        return response
    def end(self, *, force=False):
        if self.proc.poll() is None:
            if force:
                self.proc.kill()  # only a never-activated candidate can be force-stopped
            else:
                self.cmd("exit")
            self.proc.wait(timeout=5)
        self.proc.stdin.close()


class Supervisor:
    def __init__(self,root,adapter,policy_options):
        self.root=root
        self.home=_private_dir(root/".h")
        self.releases=_private_dir(self.home/"releases")
        self.pointer=self.home/"active.json"
        self.lock=threading.Lock()
        self.update_lock=threading.Lock()
        self.shutdown=threading.Event()
        self.config={"adapter":adapter,**policy_options}
        self.worker=None
        self.update_thread=None
        self.phase="starting"
        self.digest="bundled"
        self.last=None
    def _identity(self, password):
        with closing(_read_only(self.root)) as db:
            row=db.execute("SELECT salt,verifier FROM admin WHERE id=1").fetchone()
            if row is None or not hmac.compare_digest(_password_key(password,row[0]),row[1]):
                raise ValueError("invalid admin password")
    def _launch(self, path):
        child=Child(self.root,self.home/uuid.uuid4().hex[:10],path,self.config)
        try:
            child.wait("ready",5)
            if child.probe().get("worker") is not False:
                raise RuntimeError("candidate must start paused")
        except BaseException:
            child.end(force=True)
            raise
        return child
    def boot(self):
        path=SOURCE
        if self.pointer.exists():
            state=json.loads(self.pointer.read_text(encoding="ascii"))
            if state!={"release":"bundled","sha256":"bundled"}:
                if not _HEX.fullmatch(state["sha256"]) or not re.fullmatch("[a-f0-9]{32}",state["release"]):
                    raise ValueError("invalid active version pointer")
                path=self.releases/state["release"]
                self.digest=state["sha256"]
        # A previous supervisor may still be draining; never compete for dispatch.
        with gpufifo.worker_lock(self.root,"dispatcher"):
            if running(self.root):
                raise RuntimeError("interrupted jobs require operator review before supervised startup")
        self.worker=self._launch(path)
        self.worker.cmd("start","started")
        self.worker.cmd("resume","resumed")
        self.phase="idle"
    def handle(self,raw):
        try:
            request=decode(raw)
            if not isinstance(request,dict) or set(request)!={"op","credential","data"}:
                return _ERROR
            op=request.get("op")
            if op in {"hot-update","hot-update-status"}:
                self._identity(request.get("credential"))
                data=request.get("data")
                if op=="hot-update-status" and data=={}:
                    return encode({"ok":True,"result":{"phase":self.phase,"active_sha256":self.digest,"result":self.last}})
                if op!="hot-update" or not isinstance(data,dict) or set(data)!={"archive","sha256","drain_timeout"}:
                    return _ERROR
                seconds=data["drain_timeout"]
                if type(seconds) is not int or not 1<=seconds<=86400 or self.phase!="idle" or not self.update_lock.acquire(False):
                    return _ERROR
                self.phase="staging"
                self.last=None
                self.update_thread=threading.Thread(target=self.update,args=(data,),name="luigi-update")
                self.update_thread.start()
                return encode({"ok":True,"result":{"phase":"staging"}})
        except Exception:
            return _ERROR
        with self.lock:
            if self.worker is None or self.worker.proc.poll() is not None:
                return _OFFLINE
            try:
                return transport.client(self.worker.endpoint,raw)
            except (OSError,ValueError,TimeoutError,EOFError):
                return _OFFLINE  # Never replay a possibly committed submit.
    def preflight(self,path):
        with tempfile.NamedTemporaryFile(dir=self.home,delete=False,suffix=".sqlite3") as tf:
            clone=Path(tf.name)
        try:
            schema=_checkpoint(clone,self.root)
            code=("from pathlib import Path; import sys;"
                  "from luigi_app.core.broker import Broker;"
                  "from luigi_app.hotupdate.worker import ABI;"
                  "assert ABI==1; Broker(Path(sys.argv[1]))")
            # Broker needs the standard database basename; allocate a private
            # directory rather than changing a live SQLite file.
            probe=_private_dir(self.home/("probe-"+uuid.uuid4().hex[:10]))
            probe_db=probe/"queue.sqlite3"
            os.replace(clone,probe_db)
            try:
                with (probe/"preflight.log").open("ab") as log:
                    subprocess.run([sys.executable,"-E","-B","-c",code,str(probe)],cwd=path,
                                   stdin=subprocess.DEVNULL,stdout=log,stderr=log,check=True,timeout=12)
                with closing(_read_only(probe)) as db:
                    if _schema(db)!=schema or db.execute("PRAGMA quick_check").fetchone()!=("ok",):
                        raise ValueError("binary hot updates must preserve database schema")
            finally:
                probe_db.unlink(missing_ok=True)
        finally:
            clone.unlink(missing_ok=True)
    def update(self,data):
        old=self.worker
        new=None
        paused=False
        halted=False
        committed=False
        previous_digest=self.digest
        previous_pointer=json.loads(self.pointer.read_text(encoding="ascii")) if self.pointer.exists() else {"release":"bundled","sha256":"bundled"}
        try:
            path=stage(data["archive"],data["sha256"],self.releases)
            self.preflight(path)
            new=self._launch(path)
            self.phase="draining"
            paused=True
            old.cmd("drain","draining")
            until=time.monotonic()+data["drain_timeout"]
            while running(self.root):
                if self.shutdown.is_set() or time.monotonic()>=until:
                    raise TimeoutError("drain deadline reached")
                self.shutdown.wait(.1)
            if self.shutdown.is_set():
                raise InterruptedError("supervisor stopping")
            self.phase="switching"
            old.cmd("halt","halted")
            halted=True
            new.cmd("start","started")
            if new.probe().get("worker") is not True:
                raise RuntimeError("candidate dispatcher not ready")
            # Only this short atomic interval holds the public RPC route lock.
            # Jobs and receipts are already committed in the same SQLite file.
            with self.lock:
                desired={"release":path.name,"sha256":data["sha256"]}
                try:
                    _publish(self.pointer,desired)
                except OSError:
                    # POSIX directory fsync can fail after the atomic rename.
                    # Treat the on-disk decision as committed if it is visible.
                    if self.pointer.exists() and json.loads(self.pointer.read_text(encoding="ascii"))==desired:
                        committed=True
                    raise
                self.worker=new
                self.digest=data["sha256"]
                committed=True
                new.cmd("resume","resumed")
            self.last="success"
            try:
                old.end()
            except Exception:
                LOG.warning("inactive old endpoint did not exit on request")
        except Exception as e:
            self.last="failed:"+type(e).__name__
            if not committed and new is not None:
                try: new.end(force=True)
                except Exception: LOG.error("could not stop standby candidate")
            if not committed and paused and old is not None:
                try:
                    if halted:
                        old.cmd("start","started")
                    old.cmd("resume","resumed")
                except Exception:
                    self.last="manual_recovery_required"
            if committed:
                # Activation can fail before or after a resume reached the child.
                # Take back dispatch only when candidate is paused/exited and the
                # durable reservations prove that no live job can be displaced.
                self.last="manual_recovery_required"
                try:
                    if new.proc.poll() is None:
                        new.cmd("drain","draining")
                        if running(self.root):
                            raise RuntimeError("candidate has running jobs")
                        new.cmd("halt","halted")
                        new.end()
                    if running(self.root):
                        raise RuntimeError("candidate has unresolved reservations")
                    old.cmd("start","started")
                    with self.lock:
                        _publish(self.pointer,previous_pointer)
                        self.worker=old
                        self.digest=previous_digest
                        old.cmd("resume","resumed")
                    self.last="rolled_back:activation_failed"
                except Exception:
                    LOG.error("activation could not be rolled back safely")
            LOG.warning("hot update %s",self.last)
        finally:
            self.phase="recovery_required" if self.last=="manual_recovery_required" else "idle"
            self.update_lock.release()


def serve_supervised(root,adapter="all",*,stop=None,**policy_options):
    transport.require_non_elevated()
    root=transport.private_root(root)
    key=transport.identity(root)
    with gpufifo.worker_lock(root,"broker"):
        supervisor=Supervisor(root,adapter,policy_options)
        listener=None
        def stop_service(*_):
            raise KeyboardInterrupt()
        stop_signal=signal.SIGBREAK if os.name=="nt" else signal.SIGTERM
        previous=signal.signal(stop_signal,stop_service)
        try:
            supervisor.boot()
            listener=transport.listener(root)
            stopping=stop if stop is not None else threading.Event()
            def accept_requests():
                while not stopping.is_set():
                    try:
                        transport.reply_one(listener.accept(),supervisor.handle,key)
                    except (TimeoutError,ValueError,EOFError,OSError):
                        continue
            requests=threading.Thread(target=accept_requests,daemon=True,name="luigi-public-listener")
            requests.start()
            print("Luigi supervised broker ready.",flush=True)
            while not stopping.wait(.2):
                if not requests.is_alive():
                    raise RuntimeError("public listener stopped")
        except KeyboardInterrupt:
            pass
        finally:
            signal.signal(stop_signal,previous)
            supervisor.shutdown.set()
            if supervisor.update_thread is not None:
                supervisor.update_thread.join()
            if listener is not None:
                listener.close()
            if supervisor.worker:
                try: supervisor.worker.end()
                except TimeoutError:
                    # Closing stdin leaves the child to drain, keeping its dispatch lock.
                    supervisor.worker.proc.stdin.close()
    return 0
