"""Windows named pipes and Linux Unix-domain sockets, no pickle."""
import hashlib
import hmac
import secrets
import socket
import stat
import struct
import time
import os
from pathlib import Path
import sys

from luigi_app.core.broker import MAX_FRAME


def address(root):
    normalized = os.path.normcase(str(Path(root).resolve()))
    key = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return r"\\.\pipe\Luigi-" + key


def require_non_elevated():
    if sys.platform.startswith("linux"):
        if not os.getuid() or not os.geteuid() or os.getuid() != os.geteuid() or os.getgid() != os.getegid():
            raise ValueError("run Luigi as a regular, non-setuid Linux user")
        return
    if sys.platform != "win32":
        raise ValueError("Luigi broker supports Windows and Linux")
    import ctypes as C
    from ctypes import wintypes as W
    k = C.WinDLL("kernel32", use_last_error=True)
    a = C.WinDLL("advapi32", use_last_error=True)
    k.GetCurrentProcess.argtypes, k.GetCurrentProcess.restype = [], W.HANDLE
    k.CloseHandle.argtypes, k.CloseHandle.restype = [W.HANDLE], W.BOOL
    a.OpenProcessToken.argtypes = [W.HANDLE, W.DWORD, C.POINTER(W.HANDLE)]
    a.OpenProcessToken.restype = W.BOOL
    a.GetTokenInformation.argtypes = [W.HANDLE, C.c_int, C.c_void_p, W.DWORD,
                                      C.POINTER(W.DWORD)]
    a.GetTokenInformation.restype = W.BOOL
    token = W.HANDLE()
    if not a.OpenProcessToken(k.GetCurrentProcess(), 0x8, C.byref(token)):
        raise C.WinError(C.get_last_error())
    try:
        elevated, size = W.DWORD(), W.DWORD()
        if not a.GetTokenInformation(token, 20, C.byref(elevated),
                                     C.sizeof(elevated), C.byref(size)):
            raise C.WinError(C.get_last_error())
        if elevated.value:
            raise ValueError("an elevated account cannot run Luigi workloads")
    finally:
        k.CloseHandle(token)


def windows_listener(root):
    if sys.platform != "win32":
        raise ValueError("the Luigi local pipe is Windows-only")
    import _winapi
    from multiprocessing.connection import BUFSIZE, PipeListener

    class LocalPipeListener(PipeListener):
        def _new_handle(self, first=False):
            mode = _winapi.PIPE_ACCESS_DUPLEX | _winapi.FILE_FLAG_OVERLAPPED
            if first:
                mode |= _winapi.FILE_FLAG_FIRST_PIPE_INSTANCE
            pipe_mode = (_winapi.PIPE_TYPE_MESSAGE | _winapi.PIPE_READMODE_MESSAGE |
                         _winapi.PIPE_WAIT | 0x8)  # PIPE_REJECT_REMOTE_CLIENTS
            return _winapi.CreateNamedPipe(self._address, mode, pipe_mode,
                                           _winapi.PIPE_UNLIMITED_INSTANCES, BUFSIZE, BUFSIZE,
                                           _winapi.NMPWAIT_WAIT_FOREVER, 0)

    return LocalPipeListener(address(root))


def identity(root, *, initialize=False):
    path = Path(root) / "broker.identity"
    if initialize:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "wb") as out:
                out.write(secrets.token_bytes(32))
                out.flush()
                os.fsync(out.fileno())
    if sys.platform.startswith("linux"):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError("broker identity must be a private user-owned regular file")
            key = f.read(33)
    else:
        with path.open("rb") as f:
            key = f.read(33)
    if len(key) != 32:
        raise ValueError("invalid local broker identity file")
    return key


def _proof(key, nonce):
    return hmac.digest(key, b"Luigi server identity v1\0" + nonce, "sha256")


def reply_one(conn, handler, key):
    # Crucial: never Connection.recv/send, which use pickle.
    with conn:
        if not conn.poll(1):
            return
        nonce = conn.recv_bytes(32)
        if len(nonce) != 32:
            return
        conn.send_bytes(_proof(key, nonce))
        if conn.poll(1):
            conn.send_bytes(handler(conn.recv_bytes(MAX_FRAME)))


def exchange(conn, raw, key):
    nonce = secrets.token_bytes(32)
    conn.send_bytes(nonce)
    if not conn.poll(4) or not hmac.compare_digest(conn.recv_bytes(32), _proof(key, nonce)):
        raise ValueError("local Luigi broker identity verification failed")
    conn.send_bytes(raw)
    if not conn.poll(12):
        raise TimeoutError("Luigi did not answer within 12 seconds")
    return conn.recv_bytes(MAX_FRAME)


def private_root(root, *, initialize=False):
    path = Path(root).expanduser().absolute()
    if not sys.platform.startswith("linux"):
        return path
    require_non_elevated()
    if ".." in path.parts:
        raise ValueError("use a physical Linux spool path without traversal")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            if not initialize:
                raise
            current.mkdir(mode=0o700)
            info = current.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, os.geteuid()):
            raise ValueError("use a physical Linux spool path without symlinks and foreign owners")
        if current != path:
            if info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX:
                raise ValueError("unsafe ancestor of Linux spool")
        elif info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("Linux spool must be a private user-owned directory (0700)")
    if current == Path(path.anchor):
        raise ValueError("the Linux filesystem root is not a Luigi spool")
    return path


def _endpoint(root):
    address = private_root(root) / "broker.sock"
    if len(os.fsencode(address)) >= 108:
        raise ValueError("Linux socket path too long; choose a shorter private --root")
    return address


def _peer(sock):
    _, uid, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    if uid != os.geteuid():
        raise ValueError("Linux peer is not the current user")


class _UnixConnection:
    def __init__(self, sock):
        self.sock = sock
        self.deadline = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.sock.close()

    def poll(self, timeout):
        import select
        self.deadline = time.monotonic() + timeout
        return bool(select.select([self.sock], [], [], timeout)[0])

    def recv_bytes(self, maximum):
        def read(size):
            buf = bytearray()
            while len(buf) < size:
                left = self.deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError("Linux frame deadline exceeded")
                self.sock.settimeout(left)
                chunk = self.sock.recv(size - len(buf))
                if not chunk:
                    raise EOFError()
                buf.extend(chunk)
            return bytes(buf)
        size = struct.unpack("!I", read(4))[0]
        if size > maximum:
            raise ValueError("oversize Linux frame")
        return read(size)

    def send_bytes(self, data):
        self.sock.settimeout(2)
        self.sock.sendall(struct.pack("!I", len(data)) + data)


class _UnixListener:
    def __init__(self, root):
        self.path = _endpoint(root)
        try:
            info = self.path.lstat()
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError("refusing to replace an unsafe Linux socket path")
            with socket.socket(socket.AF_UNIX) as check:
                check.settimeout(1)
                try:
                    check.connect(os.fspath(self.path))
                except ConnectionRefusedError:
                    self.path.unlink()  # private, owner-controlled spool under broker lock
                else:
                    raise ValueError("a Linux broker already listens on this path")
        self.sock = socket.socket(socket.AF_UNIX)
        self.bound = None
        try:
            self.sock.bind(os.fspath(self.path))
            info = self.path.lstat()
            self.bound = (info.st_dev, info.st_ino)
            self.path.chmod(0o600)
            self.sock.listen(16)
            self.sock.settimeout(.25)
        except BaseException:
            self.close()
            raise

    def accept(self):
        sock, _ = self.sock.accept()
        try:
            _peer(sock)
            return _UnixConnection(sock)
        except BaseException:
            sock.close()
            raise

    def close(self):
        self.sock.close()
        try:
            info = self.path.lstat()
            if self.bound == (info.st_dev, info.st_ino) and stat.S_ISSOCK(info.st_mode):
                self.path.unlink()
        except FileNotFoundError:
            pass


def listener(root):
    return _UnixListener(root) if sys.platform.startswith("linux") else windows_listener(root)


def client(root, raw):
    if sys.platform.startswith("linux"):
        path = _endpoint(root)
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("Linux socket must be a private user-owned socket")
        key = identity(root)
        with socket.socket(socket.AF_UNIX) as sock:
            sock.settimeout(2)
            sock.connect(os.fspath(path))
            _peer(sock)
            return exchange(_UnixConnection(sock), raw, key)
    from multiprocessing.connection import Client
    key = identity(root)
    with Client(address(root), family="AF_PIPE", authkey=None) as conn:
        return exchange(conn, raw, key)
