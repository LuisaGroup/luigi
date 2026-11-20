"""Windows-local, no-pickle protocol transport for the cooperative broker."""
import hashlib
import hmac
import secrets
import os
from pathlib import Path
import sys

from broker import MAX_FRAME


def address(root):
    normalized = os.path.normcase(str(Path(root).resolve()))
    key = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return r"\\.\pipe\Luigi-" + key


def require_non_elevated():
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


def client(root, raw):
    from multiprocessing.connection import Client
    key = identity(root)
    with Client(address(root), family="AF_PIPE", authkey=None) as conn:
        return exchange(conn, raw, key)
