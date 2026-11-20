"""Read-only Windows AMD HIP UUID+LUID probe. No kernels, services, or queue I/O."""
from dataclasses import asdict, dataclass
import argparse
import ctypes as C
import json
import math
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import struct
import subprocess
import sys
import uuid


class HipProbeError(RuntimeError):
    pass


@dataclass(frozen=True)
class Device:
    ordinal: int
    uuid: str
    luid: str
    node_mask: int


@dataclass(frozen=True)
class VerifiedDevice:
    device: Device  # unmasked association
    visible_ordinal: int
    runtime_dll: str

    def launch_environment(self, base=None):
        """Use the same selector for a future verified-compatible HIP workload."""
        return visibility_environment(base, self.device.ordinal)


class HipUUID(C.Structure):
    # AMD ABI: struct { char bytes[16]; }. Use c_ubyte to retain embedded NULs.
    _fields_ = [("bytes", C.c_ubyte * 16)]


_MAX_DEVICES = 256
_SELECTOR_KEYS = {
    "HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL", "HIP_DEVICE_ORDER", "CUDA_DEVICE_ORDER",
}
_DLL = re.compile(r"amdhip64(?:_[0-9]+)?\.dll", re.IGNORECASE)
_LUID = re.compile(r"0x[0-9a-f]{8}_0x[0-9a-f]{8}")
_DRIVE = re.compile(r"[a-z]:", re.IGNORECASE)


def normalize_uuid(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"(?:[0-9a-f]{32}|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})", value, re.IGNORECASE
    ):
        raise HipProbeError("expected a 16-byte HIP UUID in hexadecimal")
    parsed = uuid.UUID(value)
    if not parsed.int:
        raise HipProbeError("all-zero HIP UUID cannot identify a device")
    return str(parsed)


def format_luid(raw):
    if len(raw) != 8 or not any(raw):
        raise HipProbeError("zero or missing Windows LUID")
    # Win32 LUID is DWORD LowPart, LONG HighPart. WDDM name prints high, low.
    low, high = struct.unpack("<II", raw)
    return f"0x{high:08x}_0x{low:08x}"


def _lexical_dll(value):
    path = PureWindowsPath(os.fspath(value))
    if not path.is_absolute() or not _DRIVE.fullmatch(path.drive) or not _DLL.fullmatch(path.name):
        raise HipProbeError("use an absolute local path to amdhip64.dll or amdhip64_<major>.dll")
    if any(part in ("..", ".") or ":" in part or part.endswith((".", " ")) for part in path.parts[1:]):
        raise HipProbeError("ambiguous HIP DLL path component")
    return path


def trusted_local_dll(value):
    """Accept an operator-supplied trusted path, never PATH-search or follow reparse points."""
    if sys.platform != "win32":
        raise HipProbeError("native HIP runtime probe requires Windows")
    if C.sizeof(C.c_void_p) != 8:
        raise HipProbeError("native Windows HIP runtime probe requires 64-bit Python")
    from ctypes import wintypes as W
    path = _lexical_dll(value)
    get_type = C.WinDLL("kernel32", use_last_error=True).GetDriveTypeW
    get_type.argtypes, get_type.restype = [W.LPCWSTR], W.UINT
    if get_type(path.anchor) != 3:  # DRIVE_FIXED, not mapped remote/removable drive
        raise HipProbeError("HIP runtime must be on a fixed local drive")
    current = Path(path.anchor)
    for index, part in enumerate(path.parts[1:], start=1):
        current = current / part
        try:
            info = current.lstat()
        except OSError as exc:
            raise HipProbeError("HIP runtime path does not exist or cannot be read") from exc
        if info.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise HipProbeError("HIP runtime path cannot contain reparse points")
        final = index == len(path.parts) - 1
        if final and not stat.S_ISREG(info.st_mode):
            raise HipProbeError("HIP runtime is not a regular file")
        if not final and not stat.S_ISDIR(info.st_mode):
            raise HipProbeError("HIP runtime path contains a non-directory")
    return str(current)


def _bind(lib, name, args):
    try:
        fn = getattr(lib, name)
    except AttributeError as exc:
        raise HipProbeError(f"selected HIP runtime does not export {name}") from exc
    fn.argtypes = args
    fn.restype = C.c_int
    return fn


def validate_devices(devices):
    if not 1 <= len(devices) <= _MAX_DEVICES:
        raise HipProbeError("no visible AMD HIP device or invalid device count")
    uuids, luids = set(), set()
    for i, device in enumerate(devices):
        if (type(device.ordinal) is not int or device.ordinal != i
                or type(device.node_mask) is not int or not 0 < device.node_mask <= 0xFFFFFFFF
                or device.uuid != normalize_uuid(device.uuid) or not isinstance(device.luid, str)
                or _LUID.fullmatch(device.luid) is None or device.luid == "0x00000000_0x00000000"):
            raise HipProbeError("invalid HIP device identity or inconsistent ordinal order")
        if device.uuid in uuids:
            raise HipProbeError("ambiguous duplicate HIP UUID")
        if device.luid in luids:
            raise HipProbeError("ambiguous duplicate WDDM LUID; linked adapters not supported")
        uuids.add(device.uuid)
        luids.add(device.luid)
    return tuple(devices)


def enumerate_abi(lib):
    """Bind the documented AMD HIP ABI and return only device identity properties."""
    init = _bind(lib, "hipInit", [C.c_uint])
    count_fn = _bind(lib, "hipGetDeviceCount", [C.POINTER(C.c_int)])
    uuid_fn = _bind(lib, "hipDeviceGetUuid", [C.POINTER(HipUUID), C.c_int])
    luid_fn = _bind(lib, "hipDeviceGetLuid", [C.POINTER(C.c_char), C.POINTER(C.c_uint), C.c_int])

    def check(name, status):
        if status != 0:
            raise HipProbeError(f"{name} failed with HIP status {status}")

    check("hipInit", init(0))
    count = C.c_int()
    check("hipGetDeviceCount", count_fn(C.byref(count)))
    if not 1 <= count.value <= _MAX_DEVICES:
        raise HipProbeError("no visible AMD HIP device or invalid device count")
    output = []
    for n in range(count.value):
        raw_uuid = HipUUID()
        luid = C.create_string_buffer(8)
        node_mask = C.c_uint()
        check(f"hipDeviceGetUuid({n})", uuid_fn(C.byref(raw_uuid), n))
        check(f"hipDeviceGetLuid({n})", luid_fn(luid, C.byref(node_mask), n))
        output.append(Device(n, normalize_uuid(bytes(raw_uuid.bytes).hex()), format_luid(luid.raw), node_mask.value))
    return validate_devices(output)


def _native_enumerate(dll):
    path = trusted_local_dll(dll)
    try:
        # Explicit path + only the DLL's directory and System32 for dependencies.
        lib = C.CDLL(path, winmode=0x00000100 | 0x00000800)
    except OSError as exc:
        raise HipProbeError(f"cannot load trusted HIP DLL/dependencies (Windows error {exc.winerror})") from exc
    return enumerate_abi(lib)


def visibility_environment(base=None, ordinal=None):
    env = {key: value for key, value in (os.environ if base is None else base).items()
           if key.upper() not in _SELECTOR_KEYS}
    if ordinal is not None:
        if type(ordinal) is not int or not 0 <= ordinal < _MAX_DEVICES:
            raise HipProbeError("invalid HIP mask ordinal")
        env["HIP_VISIBLE_DEVICES"] = str(ordinal)
    return env


def _decode_child(raw):
    if len(raw) > 65536:
        raise HipProbeError("oversize response from HIP probe")
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and set(obj) == {"error"} and isinstance(obj["error"], str):
            raise HipProbeError(obj["error"])
        if not isinstance(obj, dict) or set(obj) != {"devices"} or not isinstance(obj["devices"], list):
            raise ValueError()
        return validate_devices([Device(**row) for row in obj["devices"]])
    except (ValueError, TypeError, KeyError) as exc:
        raise HipProbeError("invalid reply from HIP probe") from exc


def _child(dll, env, timeout):
    if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not timeout > 0:
        raise HipProbeError("invalid HIP probe timeout")
    try:
        result = subprocess.run(
            [sys.executable, "-I", str(Path(__file__).resolve()), "--dll", dll, "_child"],
            env=env, check=False, shell=False, cwd=str(Path(dll).parent),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=timeout, creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except subprocess.TimeoutExpired as exc:
        raise HipProbeError("HIP runtime probe timed out") from exc
    except OSError as exc:
        raise HipProbeError("could not start the isolated HIP runtime probe") from exc
    if result.returncode != 0:
        raise HipProbeError("isolated HIP runtime probe exited unexpectedly")
    return _decode_child(result.stdout)


def list_devices(dll, *, base_env=None, timeout=10.0):
    """Enumerate from a fresh unmasked process; do not initialize HIP in the broker."""
    path = trusted_local_dll(dll)
    return _child(path, visibility_environment(base_env), timeout)


def _verify(probe, base_env, expected):
    physical = probe(visibility_environment(base_env))
    matches = [device for device in physical if device.uuid == expected]
    if len(matches) != 1:
        raise HipProbeError("configured AMD GPU UUID is absent or ambiguous")
    target = matches[0]
    visible = probe(visibility_environment(base_env, target.ordinal))
    if len(visible) != 1 or visible[0] != Device(0, target.uuid, target.luid, target.node_mask):
        raise HipProbeError("HIP mask did not expose exactly the expected device as visible ordinal zero")
    return target


def verify_device(dll, gpu_uuid, *, base_env=None, timeout=10.0):
    """Resolve UUID to live ordinal+LUID; validate in an independently initialized HIP process."""
    expected = normalize_uuid(gpu_uuid)
    path = trusted_local_dll(dll)
    target = _verify(lambda env: _child(path, env, timeout), base_env, expected)
    return VerifiedDevice(target, 0, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dll", required=True, help="protected absolute local AMD HIP runtime DLL")
    parser.add_argument("action", choices=("list", "verify", "_child"))
    parser.add_argument("--uuid", help="required for verify; UUID as printed by list")
    args = parser.parse_args()
    if (args.action == "verify") != (args.uuid is not None):
        parser.error("--uuid is required only for verify")
    try:
        if args.action == "_child":
            response = {"devices": [asdict(device) for device in _native_enumerate(args.dll)]}
        elif args.action == "list":
            response = {"devices": [asdict(device) for device in list_devices(args.dll)]}
        else:
            response = asdict(verify_device(args.dll, args.uuid))
        print(json.dumps(response, ensure_ascii=True))
    except HipProbeError as exc:
        if args.action == "_child":
            print(json.dumps({"error": str(exc)}, ensure_ascii=True))
            return 0
        print(f"hip probe: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
