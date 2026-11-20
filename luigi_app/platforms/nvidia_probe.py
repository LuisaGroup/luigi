"""Fail-closed cooperative full-device NVIDIA routing. No discovery on import.

Only operator-selected absolute local binaries/libraries are executed/loaded.
MIG, MPS and container device namespaces are outside this provider's scope.
"""
from dataclasses import dataclass
from datetime import datetime
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import subprocess
import sys
import time


class NvidiaProbeError(RuntimeError):
    pass


_UUID = re.compile(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\Z")
_QUERY = ('--query-gpu=timestamp,uuid,pci.bus_id,utilization.gpu,mig.mode.current,mig.mode.pending',
          '--format=csv,noheader,nounits')


def normalize_uuid(value):
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise NvidiaProbeError('expected a full NVIDIA GPU UUID; ordinals, prefixes and MIG are unsupported')
    if not any(c != '0' for c in value[4:].replace('-', '')):
        raise NvidiaProbeError('all-zero NVIDIA UUID is invalid')
    return 'GPU-' + value[4:].lower()


def normalize_pci(value):
    match = re.fullmatch(r'([0-9a-fA-F]{4,8}):([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-7])', value)
    if not match or int(match[3], 16) > 31:
        raise NvidiaProbeError('invalid PCI bus identity')
    return f'{int(match[1], 16):04x}:{match[2].lower()}:{match[3].lower()}.{match[4]}'


def _windows_local_file(value):
    import ctypes
    from ctypes import wintypes
    raw = os.fspath(value)
    path = PureWindowsPath(raw)
    if (not path.is_absolute() or not re.fullmatch(r'[a-zA-Z]:', path.drive)
            or any(part in ('.', '..') or ':' in part or part.endswith(('.', ' '))
                   for part in raw.replace('/', '\\').split('\\')[1:])):
        raise NvidiaProbeError('expected an ordinary absolute local Windows file path')
    if ctypes.sizeof(ctypes.c_void_p) != 8:
        raise NvidiaProbeError('NVIDIA Windows proof requires 64-bit Python')
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    get_type = kernel.GetDriveTypeW
    get_type.argtypes, get_type.restype = [wintypes.LPCWSTR], wintypes.UINT
    if get_type(path.anchor) != 3:
        raise NvidiaProbeError('NVIDIA file must be on a fixed local drive')
    current = Path(path.anchor)
    for index, part in enumerate(path.parts[1:], start=1):
        current /= part
        info = current.lstat()
        if info.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise NvidiaProbeError('NVIDIA file path contains a reparse point')
        final = index == len(path.parts) - 1
        if not (stat.S_ISREG(info.st_mode) if final else stat.S_ISDIR(info.st_mode)):
            raise NvidiaProbeError('NVIDIA file path has a nonordinary component')
    return str(current)


def _local_file(value):
    if sys.platform == 'win32':
        return _windows_local_file(value)
    path = Path(value)
    if not path.is_absolute() or str(path).startswith(('//', '\\\\')):
        raise NvidiaProbeError('operator path must be absolute and local')
    path = path.resolve(strict=True)
    if not path.is_file():
        raise NvidiaProbeError('operator path is not a regular file')
    return str(path)


def _digest(path):
    with open(path, 'rb') as file:
        digest = hashlib.sha256()
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)
        return digest.digest()


def clean_environment(base=None):
    # Do not let user loader, Python, mask, MPS or NVML settings redirect proofs.
    allow = {'SYSTEMROOT', 'WINDIR', 'SYSTEMDRIVE', 'USERPROFILE', 'LOCALAPPDATA',
             'APPDATA', 'PROGRAMDATA', 'HOME', 'USER', 'USERNAME', 'TMP', 'TEMP',
             'TMPDIR', 'LANG', 'LC_ALL'}
    env = {k: v for k, v in (os.environ if base is None else base).items() if k.upper() in allow}
    env['LC_ALL'] = 'C'
    env['LANG'] = 'C'
    return env


@dataclass(frozen=True)
class NvidiaDevice:
    uuid: str
    pci_bus_id: str
    percent: float
    observed_at: float


def parse_inventory(text, started, finished):
    """Parse one fresh synchronous nvidia-smi CSV response, never cached output."""
    if not math.isfinite(started) or not math.isfinite(finished) or finished < started or finished - started > 5:
        raise NvidiaProbeError('stale NVIDIA sample')
    if not isinstance(text, str) or len(text) > 1024 * 1024:
        raise NvidiaProbeError('invalid NVIDIA response')
    result, uuids, buses = [], set(), set()
    try:
        for row in csv.reader(io.StringIO(text), strict=True):
            if len(row) != 6:
                raise NvidiaProbeError('unexpected NVIDIA query schema')
            stamp, uuid, pci, load, current, pending = (x.strip() for x in row)
            observed = datetime.strptime(stamp, '%Y/%m/%d %H:%M:%S.%f').timestamp()
            if not started - 2 <= observed <= finished + 2:
                raise NvidiaProbeError('stale NVIDIA sample timestamp')
            uuid, pci = normalize_uuid(uuid), normalize_pci(pci)
            # N/A is the documented not-supported response on non-MIG GPUs.
            # Only the pair N/A/N/A or Disabled/Disabled is accepted.
            if (current, pending) not in {('Disabled', 'Disabled'), ('N/A', 'N/A'), ('[N/A]', '[N/A]')}:
                raise NvidiaProbeError('MIG enabled, pending or unknown')
            if not re.fullmatch(r'\d+(?:\.\d+)?', load):
                raise NvidiaProbeError('unsupported NVIDIA utilization')
            percent = float(load)
            if not math.isfinite(percent) or not 0 <= percent <= 100:
                raise NvidiaProbeError('invalid NVIDIA utilization')
            if uuid in uuids or pci in buses:
                raise NvidiaProbeError('ambiguous NVIDIA identity')
            uuids.add(uuid)
            buses.add(pci)
            result.append(NvidiaDevice(uuid, pci, percent, observed))
    except (ValueError, csv.Error, OverflowError) as exc:
        raise NvidiaProbeError('malformed NVIDIA response') from exc
    if not result:
        raise NvidiaProbeError('no NVIDIA devices discovered')
    return tuple(result)


class NvidiaProvider:
    def __init__(self, smi_path, driver_path=None, workloads=(), *,
                 runner=subprocess.run, monotonic=time.monotonic, wall_time=time.time):
        if sys.platform not in ('linux', 'win32'):
            raise NvidiaProbeError('unsupported NVIDIA platform')
        self._run, self._now, self._wall = runner, monotonic, wall_time
        self._smi = _local_file(smi_path)
        if Path(self._smi).name.lower() not in ('nvidia-smi', 'nvidia-smi.exe'):
            raise NvidiaProbeError('expected an explicitly selected nvidia-smi binary')
        self._pins = {self._smi: _digest(self._smi)}
        self._driver = _local_file(driver_path) if driver_path else None
        if self._driver:
            self._pins[self._driver] = _digest(self._driver)
        self._workloads = {_local_file(app) for app in workloads}
        for app in self._workloads:
            self._pins[app] = _digest(app)

    @property
    def can_launch(self):
        return bool(self._driver and self._workloads)

    def _check_pins(self):
        try:
            if any(_local_file(path) != path or _digest(path) != digest for path, digest in self._pins.items()):
                raise NvidiaProbeError('operator-selected NVIDIA file changed; reconfigure provider')
        except OSError as exc:
            raise NvidiaProbeError('operator-selected NVIDIA file unavailable') from exc

    def _execute(self, argv, env, cwd):
        start = self._now()
        try:
            kwargs = dict(shell=False, check=False, capture_output=True, text=True,
                          encoding='utf-8', errors='strict', timeout=4, env=env, cwd=cwd)
            if sys.platform == 'win32':
                kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
            response = self._run(argv, **kwargs)
            elapsed = self._now() - start
            if response.returncode or response.stderr.strip() or not 0 <= elapsed <= 5:
                raise NvidiaProbeError('NVIDIA probe failed or timed out')
            return response.stdout
        except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
            raise NvidiaProbeError('NVIDIA probe unavailable') from exc

    def inventory_load(self):
        self._check_pins()
        started = self._wall()
        text = self._execute([self._smi, *_QUERY], clean_environment(), str(Path(self._smi).parent))
        result = parse_inventory(text, started, self._wall())
        self._check_pins()
        return result

    def verify_device(self, uuid):
        uuid = normalize_uuid(uuid)
        matches = [row for row in self.inventory_load() if row.uuid == uuid]
        if len(matches) != 1:
            raise NvidiaProbeError('requested NVIDIA UUID is not uniquely discoverable')
        return matches[0]

    def validate_submission(self, argv):
        if not self._driver or not self._workloads:
            raise NvidiaProbeError('NVIDIA provider is discovery-only: driver and approved workloads required')
        if not argv or _local_file(argv[0]) not in self._workloads:
            raise NvidiaProbeError('workload is not operator-approved for CUDA UUID routing')
        self._check_pins()
        app = _local_file(argv[0])
        # A sibling driver could make workload loading differ from the proof.
        for sibling in Path(app).parent.iterdir():
            if sibling.name.lower() == 'nvcuda.dll' or sibling.name.startswith('libcuda.so'):
                if str(sibling.resolve()) != self._driver:
                    raise NvidiaProbeError('unverified application-local CUDA driver')
        return app

    def launch_environment(self, uuid, argv, base_env=None, *, busy_at=80.0):
        if isinstance(busy_at, bool) or not isinstance(busy_at, (int, float)) or not math.isfinite(busy_at) or not 0 < busy_at <= 100:
            raise NvidiaProbeError('invalid GPU busy threshold')
        app = self.validate_submission(argv)
        device = self.verify_device(uuid)
        if device.percent >= busy_at:
            raise NvidiaProbeError('NVIDIA GPU is busy')
        env = clean_environment(base_env)
        env['CUDA_VISIBLE_DEVICES'] = device.uuid
        env['CUDA_DEVICE_ORDER'] = 'PCI_BUS_ID'
        # Isolated interpreter; same sanitized environment/cwd as the workload.
        output = self._execute([str(Path(sys.executable).resolve()), '-I',
                                str(Path(__file__).resolve()), '--cuda-proof', self._driver],
                               env, str(Path(app).parent))
        try:
            proof = json.loads(output)
            if (not isinstance(proof, dict) or set(proof) != {'uuid', 'pci_bus_id'} or
                    normalize_uuid(proof['uuid']) != device.uuid or
                    normalize_pci(proof['pci_bus_id']) != device.pci_bus_id):
                raise NvidiaProbeError('masked CUDA device proof mismatch')
        except (ValueError, TypeError, KeyError) as exc:
            raise NvidiaProbeError('invalid masked CUDA proof') from exc
        fresh = self.verify_device(device.uuid)
        if fresh.percent >= busy_at:
            raise NvidiaProbeError('NVIDIA GPU became busy before launch')
        if fresh.pci_bus_id != device.pci_bus_id:
            raise NvidiaProbeError('NVIDIA mapping changed before launch')
        self._check_pins()
        return env


def _cuda_proof(driver_path):
    """Read driver enumeration in a short-lived process; never creates a context."""
    import ctypes
    import uuid
    path = _local_file(driver_path)
    library = (ctypes.WinDLL(path, winmode=0x1100) if sys.platform == 'win32'
               else ctypes.CDLL(path, mode=ctypes.RTLD_LOCAL))

    def function(name, args):
        fn = getattr(library, name)
        fn.argtypes, fn.restype = args, ctypes.c_int
        return fn

    def checked(fn, *args):
        if fn(*args) != 0:
            raise NvidiaProbeError('CUDA driver enumeration failed')

    integer = ctypes.c_int
    checked(function('cuInit', [ctypes.c_uint]), 0)
    count = integer()
    checked(function('cuDeviceGetCount', [ctypes.POINTER(integer)]), ctypes.byref(count))
    if count.value != 1:
        raise NvidiaProbeError('CUDA mask did not expose exactly one device')
    device = integer()
    checked(function('cuDeviceGet', [ctypes.POINTER(integer), integer]), ctypes.byref(device), 0)
    class UUID(ctypes.Structure):
        _fields_ = [('bytes', ctypes.c_ubyte * 16)]
    ident = UUID()
    name = 'cuDeviceGetUuid_v2' if hasattr(library, 'cuDeviceGetUuid_v2') else 'cuDeviceGetUuid'
    checked(function(name, [ctypes.POINTER(UUID), integer]), ctypes.byref(ident), device)
    pci = ctypes.create_string_buffer(32)
    checked(function('cuDeviceGetPCIBusId', [ctypes.c_char_p, integer, integer]), pci, len(pci), device)
    return {'uuid': 'GPU-' + str(uuid.UUID(bytes=bytes(ident.bytes))),
            'pci_bus_id': normalize_pci(pci.value.decode('ascii'))}


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '--cuda-proof':
        raise SystemExit('only the internal CUDA proof entry point is supported')
    try:
        print(json.dumps(_cuda_proof(sys.argv[2]), sort_keys=True))
    except Exception:
        raise SystemExit('CUDA proof failed')
