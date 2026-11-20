"""Luigi launch-only busy gate, stdlib; no scheduler, database, or service."""
from dataclasses import dataclass
import ctypes
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time


@dataclass(frozen=True)
class Rule:
    busy_at: float = 80.0
    recover_at: float = 60.0
    recover_samples: int = 2
    interval: float = 1.0
    stale_after: float = 4.0

    def __post_init__(self):
        if not (0 <= self.recover_at < self.busy_at <= 100
                and type(self.recover_samples) is int and self.recover_samples >= 1
                and 0 < self.interval < self.stale_after < float("inf")):
            raise ValueError("invalid load guard rule")


@dataclass(frozen=True)
class Reading:
    percent: float | None
    detail: str


@dataclass(frozen=True)
class Sample:
    percent: float | None
    observed_unix: float
    observed_mono: float
    detail: str


@dataclass(frozen=True)
class Metric:
    allowed: bool
    reason: str
    sample: Sample | None
    age_seconds: float | None


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    cpu: Metric
    gpu: Metric


class _Gate:
    def __init__(self, rule, probe):
        self.rule = rule
        self.probe = probe
        self.latest = None
        self.last_started = -float("inf")
        self.polling = False
        self.ready = False
        self.low_count = 0

    def publish(self, sample):
        if self.latest is None or sample.observed_mono - self.latest.observed_mono > self.rule.stale_after:
            self.ready = False
            self.low_count = 0
        self.latest = sample
        p = sample.percent
        if p is None or p >= self.rule.busy_at:
            self.ready = False
            self.low_count = 0
        elif not self.ready:
            self.low_count = self.low_count + 1 if p <= self.rule.recover_at else 0
            self.ready = self.low_count >= self.rule.recover_samples

    def metric(self, now):
        if self.latest is None:
            return Metric(False, "no_sample", None, None)
        sample = self.latest
        age = max(0.0, now - sample.observed_mono)
        if age > self.rule.stale_after:
            return Metric(False, "stale", sample, age)
        if sample.percent is None:
            return Metric(False, "unavailable", sample, age)
        if self.ready:
            return Metric(True, "ready", sample, age)
        if sample.percent >= self.rule.busy_at:
            return Metric(False, "busy", sample, age)
        return Metric(False, f"recovering_{self.low_count}_of_{self.rule.recover_samples}", sample, age)


class LoadGuard:
    def __init__(self, cpu_probe, gpu_probe, *, cpu_rule=Rule(),
                 gpu_rule=Rule(interval=5.0, stale_after=13.0),
                 gpu_requires_cpu=True, monotonic=time.monotonic, wall=time.time):
        self._gates = {"cpu": _Gate(cpu_rule, cpu_probe), "gpu": _Gate(gpu_rule, gpu_probe)}
        self._gpu_requires_cpu = gpu_requires_cpu
        self._monotonic = monotonic
        self._wall = wall
        self._lock = threading.Lock()

    @classmethod
    def windows(cls, *, adapter_luid=None, gpu_engine_types=None, **options):
        # Call from the trusted worker, not from the submitting process.
        cpu_rule = options.get("cpu_rule", Rule())
        return cls(WindowsCpu(max_window=cpu_rule.stale_after), WindowsGpu(adapter_luid, gpu_engine_types), **options)

    def refresh(self, metric):
        """Poll once if due. May block (GPU subprocess); call outside the DB lock."""
        gate = self._gates[metric]
        with self._lock:
            started = self._monotonic()
            if gate.polling or started - gate.last_started < gate.rule.interval:
                return False
            gate.polling = True
            gate.last_started = started
            wall = self._wall()
        try:
            try:
                reading = gate.probe()
                value = reading.percent
                if value is not None and (not math.isfinite(value) or not 0 <= value <= 100):
                    reading = Reading(None, "invalid_percentage")
            except Exception as exc:
                reading = Reading(None, "probe_error:" + type(exc).__name__)
            with self._lock:
                # Timestamp at collection start so slow collection is not given extra freshness.
                gate.publish(Sample(reading.percent, wall, started, reading.detail))
            return True
        finally:
            with self._lock:
                gate.polling = False

    def decision(self, lane):
        """Read the bounded in-memory snapshot, without I/O or touching recovery counters."""
        if lane not in ("cpu", "gpu"):
            raise ValueError("unknown lane")
        with self._lock:
            stamp = self._monotonic()
            cpu, gpu = (self._gates[key].metric(stamp) for key in ("cpu", "gpu"))
        required = (("cpu", cpu),) if lane == "cpu" else (
            (("cpu", cpu), ("gpu", gpu)) if self._gpu_requires_cpu else (("gpu", gpu),)
        )
        blockers = [name + "_" + reading.reason for name, reading in required if not reading.allowed]
        return Decision(not blockers, ",".join(blockers) if blockers else "ready", cpu, gpu)


def cpu_busy(before, after):
    delta = tuple(b - a for a, b in zip(before, after))
    idle, kernel, user = delta
    total = kernel + user
    if min(delta) < 0 or total <= 0 or idle > kernel:
        return None
    return 100.0 * (total - idle) / total


class WindowsCpu:
    def __init__(self, *, max_window=4.0):
        self._last = None
        self._max_window = max_window
        self._api = None

    def __call__(self):
        if os.name != "nt":
            return Reading(None, "windows_required")
        from ctypes import wintypes
        if self._api is None:
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            pointer = ctypes.POINTER(wintypes.FILETIME)
            kernel.GetSystemTimes.argtypes = [pointer, pointer, pointer]
            kernel.GetSystemTimes.restype = wintypes.BOOL
            kernel.GetActiveProcessorGroupCount.argtypes = []
            kernel.GetActiveProcessorGroupCount.restype = wintypes.WORD
            self._api = kernel
        if self._api.GetActiveProcessorGroupCount() != 1:
            self._last = None
            return Reading(None, "cpu_processor_groups_not_supported")
        times = [wintypes.FILETIME() for _ in range(3)]
        if not self._api.GetSystemTimes(*(ctypes.byref(value) for value in times)):
            self._last = None
            return Reading(None, "get_system_times_failed")
        ticks = tuple((value.dwHighDateTime << 32) | value.dwLowDateTime for value in times)
        stamp = time.monotonic()
        before, self._last = self._last, (stamp, ticks)
        if before is None or stamp - before[0] > self._max_window:
            return Reading(None, "cpu_warmup")
        percent = cpu_busy(before[1], ticks)
        return Reading(percent, "get_system_times" if percent is not None else "invalid_cpu_delta")


_LUID = re.compile(r"0x[0-9a-f]{1,8}_0x[0-9a-f]{1,8}", re.IGNORECASE)
_ENGINE = re.compile(
    r"pid_(?P<pid>[0-9]+)_luid_(?P<luid>0x[0-9a-f]{1,8}_0x[0-9a-f]{1,8})"
    r"_phys_(?P<phys>[0-9]+)_eng_(?P<eng>[0-9]+)_engtype_(?P<type>[^#]+)(?:#[0-9]+)?",
    re.IGNORECASE,
)


def parse_luid(luid):
    if not isinstance(luid, str) or _LUID.fullmatch(luid) is None:
        raise ValueError("adapter LUID must contain high and low hexadecimal parts")
    return tuple(int(part, 16) for part in luid.split("_"))


def gpu_busy(rows, adapter, engine_types=None):
    """Use the busiest physical engine, aggregating distinct PIDs within that engine only."""
    totals = {}
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("Name"), str):
            return Reading(None, "invalid_gpu_instance")
        name = row["Name"]
        m = _ENGINE.fullmatch(name)
        if m is None:
            # Do not hide missing utilization when part of the target instance layout is unfamiliar.
            if any(adapter is None or parse_luid(part.group()) == adapter for part in _LUID.finditer(name)):
                return Reading(None, "unknown_target_engine_layout")
            continue
        identity = parse_luid(m["luid"])
        if adapter is not None and identity != adapter:
            continue
        typ = m["type"].casefold()
        if engine_types is not None and typ not in engine_types:
            continue
        engine = (*identity, int(m["phys"]), int(m["eng"]))
        descriptor = (engine, int(m["pid"]))
        if descriptor in seen:
            return Reading(None, "duplicate_target_engine_instance")
        seen.add(descriptor)
        raw = row.get("UtilizationPercentage")
        # WMI uint64 normally serializes to a JSON number.
        if type(raw) not in (int, float) or not 0 <= raw <= 100:
            return Reading(None, "invalid_target_engine_percentage")
        totals[engine] = totals.get(engine, 0) + raw
    if not totals:
        return Reading(None, "no_target_engine_sample")
    engine = max(totals, key=totals.get)
    return Reading(min(100.0, totals[engine]), f"wddm_luid_0x{engine[0]:08x}_0x{engine[1]:08x}_phys_{engine[2]}_engine_{engine[3]}")


_GPU_QUERY = r"""
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
Import-Module -Name (Join-Path $PSHOME 'Modules\CimCmdlets\CimCmdlets.psd1')
$items = @(Get-CimInstance -Namespace root/cimv2 -ClassName Win32_PerfFormattedData_GPUPerformanceCounters_GPUEngine |
           Select-Object Name, UtilizationPercentage)
ConvertTo-Json -Compress -InputObject $items
"""


class WindowsGpu:
    def __init__(self, adapter_luid, engine_types=None, *, timeout=4.0):
        self._adapter = None if adapter_luid is None else parse_luid(adapter_luid)
        self._types = None if engine_types is None else frozenset(x.casefold() for x in engine_types)
        if self._types is not None and (not self._types or "" in self._types):
            raise ValueError("engine_types must contain at least one non-empty engine type")
        if not 0 < timeout < float("inf"):
            raise ValueError("invalid GPU sample timeout")
        self._timeout = timeout

    def __call__(self):
        if os.name != "nt":
            return Reading(None, "windows_required")
        from ctypes import wintypes
        get_dir = ctypes.WinDLL("kernel32", use_last_error=True).GetSystemDirectoryW
        get_dir.argtypes = [wintypes.LPWSTR, wintypes.UINT]
        get_dir.restype = wintypes.UINT
        buf = ctypes.create_unicode_buffer(32768)
        size = get_dir(buf, len(buf))
        if not 0 < size < len(buf):
            return Reading(None, "system_directory_unavailable")
        exe = Path(buf.value, "WindowsPowerShell", "v1.0", "powershell.exe")
        try:
            result = subprocess.run(
                [str(exe), "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", _GPU_QUERY],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                cwd=str(exe.parent), timeout=self._timeout, creationflags=subprocess.CREATE_NO_WINDOW,
                text=True, encoding="utf-8-sig", shell=False, check=False,
            )
        except subprocess.TimeoutExpired:
            return Reading(None, "gpu_counter_timeout")
        if result.returncode != 0:
            return Reading(None, "gpu_counter_query_failed")
        try:
            rows = json.loads(result.stdout)
        except (UnicodeError, ValueError):
            return Reading(None, "invalid_gpu_counter_json")
        if not isinstance(rows, list):
            return Reading(None, "invalid_gpu_counter_shape")
        return gpu_busy(rows, self._adapter, self._types)
