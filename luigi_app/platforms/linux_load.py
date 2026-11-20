"""Read-only Linux load probes for Luigi (procfs CPU and amdgpu PCI sysfs only)."""
from collections.abc import Mapping
from dataclasses import dataclass
import math
from pathlib import Path
import re
import sys
import time

from luigi_app.platforms.loadguard import LoadGuard, Reading, Rule


_TICK = re.compile(r"[0-9]{1,20}", re.ASCII)
_HEX = re.compile(r"0x[0-9a-f]{1,8}", re.ASCII | re.IGNORECASE)
_PCI = re.compile(r"[0-9a-f]{4,8}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", re.ASCII | re.IGNORECASE)


def _line(path, limit=128):
    with path.open("r", encoding="ascii") as stream:
        row = stream.readline(limit + 1)
    if not row.strip() or len(row) > limit:
        raise ValueError("empty or oversized counter")
    return row.strip()


def parse_proc_cpu(row):
    """Parse only the aggregate cpu line; guest fields duplicate time in user/nice."""
    items = row.split()
    if not 9 <= len(items) <= 11 or items[0] != "cpu" or not all(_TICK.fullmatch(x) for x in items[1:]):
        raise ValueError("unrecognized /proc/stat cpu line")
    ticks = tuple(int(x) for x in items[1:])
    if any(x > 2**64 - 1 for x in ticks):
        raise ValueError("invalid cpu counter width")
    return ticks[:8]


def proc_cpu_percent(before, after):
    if len(before) != 8 or len(after) != 8:
        return None
    delta = tuple(new - old for old, new in zip(before, after))
    total = sum(delta)
    if min(delta) < 0 or total <= 0:
        return None
    # idle and I/O wait are not CPU execution; steal is unavailable capacity.
    idle = delta[3] + delta[4]
    return 100.0 * (total - idle) / total


class LinuxCpu:
    def __init__(self, path="/proc/stat", *, max_window=4.0, clock=time.monotonic):
        if not math.isfinite(max_window) or max_window <= 0:
            raise ValueError("invalid CPU observation window")
        self._path = Path(path)
        self._max_window = max_window
        self._clock = clock
        self._last = None

    def __call__(self):
        try:
            ticks = parse_proc_cpu(_line(self._path, 512))
        except (OSError, UnicodeError, ValueError):
            self._last = None
            return Reading(None, "linux_proc_stat_unavailable")
        stamp = self._clock()
        prev, self._last = self._last, (stamp, ticks)
        if prev is None or not 0 < stamp - prev[0] <= self._max_window:
            return Reading(None, "linux_cpu_warmup")
        value = proc_cpu_percent(prev[1], ticks)
        return Reading(value, "linux_proc_stat" if value is not None else "linux_cpu_invalid_delta")


def _pci_value(path):
    value = _line(path)
    if not _HEX.fullmatch(value):
        raise ValueError("invalid PCI attribute")
    return int(value, 16)


@dataclass(frozen=True)
class PciGpu:
    bdf: str
    vendor: int
    driver: str | None
    _path: Path


def canonical_bdf(value):
    if not isinstance(value, str) or not _PCI.fullmatch(value):
        raise ValueError("invalid PCI BDF")
    domain, bus, slot_function = value.lower().split(":")
    slot, function = slot_function.split(".")
    if int(slot, 16) > 31:
        raise ValueError("invalid PCI device field")
    return f"{int(domain, 16):04x}:{bus}:{slot}.{function}"


def linux_pci_inventory(pci_root="/sys/bus/pci/devices"):
    """Current kernel PCI GPU inventory; BDF is transient, never a queue identity."""
    result = {}
    for entry in Path(pci_root).iterdir():
        if not _PCI.fullmatch(entry.name):
            continue
        gpu_class = _pci_value(entry / "class")
        if not 0 <= gpu_class <= 0xFFFFFF:
            raise ValueError("invalid PCI class")
        if gpu_class >> 16 not in (0x03, 0x12):  # display and processing accelerators
            continue
        bdf = canonical_bdf(entry.name)
        vendor = _pci_value(entry / "vendor")
        if bdf in result or not 0 < vendor <= 0xFFFF:
            raise ValueError("duplicate PCI address or invalid vendor")
        try:
            driver = (entry / "driver").readlink().name
        except FileNotFoundError:
            driver = None
        result[bdf] = PciGpu(bdf, vendor, driver, entry)
    return tuple(result[key] for key in sorted(result))


def amdgpu_sample(device):
    """Read the metric from an inventory object, never from an agent-supplied path."""
    if device.vendor != 0x1002:
        return Reading(None, "linux_gpu_provider_unsupported")
    if device.driver != "amdgpu":
        return Reading(None, "linux_gpu_driver_unsupported")
    try:
        value = _line(device._path / "gpu_busy_percent")
        if not re.fullmatch(r"[0-9]{1,3}", value, re.ASCII) or not 0 <= int(value) <= 100:
            return Reading(None, "linux_gpu_busy_percent_invalid")
        return Reading(float(value), "linux_amdgpu_sysfs")
    except UnicodeError:
        return Reading(None, "linux_gpu_busy_percent_invalid")
    except (OSError, ValueError):
        return Reading(None, "linux_gpu_busy_percent_unavailable")


class LinuxPciAggregate:
    """Unbound lane gate: complete PCI inventory matched to current samples, never ordinal mapping.

    nvidia_readings, when supplied by trusted worker config, must return a
    fresh Mapping[BDF, numeric percent]. Provider authenticates its executable,
    stable UUIDs, parsing, and freshness, before handing off this sample batch.
    """

    def __init__(self, pci_root="/sys/bus/pci/devices", *, nvidia_readings=None):
        self._root = Path(pci_root)
        self._nvidia_readings = nvidia_readings

    def __call__(self):
        try:
            cards = linux_pci_inventory(self._root)
        except (OSError, UnicodeError, ValueError):
            return Reading(None, "linux_gpu_inventory_unavailable")
        if not cards:
            return Reading(None, "linux_no_pci_gpu")

        samples = []
        nvidia = set()
        for card in cards:
            if card.vendor == 0x1002:
                sample = amdgpu_sample(card)
                if sample.percent is None:
                    return sample
                samples.append(sample.percent)
            elif card.vendor == 0x10DE and self._nvidia_readings is not None:
                if card.driver != "nvidia":
                    return Reading(None, "linux_gpu_driver_unsupported")
                nvidia.add(card.bdf)
            else:
                return Reading(None, "linux_gpu_provider_unsupported")

        if nvidia:
            try:
                batch = self._nvidia_readings()
                if not isinstance(batch, Mapping):
                    raise ValueError("expected provider mapping")
                canonical = {}
                for bdf, value in batch.items():
                    key = canonical_bdf(bdf)
                    if key in canonical or type(value) not in (int, float) or not 0 <= value <= 100:
                        raise ValueError("invalid provider row")
                    canonical[key] = value
                if nvidia != set(canonical):
                    raise ValueError("provider inventory does not match sysfs")
                samples.extend(canonical.values())
            except (OSError, RuntimeError, ValueError, TypeError):
                return Reading(None, "linux_nvidia_telemetry_unavailable")

        # Recheck inventory after collection. No launch guarantee against
        # external changes after the sample; use the normal stale policy too.
        try:
            if linux_pci_inventory(self._root) != cards:
                return Reading(None, "linux_gpu_inventory_changed")
        except (OSError, UnicodeError, ValueError):
            return Reading(None, "linux_gpu_inventory_unavailable")

        return Reading(float(max(samples)), "linux_pci_gpu_max" if nvidia else "linux_amdgpu_sysfs_max")


class LinuxAmdGpu(LinuxPciAggregate):
    """Fail-closed AMD-only default with no external provider subprocess."""
    def __init__(self, pci_root="/sys/bus/pci/devices"):
        super().__init__(pci_root)


def linux_guard(*, proc_stat="/proc/stat", pci_root="/sys/bus/pci/devices", gpu_probe=None, **options):
    """Use existing cached launch policy and telemetry shape, no worker/DB ownership."""
    if not sys.platform.startswith("linux"):
        raise RuntimeError("Linux load probes require Linux")
    cpu_rule = options.get("cpu_rule", Rule())
    return LoadGuard(
        LinuxCpu(proc_stat, max_window=cpu_rule.stale_after,
                 clock=options.get("monotonic", time.monotonic)),
        LinuxAmdGpu(pci_root) if gpu_probe is None else gpu_probe,
        **options,
    )
