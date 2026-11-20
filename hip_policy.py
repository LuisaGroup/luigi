"""Trusted operator policy for cooperative AMD HIP UUID queues."""
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import threading
import time

import gpufifo as fifo
from hip_gpu_probe import HipProbeError, trusted_local_dll, verify_device
from luigi_loadguard import LoadGuard, WindowsGpu


def workload_environment():
    allow = {
        "SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "COMSPEC", "SYSTEMDRIVE",
        "USERPROFILE", "LOCALAPPDATA", "APPDATA", "PROGRAMDATA",
        "PROGRAMFILES", "PROGRAMFILES(X86)", "COMMONPROGRAMFILES",
        "USERNAME", "USERDOMAIN", "HOME", "HIP_PATH", "ROCM_PATH",
        "CUDA_PATH", "HOMEDRIVE", "HOMEPATH",
    }
    return {k: v for k, v in os.environ.items() if k.upper() in allow}


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for part in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(part)
    return digest.digest()


class DeferredLaunch(RuntimeError):
    """A fresh launch was not safe; no child has been created."""


@dataclass
class Route:
    proof: object
    stamp: float
    guard: LoadGuard


def _metric(sample):
    if sample.sample is None:
        return {"percent": None, "observed_at": None, "reason": sample.reason}
    return {"percent": sample.sample.percent,
            "observed_at": datetime.fromtimestamp(sample.sample.observed_unix, timezone.utc).isoformat(),
            "reason": sample.reason}


class HipPolicy:
    # All discovery and WDDM work runs in the sampler, not claim transactions.
    def __init__(self, adapter, dll=None, workloads=(), *, monotonic=time.monotonic,
                 verify=verify_device, guard_factory=None, trust=trusted_local_dll):
        self._lock = threading.Lock()
        self._now = monotonic
        self._verify = verify
        self._trust = trust
        self._base = workload_environment()
        self._dll = self._trust(dll) if dll else None
        self._workloads = {os.path.normcase(fifo.validate_local_path(str(app))) for app in workloads}
        if self._workloads and not self._dll:
            raise ValueError("approved HIP workloads require a pinned HIP runtime")
        self._digest = _sha256(self._dll) if self._dll else None
        self._routes = {}
        self._errors = {}
        self._ambiguous = set()
        self._threads = {}
        self._raw = (guard_factory(adapter) if guard_factory else
                     LoadGuard.windows(adapter_luid=adapter))
        self._guard = guard_factory or (
            lambda luid: LoadGuard(lambda: None, WindowsGpu(luid)))
        self._identity_ttl = 20.

    def _bindings(self, proof, device):
        if proof.device.uuid != device or proof.visible_ordinal != 0:
            raise HipProbeError("masked probe returned the wrong device")
        return proof.device

    def refresh(self, q):
        """Probe off lock; publish only a complete identity and telemetry pair."""
        name, device = q["name"], q["gpu_uuid"]
        if not self._dll:
            return
        with self._lock:
            if name in self._ambiguous:
                return
            previous = self._routes.get(name)
        attempt = self._now()
        try:
            if previous is None or attempt - previous.stamp >= 8:
                current = self._verify(self._dll, device, base_env=self._base, timeout=5)
                entry = self._bindings(current, device)
                if (previous and previous.proof.device == entry
                        and attempt - previous.stamp <= self._identity_ttl):
                    route = Route(current, attempt, previous.guard)
                else:
                    route = Route(current, attempt, self._guard(entry.luid))
            else:
                route = previous
            route.guard.refresh("gpu")
            with self._lock:
                if name in self._ambiguous:
                    return
                conflicts = [other for other, value in self._routes.items()
                             if other != name and value.proof.device.uuid != device
                             and value.proof.device.luid == route.proof.device.luid
                             and self._now() - value.stamp <= self._identity_ttl]
                if conflicts:
                    self._ambiguous.update([name, *conflicts])
                    for other in (name, *conflicts):
                        self._routes.pop(other, None)
                        self._errors[other] = "gpu_identity_ambiguous"
                    return
                self._routes[name] = route
                self._errors.pop(name, None)
        except (OSError, ValueError, HipProbeError):
            with self._lock:
                self._routes.pop(name, None)
                self._errors[name] = "gpu_identity_unverified"

    def _route(self, name):
        with self._lock:
            route = self._routes.get(name)
            if route and self._now() - route.stamp <= self._identity_ttl:
                return route, "ready"
            return None, ("gpu_identity_stale" if route else self._errors.get(name, "gpu_identity_unverified"))

    def decision(self, q):
        if not q["gpu_uuid"]:
            value = self._raw.decision(q["kind"])
            return value.allowed, value.reason
        if not self._dll or not self._workloads:
            return False, "gpu_provider_not_configured"
        cpu = self._raw.decision("cpu").cpu
        if not cpu.allowed:
            return False, "cpu_" + cpu.reason
        route, reason = self._route(q["name"])
        if route is None:
            return False, reason
        # Select only this UUID's matched WDDM LUID; never fall back globally.
        gpu = route.guard.decision("gpu").gpu
        return gpu.allowed, "ready" if gpu.allowed else "gpu_" + gpu.reason

    def observe(self, broker, stop):
        def poll_raw(metric):
            while not stop.is_set():
                self._raw.refresh(metric)
                stop.wait(.25)

        def poll_adapter(q):
            while not stop.is_set():
                self.refresh(q)
                stop.wait(.5)

        for metric in ("cpu", "gpu"):
            threading.Thread(target=poll_raw, args=(metric,), daemon=True).start()
        while not stop.is_set():
            for q in broker.queue_configs():
                name = q["name"]
                if self._dll and q["gpu_uuid"] and name not in self._threads:
                    t = threading.Thread(target=poll_adapter, args=(q,), daemon=True)
                    self._threads[name] = t
                    t.start()
            stop.wait(.5)

    def validate_submission(self, q, argv):
        if not self._dll or not self._workloads:
            return  # Accept while provider is not configured; show a hold.
        executable = fifo.validate_local_path(argv[0])
        if os.path.normcase(executable) not in self._workloads:
            raise ValueError("executable is not an operator-approved pinned HIP workload")
        pinned = self._trust(self._dll)
        app_runtime = self._trust(Path(executable).parent / Path(pinned).name)
        siblings = tuple(Path(executable).parent.glob("amdhip64*.dll"))
        if len(siblings) != 1 or os.path.normcase(str(siblings[0])) != os.path.normcase(app_runtime):
            raise ValueError("app-local HIP runtime is ambiguous")
        return pinned, app_runtime

    def launch_environment(self, q, argv):
        result = self.validate_submission(q, argv)
        if not result:
            raise DeferredLaunch("gpu_provider_not_configured")
        pinned, app_runtime = result
        if _sha256(pinned) != self._digest or _sha256(app_runtime) != self._digest:
            raise ValueError("app-local HIP runtime does not match the pinned runtime")
        try:
            return self._verified_environment(q, app_runtime)
        except HipProbeError as exc:
            with self._lock:
                self._routes.pop(q["name"], None)
                self._errors[q["name"]] = "gpu_identity_unverified"
            raise DeferredLaunch("gpu_identity_unverified") from exc

    def _verified_environment(self, q, runtime):
        old_route, reason = self._route(q["name"])
        if not old_route:
            raise DeferredLaunch(reason)
        current = self._verify(runtime, q["gpu_uuid"], base_env=self._base, timeout=5)
        if self._bindings(current, q["gpu_uuid"]) != old_route.proof.device:
            raise HipProbeError("live GPU identity or device mapping changed before launch")
        route, _ = self._route(q["name"])
        allowed, reason = self.decision(q)
        if route is None or route.proof.device != current.device:
            raise HipProbeError("GPU identity changed during launch proof")
        if not allowed:
            raise DeferredLaunch(reason)
        return current.launch_environment(self._base)

    def telemetry(self, configs):
        raw = self._raw.decision("gpu")
        entries = []
        for q in configs:
            if q["kind"] != "gpu":
                continue
            route, reason = self._route(q["name"]) if q["gpu_uuid"] else (None, None)
            value = (_metric(route.guard.decision("gpu").gpu) if route else
                     ({"percent": None, "observed_at": None, "reason": reason}
                      if reason else _metric(raw.gpu)))
            entries.append({"queue": q["name"]} | value)
        return {"cpu": _metric(raw.cpu), "gpu": entries}
