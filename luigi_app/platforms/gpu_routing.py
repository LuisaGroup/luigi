"""Select a verified backend for a named UUID without using GPU ordinals as identities."""
import threading
import time

from luigi_app.platforms.hip_policy import DeferredLaunch, _metric
from luigi_app.platforms.loadguard import LoadGuard, Reading


class RoutedPolicy:
    def __init__(self, base, nvidia=None, *, monotonic=time.monotonic):
        self.base = base
        self.nvidia = nvidia
        self._now = monotonic
        self._lock = threading.Lock()
        self._guards = {}
        self._inventory = {}
        self._stamp = -float("inf")

    def _bound_nvidia(self, q):
        return q["gpu_uuid"] and q.get("gpu_provider") == "nvidia"

    @staticmethod
    def _uuid(q):
        return "GPU-" + q["gpu_uuid"]

    def _reading(self, uuid):
        with self._lock:
            row = self._inventory.get(uuid)
            if row is None or self._now() - self._stamp > 13:
                return Reading(None, "gpu_uuid_sample_unavailable")
            return Reading(row.percent, "nvidia_uuid_utilization")

    def _poll(self, broker):
        configs = [q for q in broker.queue_configs() if self._bound_nvidia(q)]
        if not configs:
            return
        attempt = self._now()
        try:
            rows = self.nvidia.inventory_load()
            inventory = {row.uuid:row for row in rows}
        except Exception:
            inventory = {}
        with self._lock:
            self._inventory, self._stamp = inventory, attempt
            for q in configs:
                name, uuid = q["name"], self._uuid(q)
                if name not in self._guards:
                    self._guards[name] = LoadGuard(lambda: None, lambda u=uuid: self._reading(u),
                                                   monotonic=self._now)
            guards = list(self._guards.values())
        for guard in guards:
            guard.refresh("gpu")

    def observe(self, broker, stop):
        class WithoutNvidia:
            def queue_configs(inner):
                return [q for q in broker.queue_configs() if not self._bound_nvidia(q)]
        if not self.nvidia:
            return self.base.observe(WithoutNvidia(), stop)
        worker = threading.Thread(target=self.base.observe, args=(WithoutNvidia(), stop), daemon=True)
        worker.start()
        while not stop.is_set():
            self._poll(broker)
            stop.wait(5)

    def _native_metric(self, q):
        with self._lock:
            guard = self._guards.get(q["name"])
            row = self._inventory.get(self._uuid(q))
            timed_out = self._now() - self._stamp > 13
        if guard is None or row is None or timed_out:
            return None
        return guard.decision("gpu").gpu

    def decision(self, q):
        if not self._bound_nvidia(q):
            return self.base.decision(q)
        if not self.nvidia or not getattr(self.nvidia, "can_launch", False):
            return False, "gpu_provider_not_configured"
        cpu_allowed, cpu_reason = self.base.decision({"kind":"cpu", "gpu_uuid":None})
        if not cpu_allowed:
            return False, cpu_reason
        sample = self._native_metric(q)
        return ((sample.allowed, "ready" if sample.allowed else "gpu_" + sample.reason) if sample else
                (False, "gpu_identity_unverified"))

    def validate_submission(self, q, argv):
        if not self._bound_nvidia(q):
            return self.base.validate_submission(q, argv)
        if self.nvidia and getattr(self.nvidia, "can_launch", False):
            return self.nvidia.validate_submission(argv)

    def launch_environment(self, q, argv):
        if not self._bound_nvidia(q):
            return self.base.launch_environment(q, argv)
        from luigi_app.platforms.nvidia_probe import NvidiaProbeError
        from luigi_app.platforms.hip_policy import workload_environment
        if not self.nvidia or not self.nvidia.can_launch:
            raise DeferredLaunch("gpu_provider_not_configured")
        # A known non-approved executable is permanent, not a telemetry timeout.
        try:
            self.nvidia.validate_submission(argv)
        except (NvidiaProbeError, OSError) as exc:
            raise ValueError("operator-approved CUDA workload could not be verified") from exc
        try:
            env = self.nvidia.launch_environment(self._uuid(q), argv, workload_environment())
        except NvidiaProbeError as exc:
            with self._lock:
                self._inventory.pop(self._uuid(q), None)
            raise DeferredLaunch("gpu_identity_unverified") from exc
        allowed, reason = self.decision(q)
        if not allowed:
            raise DeferredLaunch(reason)
        return env

    def telemetry(self, configs):
        result = self.base.telemetry([q for q in configs if not self._bound_nvidia(q)])
        for q in configs:
            if self._bound_nvidia(q):
                sample = self._native_metric(q)
                result["gpu"].append({"queue":q["name"]} | (_metric(sample) if sample else
                    {"percent":None,"observed_at":None,"reason":"gpu_identity_unverified"}))
        return result
