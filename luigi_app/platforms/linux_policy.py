"""Cooperative Linux launch gating; never infer a stable GPU UUID from an index."""
import threading

from luigi_app.platforms.hip_policy import DeferredLaunch, _metric


class LinuxPolicy:
    def __init__(self, guard=None):
        if guard is None:
            from luigi_app.platforms.linux_load import linux_guard
            guard = linux_guard()
        self._guard = guard

    def decision(self, q):
        if q["gpu_uuid"]:
            return False, "gpu_provider_not_configured"
        decision = self._guard.decision(q["kind"])
        return decision.allowed, decision.reason

    def validate_submission(self, q, argv):
        # An operator can explicitly configure a named UUID even before a
        # verified Linux binding provider exists; those jobs remain held.
        return None

    def launch_environment(self, q, argv):
        raise DeferredLaunch("gpu_provider_not_configured")

    def observe(self, broker, stop):
        def poll(name):
            while not stop.is_set():
                self._guard.refresh(name)
                stop.wait(.25)
        gpu = threading.Thread(target=poll, args=("gpu",), daemon=True)
        gpu.start()
        poll("cpu")

    def telemetry(self, configs):
        value = self._guard.decision("gpu")
        return {"cpu": _metric(value.cpu), "gpu": [
            {"queue": q["name"]} | (_metric(value.gpu) if not q["gpu_uuid"] else
            {"percent": None, "observed_at": None, "reason": "gpu_provider_not_configured"})
            for q in configs if q["kind"] == "gpu"
        ]}


class NoGpuMonitorPolicy:
    """Explicit operator bypass of GPU sensor only; regular CPU gate remains."""
    gpu_monitor_disabled = True

    def __init__(self, guard):
        self._guard = guard

    def decision(self, q):
        if q["gpu_uuid"]:
            return False, "gpu_monitor_disabled"
        cpu = self._guard.decision("cpu").cpu
        if not cpu.allowed:
            return False, "cpu_" + cpu.reason
        return True, "gpu_monitor_disabled" if q["kind"] == "gpu" else "ready"

    def validate_submission(self, q, argv):
        if q["gpu_uuid"]:
            raise ValueError("GPU UUID bindings require enabled monitoring")

    def observe(self, broker, stop):
        while not stop.is_set():
            self._guard.refresh("cpu")
            stop.wait(.25)

    def telemetry(self, configs):
        cpu = self._guard.decision("cpu").cpu
        return {"cpu": _metric(cpu), "gpu": [
            {"queue": q["name"], "percent": None, "observed_at": None,
             "reason": "gpu_monitor_disabled"} for q in configs if q["kind"] == "gpu"]}
