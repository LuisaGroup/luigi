import unittest
from unittest.mock import patch

from luigi_app.platforms.linux_policy import LinuxPolicy, NoGpuMonitorPolicy
from luigi_app.platforms.loadguard import LoadGuard, Reading, Rule


class LinuxPolicyTests(unittest.TestCase):
    def test_gpu_default_holds_when_missing_and_explicit_bypass_keeps_cpu_gate(self):
        cpu = [0]
        stamp = [0.]
        rule = Rule(recover_samples=1)
        guard = LoadGuard(lambda: Reading(cpu[0], "test"), lambda: Reading(None, "test"),
                          cpu_rule=rule, gpu_rule=rule, monotonic=lambda: stamp[0])
        guard.refresh("cpu")
        guard.refresh("gpu")
        q = {"name":"gpu", "kind":"gpu", "gpu_uuid":None}
        bound = q | {"gpu_uuid":"aa000000-0000-0000-0000-000000000000"}
        normal = LinuxPolicy(guard)
        self.assertEqual(normal.decision(q), (False, "gpu_unavailable"))
        self.assertEqual(normal.decision(bound), (False, "gpu_provider_not_configured"))
        bypass = NoGpuMonitorPolicy(guard)
        self.assertEqual(bypass.decision(q), (True, "gpu_monitor_disabled"))
        self.assertEqual(bypass.decision(bound), (False, "gpu_monitor_disabled"))
        self.assertEqual(bypass.telemetry([q])["gpu"][0]["reason"], "gpu_monitor_disabled")
        with self.assertRaises(ValueError):
            bypass.validate_submission(bound, [])
        stamp[0] = 1
        cpu[0] = 90
        guard.refresh("cpu")
        self.assertEqual(bypass.decision(q), (False, "cpu_busy"))

    def test_no_monitor_cli_refuses_binding_options(self):
        import os
        from pathlib import Path
        import subprocess
        import sys
        import tempfile
        from luigi_app.core.broker import Broker
        from luigi_app.platforms import transport
        if sys.platform != "linux" or os.geteuid() == 0:
            self.skipTest("Linux non-root local runner")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "s"
            transport.private_root(root, initialize=True)
            Broker(root).auth.bootstrap_admin("test synthetic lengthy admin password")
            transport.identity(root, initialize=True)
            shim = Path(__file__).resolve().parents[1] / "luigi.py"
            result = subprocess.run([sys.executable, str(shim), "--root", str(root), "serve",
                                     "--no-gpu-monitor", "--nvidia-smi", "/ignored/nonexistent"],
                                    cwd=tmp, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            self.assertIn("cannot be combined with device binding options", result.stderr)
