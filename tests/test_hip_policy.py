from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from luigi_app.platforms.hip_probe import Device, HipProbeError, VerifiedDevice
from luigi_app.platforms.hip_policy import DeferredLaunch, HipPolicy
from luigi_app.platforms.loadguard import LoadGuard, Reading, Rule

UUID = "ab000000-0000-0000-0000-000000000001"
LUID = "0x00000000_0x00000001"
GPU = {"name": "render", "kind": "gpu", "gpu_uuid": UUID}


class HipPolicyTests(unittest.TestCase):
    def test_runtime_path_mask_uuid_route_and_stale_gate(self):
        clock = [0.]
        observed = []
        reading = [0.]
        route = [Device(1, UUID, LUID, 1)]
        def verifier(path, uuid, *, base_env, timeout):
            self.assertEqual(uuid, UUID)
            self.assertNotIn("HIP_VISIBLE_DEVICES", base_env)
            return VerifiedDevice(route[0], 0, path)
        green = Rule(recover_samples=1)
        def guard(luid):
            observed.append(luid)
            return LoadGuard(lambda: Reading(0, "synthetic"),
                             lambda: Reading(reading[0], "synthetic"),
                             monotonic=lambda:clock[0], cpu_rule=green, gpu_rule=green)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            dll = root / "amdhip64_7.dll"
            dll.write_bytes(b"synthetic runtime")
            app = root / "app"
            app.mkdir()
            exe = app / "work.exe"
            exe.touch()
            shutil.copy(dll, app)
            with patch.dict("os.environ", {"HIP_VISIBLE_DEVICES":"7", "LUIGI_SECRET":"SECRET"}):
                policy = HipPolicy(None, dll, [exe], verify=verifier, guard_factory=guard,
                                   monotonic=lambda:clock[0], trust=lambda p: str(p))
            policy._raw.refresh("cpu")
            policy.refresh(GPU)
            self.assertTrue(policy.decision(GPU)[0])
            self.assertEqual(observed, [None, LUID])
            env = policy.launch_environment(GPU, [str(exe)])
            self.assertEqual(env["HIP_VISIBLE_DEVICES"], "1")
            self.assertNotIn("LUIGI_SECRET", env)
            self.assertEqual(policy.launch_environment(GPU, [str(exe)])["HIP_VISIBLE_DEVICES"], "1")
            self.assertEqual(policy.telemetry([GPU])["gpu"][0]["percent"], 0)
            with self.assertRaises(ValueError):
                policy.validate_submission(GPU, [str(dll)])
            (app / dll.name).write_bytes(b"different")
            with self.assertRaisesRegex(ValueError, "does not match"):
                policy.launch_environment(GPU, [str(exe)])
            shutil.copy(dll, app)
            clock[0] = 5
            policy._raw.refresh("cpu")
            self.assertEqual(policy.decision(GPU), (False, "gpu_stale"))
            with self.assertRaisesRegex(DeferredLaunch, "gpu_stale"):
                policy.launch_environment(GPU, [str(exe)])
            policy.refresh(GPU)
            route[0] = Device(0, UUID, LUID, 1)
            with self.assertRaisesRegex(DeferredLaunch, "gpu_identity_unverified"):
                policy.launch_environment(GPU, [str(exe)])
