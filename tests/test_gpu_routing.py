import unittest
from types import SimpleNamespace

from luigi_app.platforms.gpu_routing import RoutedPolicy

TARGET = "a0000000-0000-0000-0000-000000000001"
NVIDIA = {"name":"render", "kind":"gpu", "gpu_uuid":TARGET, "gpu_provider":"nvidia"}


class RoutingTests(unittest.TestCase):
    def test_named_nvidia_requires_fresh_samples_and_cpu(self):
        class Base:
            def decision(self, q):
                return True, "ready"
        class Provider:
            can_launch = True
            available = True
            def inventory_load(self):
                if not self.available:
                    raise RuntimeError("unsupported telemetry")
                return [SimpleNamespace(uuid="GPU-"+TARGET, percent=0)]
        provider = Provider()
        elapsed = [0.]
        policy = RoutedPolicy(Base(), provider, monotonic=lambda:elapsed[0])
        broker = SimpleNamespace(queue_configs=lambda:[NVIDIA])
        self.assertEqual(policy.decision(NVIDIA), (False, "gpu_identity_unverified"))
        policy._poll(broker)
        self.assertFalse(policy.decision(NVIDIA)[0]) # Hysteresis requires two healthy samples.
        elapsed[0] = 5
        policy._poll(broker)
        self.assertEqual(policy.decision(NVIDIA), (True, "ready"))
        elapsed[0] = 19
        self.assertFalse(policy.decision(NVIDIA)[0])
        provider.available = False
        policy._poll(broker)
        self.assertFalse(policy.decision(NVIDIA)[0])
