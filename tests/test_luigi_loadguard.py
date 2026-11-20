import threading
import unittest

from luigi_app.platforms.loadguard import LoadGuard, Reading, Rule, cpu_busy, gpu_busy, parse_luid


A = "0x00000000_0x0000aaaa"
B = "0x00000000_0x0000bbbb"


def row(pid, adapter, phys, eng, typ, percent):
    return {"Name": f"pid_{pid}_luid_{adapter}_phys_{phys}_eng_{eng}_engtype_{typ}",
            "UtilizationPercentage": percent}


class TelemetryFixtures(unittest.TestCase):
    def test_cpu_kernel_includes_idle(self):
        self.assertEqual(cpu_busy((10, 30, 10), (60, 100, 40)), 50)
        self.assertIsNone(cpu_busy((10, 30, 10), (10, 30, 10)))
        self.assertIsNone(cpu_busy((10, 30, 10), (20, 35, 40)))

    def test_adapters_engines_not_summed(self):
        rows = [row(1, A, 0, 0, "3D", 41), row(2, A, 0, 0, "3D", 38),
                row(2, A, 0, 1, "Video Decode", 77), row(2, A, 1, 0, "3D", 70),
                row(2, B, 0, 0, "Compute 0", 90)]
        self.assertEqual(gpu_busy(rows, parse_luid(A)).percent, 79)
        self.assertEqual(gpu_busy(rows, None).percent, 90)
        self.assertEqual(gpu_busy(rows, parse_luid(A), {"video decode"}).percent, 77)
        self.assertIn("aaaa_phys_0_engine_0", gpu_busy(rows, parse_luid(A)).detail)

    def test_missing_invalid_duplicate_and_actual_zero(self):
        item = row(1, A, 0, 0, "3D", 0)
        self.assertEqual(gpu_busy([item], None).percent, 0)
        self.assertIsNone(gpu_busy([], None).percent)
        self.assertIsNone(gpu_busy([item], parse_luid(B)).percent)
        self.assertIsNone(gpu_busy([item, item], None).percent)
        self.assertIsNone(gpu_busy([row(1, A, 0, 0, "3D", 101)], None).percent)
        self.assertIsNone(gpu_busy([{"Name": A + "_changed", "UtilizationPercentage": 0}], None).percent)
        self.assertEqual(gpu_busy([row(1, A, 0, 0, "3D", 80), row(2, A, 0, 0, "3D", 60)], None).percent, 100)


class PolicyFixtures(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.state = {"cpu": 20, "gpu": 20}
        self.calls = {"cpu": 0, "gpu": 0}
        def read(key):
            self.calls[key] += 1
            return Reading(self.state[key], key)
        self.probes = (lambda: read("cpu"), lambda: read("gpu"))
        rule = Rule(interval=1, stale_after=4)
        self.options = dict(cpu_rule=rule, gpu_rule=rule, monotonic=lambda: self.now,
                            wall=lambda: 1000 + self.now)
        self.guard = LoadGuard(*self.probes, **self.options)

    def poll(self, *keys):
        self.now += 1
        for key in keys:
            self.guard.refresh(key)

    def test_lanes_recovery_and_hysteresis(self):
        self.assertEqual(self.guard.decision("gpu").reason, "cpu_no_sample,gpu_no_sample")
        self.poll("cpu", "gpu")
        self.assertFalse(self.guard.decision("cpu").allowed)
        self.poll("cpu", "gpu")
        self.assertTrue(self.guard.decision("gpu").allowed)
        self.state["gpu"] = 80
        self.poll("cpu", "gpu")
        self.assertTrue(self.guard.decision("cpu").allowed)
        self.assertEqual(self.guard.decision("gpu").reason, "gpu_busy")
        self.state["gpu"] = 60
        self.poll("gpu")
        self.state["gpu"] = 70
        self.poll("gpu")
        self.assertEqual(self.guard.decision("gpu").reason, "gpu_recovering_0_of_2")
        self.state["gpu"] = 60
        self.poll("gpu", "cpu")
        self.poll("gpu", "cpu")
        self.assertTrue(self.guard.decision("gpu").allowed)
        self.state["gpu"] = 79
        self.poll("gpu", "cpu")
        self.assertTrue(self.guard.decision("gpu").allowed)

    def test_stale_unknown_and_non_polling_status(self):
        self.poll("cpu")
        for _ in range(5):
            self.assertFalse(self.guard.decision("cpu").allowed)
            self.assertFalse(self.guard.refresh("cpu"))
        self.assertEqual(self.calls["cpu"], 1)
        self.poll("cpu")
        self.assertEqual(self.guard.decision("cpu").cpu.sample.observed_unix, 1002)
        self.now += 5
        self.assertEqual(self.guard.decision("cpu").reason, "cpu_stale")
        self.poll("cpu")
        self.assertFalse(self.guard.decision("cpu").allowed)
        self.state["cpu"] = None
        self.poll("cpu")
        self.assertEqual(self.guard.decision("cpu").reason, "cpu_unavailable")

    def test_opt_out_cpu_only_for_gpu_lane(self):
        guard = LoadGuard(*self.probes, gpu_requires_cpu=False, **self.options)
        guard.refresh("gpu")
        self.now += 1
        guard.refresh("gpu")
        self.assertTrue(guard.decision("gpu").allowed)
        self.assertFalse(guard.decision("cpu").allowed)

    def test_blocked_probe_does_not_block_cached_status_or_cpu_probe(self):
        waiting, resume = threading.Event(), threading.Event()
        def slow():
            waiting.set()
            if not resume.wait(timeout=2):
                raise RuntimeError("fixture timed out")
            return Reading(0, "fixture")
        guard = LoadGuard(self.probes[0], slow, **self.options)
        worker = threading.Thread(target=guard.refresh, args=("gpu",))
        worker.start()
        try:
            self.assertTrue(waiting.wait(1))
            self.assertTrue(guard.refresh("cpu"))
            self.assertFalse(guard.refresh("gpu"))
            self.assertEqual(guard.decision("gpu").gpu.reason, "no_sample")
        finally:
            resume.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())


if __name__ == "__main__":
    unittest.main()
