"""Local deterministic fixtures; no GPU driver's state is touched."""
from pathlib import Path
import sys
import tempfile
import unittest

from luigi_app.platforms.linux_load import (LinuxAmdGpu, LinuxCpu, LinuxPciAggregate, canonical_bdf,
                        linux_guard, linux_pci_inventory, parse_proc_cpu, proc_cpu_percent)


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux procfs and sysfs fixtures")
class ScratchFixtures(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.pci = self.root / "pci"
        self.pci.mkdir()

    def adapter(self, slot="00", *, vendor="0x1002", driver="amdgpu", percent="0", device_class="0x030000"):
        item = self.pci / f"0000:01:{slot}.0"
        item.mkdir()
        (item / "class").write_text(device_class + "\n")
        (item / "vendor").write_text(vendor + "\n")
        if driver is not None:
            (item / "driver").symlink_to(self.root / "drivers" / driver)
        if percent is not None:
            (item / "gpu_busy_percent").write_text(percent + "\n")
        return item

    def test_cpu_guest_counted_once_and_io_wait_not_execution(self):
        one = parse_proc_cpu("cpu 100 0 100 100 0 0 0 0 0 0")
        two = parse_proc_cpu("cpu 120 3 105 130 12 1 0 4 17 3")
        # 20+3+5+30+12+1+0+4 total, 42 idle+iowait.
        self.assertAlmostEqual(proc_cpu_percent(one,two), 100 * 33 / 75)
        self.assertIsNone(proc_cpu_percent(two, one))
        self.assertIsNone(proc_cpu_percent(two, two))
        for malformed in ("cpu1 0 0 0 0 0 0 0 0", "cpu 0 0", "cpu 0 0 0 0 X 0 0 0"):
            with self.assertRaises(ValueError):
                parse_proc_cpu(malformed)

    def test_cpu_missing_or_long_gap_requires_baseline(self):
        stat = self.root / "stat"
        clock = 1.0
        cpu = LinuxCpu(stat, clock=lambda: clock)
        self.assertIsNone(cpu().percent)
        stat.write_text("cpu 100 0 100 100 0 0 0 0 0 0\ncpu0 1 2\n")
        self.assertEqual(cpu().detail, "linux_cpu_warmup")
        clock += 1
        stat.write_text("cpu 115 0 105 110 0 0 0 0 0 0\n")
        self.assertAlmostEqual(cpu().percent, 100 * 20 / 30)
        clock += 5
        stat.write_text("cpu 135 0 105 120 0 0 0 0 0 0\n")
        self.assertEqual(cpu().detail, "linux_cpu_warmup")
        clock += 1
        stat.write_text("cpu 130 0 105 120 0 0 0 0 0 0\n")
        self.assertEqual(cpu().detail, "linux_cpu_invalid_delta")

    def test_amd_complete_reports_max_and_real_zero(self):
        one = self.adapter(percent="35")
        two = self.adapter("01", percent="55", device_class="0x120000")
        self.adapter("02", vendor="0x8086", driver=None, percent=None, device_class="0x020000")
        gpu = LinuxAmdGpu(self.pci)
        self.assertEqual(gpu().percent, 55)
        (one / "gpu_busy_percent").write_text("0\n")
        (two / "gpu_busy_percent").write_text("0\n")
        self.assertEqual(gpu().percent, 0)

    def test_missing_invalid_and_mixed_inventory_hold(self):
        gpu = LinuxAmdGpu(self.pci)
        self.assertEqual(gpu().detail, "linux_no_pci_gpu")
        first = self.adapter("01", percent=None)
        self.adapter("02", percent="0")
        self.assertIsNone(gpu().percent)
        self.assertEqual(gpu().detail, "linux_gpu_busy_percent_unavailable")
        (first / "gpu_busy_percent").write_text("101\n")
        self.assertEqual(gpu().detail, "linux_gpu_busy_percent_invalid")
        (first / "gpu_busy_percent").write_text("0\n")
        self.adapter("03", vendor="0x10de", driver="nvidia", percent=None)
        self.assertEqual(gpu().detail, "linux_gpu_provider_unsupported")
        self.assertIsNone(gpu().percent)

    def test_unbound_and_different_driver_not_assumed_idle(self):
        item = self.adapter(driver=None)
        gpu = LinuxAmdGpu(self.pci)
        self.assertIsNone(gpu().percent)
        (item / "driver").symlink_to(self.root / "vfio-pci")
        self.assertEqual(gpu().detail, "linux_gpu_driver_unsupported")

    def test_mixed_provider_exact_current_bdf_join(self):
        self.adapter("00", percent="31")
        self.adapter("01", vendor="0x10de", driver="nvidia", percent=None)
        create = lambda rows: LinuxPciAggregate(self.pci, nvidia_readings=lambda: rows)
        self.assertEqual(canonical_bdf("00000000:01:01.0"), "0000:01:01.0")
        self.assertEqual([card.bdf for card in linux_pci_inventory(self.pci)],
                         ["0000:01:00.0", "0000:01:01.0"])
        self.assertEqual(create({"00000000:01:01.0": 79})().percent, 79)
        self.assertEqual(create({"00000000:01:01.0": 0})().percent, 31)
        for rejected in ({}, {"0000:01:00.0": 0}, {"0000:01:01.0": None},
                         {"0000:01:01.0": 0, "00000000:01:01.0": 0}):
            self.assertIsNone(create(rejected)().percent)

    def test_inventory_change_during_provider_read_holds(self):
        self.adapter("01", vendor="0x10de", driver="nvidia", percent=None)
        def sample():
            self.adapter("02", percent="0")
            return {"0000:01:01.0": 0}
        aggregate = LinuxPciAggregate(self.pci, nvidia_readings=sample)
        self.assertEqual(aggregate().detail, "linux_gpu_inventory_changed")

    def test_guard_holds_gpu_but_allows_measured_idle_cpu(self):
        stat = self.root / "stat"
        clock = 0
        guard = linux_guard(proc_stat=stat, pci_root=self.pci, monotonic=lambda: clock)
        for i in range(3):
            clock += 1
            stat.write_text(f"cpu 10 0 10 {10+i} 0 0 0 0 0 0\n")
            guard.refresh("cpu")
        guard.refresh("gpu")
        self.assertTrue(guard.decision("cpu").allowed)
        self.assertFalse(guard.decision("gpu").allowed)
        self.assertEqual(guard.decision("gpu").gpu.sample.detail, "linux_no_pci_gpu")


if __name__ == "__main__":
    unittest.main()
