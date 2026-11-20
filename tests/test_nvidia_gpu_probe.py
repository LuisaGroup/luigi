"""All vendor command responses are fixtures. Never runs NVIDIA tools/driver."""
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from luigi_app.platforms.nvidia_probe import (NvidiaProvider, NvidiaProbeError, clean_environment,
                              normalize_uuid, normalize_pci, parse_inventory)

UUID = 'GPU-8932f937-d72c-4106-c12f-20bd9faed9f6'
OTHER = 'GPU-8932f937-d72c-4106-c12f-20bd9faed9f7'
NOW = 1800000000.
STAMP = datetime.fromtimestamp(NOW).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]


def row(uuid=UUID, pci='00000000:01:00.0', load='3', current='Disabled', pending='Disabled', stamp=STAMP):
    return f'{stamp}, {uuid}, {pci}, {load}, {current}, {pending}\n'


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.smi = self.root / 'nvidia-smi'
        self.driver = self.root / 'driver' / 'libcuda.so.1'
        self.app = self.root / 'workloads' / 'approved'
        for path in (self.smi, self.driver, self.app):
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b'fixture only - never executed')
        self.calls = []
        self.inventory = row()
        self.proof = {'uuid': UUID, 'pci_bus_id': '0000:01:00.0'}

    def runner(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        output = self.inventory if argv[0] == str(self.smi) else json.dumps(self.proof)
        return subprocess.CompletedProcess(argv, 0, output, '')

    def provider(self, **kwargs):
        return NvidiaProvider(str(self.smi), str(self.driver), [str(self.app)],
                              runner=kwargs.pop('runner', self.runner),
                              monotonic=lambda: 1., wall_time=lambda: NOW, **kwargs)

    def test_identity_normalization_requires_full_nvidia_uuid(self):
        self.assertEqual(normalize_uuid(UUID.upper().replace('GPU-', 'GPU-')), UUID)
        for invalid in ('0', UUID[4:], UUID[:12], 'MIG-' + UUID, UUID + ',0', ' ' + UUID, 1):
            with self.subTest(invalid=invalid), self.assertRaises(NvidiaProbeError):
                normalize_uuid(invalid)
        self.assertEqual(normalize_pci('00000000:AB:1F.7'), '0000:ab:1f.7')
        with self.assertRaises(NvidiaProbeError):
            normalize_pci('0000:01:20.0')

    def test_inventory_fixed_argv_and_clean_environment(self):
        provider = self.provider()
        with patch.dict('os.environ', {'PATH': '/evil', 'LD_PRELOAD': '/evil.so',
                                      'NVML_MPS_DEVICE_ENUMERATION': '1', 'CUDA_VISIBLE_DEVICES': '0'}):
            devices = provider.inventory_load()
        self.assertEqual(devices[0].uuid, UUID)
        self.assertEqual(devices[0].pci_bus_id, '0000:01:00.0')
        self.assertEqual(devices[0].percent, 3.)
        argv, kwargs = self.calls[0]
        self.assertEqual(argv, [str(self.smi), '--query-gpu=timestamp,uuid,pci.bus_id,utilization.gpu,mig.mode.current,mig.mode.pending', '--format=csv,noheader,nounits'])
        self.assertFalse(kwargs['shell'])
        self.assertEqual(kwargs['timeout'], 4)
        for name in ('PATH', 'LD_PRELOAD', 'CUDA_VISIBLE_DEVICES', 'NVML_MPS_DEVICE_ENUMERATION'):
            self.assertNotIn(name, kwargs['env'])

    def test_unknown_or_malformed_load_never_idle(self):
        for load in ('N/A', '[N/A]', 'nan', 'inf', '-1', '101', '', '3 %'):
            with self.subTest(load=load), self.assertRaises(NvidiaProbeError):
                parse_inventory(row(load=load), NOW, NOW)

    def test_mig_modes_fail_closed_even_with_physical_uuid(self):
        for current, pending in (('Enabled', 'Enabled'), ('Disabled', 'Enabled'), ('N/A', 'Disabled'), ('Unknown', 'Unknown')):
            with self.subTest(mode=(current, pending)), self.assertRaises(NvidiaProbeError):
                parse_inventory(row(current=current, pending=pending), NOW, NOW)
        for mode in ('N/A', '[N/A]'):
            self.assertEqual(parse_inventory(row(current=mode, pending=mode), NOW, NOW)[0].percent, 3.)
        # Valid physical identity does not make MIG's unsupported load usable.
        with self.assertRaises(NvidiaProbeError):
            parse_inventory(row(load='N/A', current='Enabled', pending='Enabled'), NOW, NOW)

    def test_empty_duplicates_and_malformed_rows_hold(self):
        for text in ('', '\n', 'bad,row\n', row() + row(), row() + row(uuid=OTHER),
                     row() + row(pci='0000:02:00.0'), row().replace(UUID, 'MIG-' + UUID)):
            with self.subTest(text=text), self.assertRaises(NvidiaProbeError):
                parse_inventory(text, NOW, NOW)
        inventory = parse_inventory(row() + row(uuid=OTHER, pci='0000:02:00.0', load='89'), NOW, NOW)
        self.assertEqual([x.percent for x in inventory], [3., 89.])

    def test_stale_and_clock_jump_hold(self):
        for start, finish in ((NOW + 10, NOW + 10), (NOW - 10, NOW - 10), (NOW, NOW + 6), (NOW, NOW - 1)):
            with self.subTest(times=(start, finish)), self.assertRaises(NvidiaProbeError):
                parse_inventory(row(), start, finish)

    def test_command_failure_and_timeout_hold(self):
        for failure in (subprocess.TimeoutExpired('fixture', 4), OSError('missing')):
            def runner(*args, **kwargs):
                raise failure
            with self.subTest(failure=failure), self.assertRaises(NvidiaProbeError):
                self.provider(runner=runner).inventory_load()
        for code, err in ((1, ''), (0, 'warning')):
            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(args[0], code, row(), err)
            with self.subTest(code=code, err=err), self.assertRaises(NvidiaProbeError):
                self.provider(runner=runner).inventory_load()

    def test_missing_uuid_does_not_fall_back(self):
        with self.assertRaises(NvidiaProbeError):
            self.provider().verify_device(OTHER)

    def test_launch_fresh_mask_proof_and_environment(self):
        env = self.provider().launch_environment(UUID, [str(self.app), '--work'],
                {'HOME': '/fixture', 'CUDA_VISIBLE_DEVICES': '0', 'HIP_VISIBLE_DEVICES': '1',
                 'PYTHONPATH': '/evil', 'LD_LIBRARY_PATH': '/evil', 'CUDA_MPS_PIPE_DIRECTORY': '/evil'})
        self.assertEqual(env['CUDA_VISIBLE_DEVICES'], UUID)
        self.assertEqual(env['HOME'], '/fixture')
        self.assertEqual(len(self.calls), 3)  # fresh inventory, masked driver proof, inventory recheck
        argv, kwargs = self.calls[1]
        self.assertIn('-I', argv)
        self.assertEqual(argv[-2:], ['--cuda-proof', str(self.driver)])
        self.assertEqual(kwargs['cwd'], str(self.app.parent))
        self.assertEqual(kwargs['env'], env)
        for name in ('HIP_VISIBLE_DEVICES', 'PYTHONPATH', 'LD_LIBRARY_PATH', 'CUDA_MPS_PIPE_DIRECTORY'):
            self.assertNotIn(name, env)

    def test_wrong_or_unproven_mask_holds(self):
        for proof in ({'uuid': OTHER, 'pci_bus_id': '0000:01:00.0'},
                      {'uuid': UUID, 'pci_bus_id': '0000:02:00.0'}, [], {},
                      {'uuid': UUID, 'pci_bus_id': '0000:01:00.0', 'extra': True}):
            self.proof = proof
            with self.subTest(proof=proof), self.assertRaises(NvidiaProbeError):
                self.provider().launch_environment(UUID, [str(self.app)])
        discovery = NvidiaProvider(str(self.smi), runner=self.runner, wall_time=lambda: NOW)
        self.assertEqual(discovery.inventory_load()[0].uuid, UUID)
        self.assertFalse(discovery.can_launch)
        self.assertTrue(self.provider().can_launch)
        with self.assertRaises(NvidiaProbeError):
            discovery.launch_environment(UUID, [str(self.app)])

    def test_mapping_changes_before_launch_hold(self):
        count = 0
        def runner(argv, **kwargs):
            nonlocal count
            count += 1
            if count == 3:
                self.inventory = row(pci='0000:02:00.0')
            return self.runner(argv, **kwargs)
        with self.assertRaises(NvidiaProbeError):
            self.provider(runner=runner).launch_environment(UUID, [str(self.app)])

    def test_busy_before_or_after_proof_holds(self):
        self.inventory = row(load='80')
        with self.assertRaises(NvidiaProbeError):
            self.provider().launch_environment(UUID, [str(self.app)])
        self.assertEqual(len(self.calls), 1)
        self.calls.clear()
        self.inventory = row()
        def runner(argv, **kwargs):
            if len(self.calls) == 2:
                self.inventory = row(load='80')
            return self.runner(argv, **kwargs)
        with self.assertRaises(NvidiaProbeError):
            self.provider(runner=runner).launch_environment(UUID, [str(self.app)])
        self.assertEqual(len(self.calls), 3)
        for limit in (True, 0, float('nan'), 101):
            with self.subTest(limit=limit), self.assertRaises(NvidiaProbeError):
                self.provider().launch_environment(UUID, [str(self.app)], busy_at=limit)

    def test_pins_and_workload_allowlist(self):
        provider = self.provider()
        with self.assertRaises(NvidiaProbeError):
            provider.launch_environment(UUID, [str(self.smi)])
        self.smi.write_bytes(b'changed')
        with self.assertRaises(NvidiaProbeError):
            provider.inventory_load()
        self.assertEqual(self.calls, [])

    def test_application_local_driver_rejected(self):
        provider = self.provider()
        (self.app.parent / 'libcuda.so.1').write_bytes(b'other runtime')
        with self.assertRaises(NvidiaProbeError):
            provider.launch_environment(UUID, [str(self.app)])

    def test_windows_command_is_hidden_and_shell_free(self):
        provider = self.provider()
        with patch('sys.platform', 'win32'), patch('subprocess.CREATE_NO_WINDOW', 0x08000000, create=True):
            provider._execute([str(self.smi)], {}, str(self.root))
        self.assertEqual(self.calls[0][1]['creationflags'], 0x08000000)
        self.assertFalse(self.calls[0][1]['shell'])

    def test_driver_helper_uses_only_enumeration_calls(self):
        import ctypes
        import uuid
        from luigi_app.platforms.nvidia_probe import _cuda_proof
        called = []
        class Fn:
            def __init__(self, name):
                self.name = name
            def __call__(self, *args):
                called.append(self.name)
                if self.name == 'cuDeviceGetCount':
                    args[0]._obj.value = 1
                elif self.name == 'cuDeviceGet':
                    args[0]._obj.value = 0
                elif self.name == 'cuDeviceGetUuid_v2':
                    args[0]._obj.bytes[:] = uuid.UUID(UUID[4:]).bytes
                elif self.name == 'cuDeviceGetPCIBusId':
                    args[0].value = b'0000:01:00.0'
                return 0
        class Library:
            pass
        library = Library()
        for name in ('cuInit', 'cuDeviceGetCount', 'cuDeviceGet', 'cuDeviceGetUuid_v2', 'cuDeviceGetPCIBusId'):
            setattr(library, name, Fn(name))
        loader_name = 'WinDLL' if sys.platform == 'win32' else 'CDLL'
        loader_options = {'winmode': 0x1100} if sys.platform == 'win32' else {'mode': ctypes.RTLD_LOCAL}
        actual_loader = getattr(ctypes, loader_name)
        approved = Path(self.driver).resolve()
        def select_library(name, *args, **kwargs):
            if Path(name).is_absolute() and Path(name).resolve() == approved:
                self.assertEqual(kwargs, loader_options)
                return library
            # Let Windows path validation really check the fixed drive.
            if sys.platform == 'win32' and name == 'kernel32':
                return actual_loader(name, *args, **kwargs)
            self.fail(f"unexpected native library load in fixture: {name}")
        with patch.object(ctypes, loader_name, side_effect=select_library):
            self.assertEqual(_cuda_proof(str(self.driver)), self.proof)
        self.assertEqual(called, ['cuInit', 'cuDeviceGetCount', 'cuDeviceGet', 'cuDeviceGetUuid_v2', 'cuDeviceGetPCIBusId'])
        library.cuDeviceGetCount = lambda ptr: setattr(ptr._obj, 'value', 2) or 0
        with patch.object(ctypes, loader_name, side_effect=select_library), self.assertRaises(NvidiaProbeError):
            _cuda_proof(str(self.driver))

    def test_relative_paths_and_unsupported_platform_rejected(self):
        with self.assertRaises(NvidiaProbeError):
            NvidiaProvider('nvidia-smi')
        with patch('sys.platform', 'darwin'), self.assertRaises(NvidiaProbeError):
            self.provider()


if __name__ == '__main__':
    unittest.main()
