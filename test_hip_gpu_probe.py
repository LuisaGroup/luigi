import ctypes as C
import json
import struct
import unittest
import uuid

from hip_gpu_probe import (
    Device, HipProbeError, HipUUID, _decode_child, _lexical_dll, _verify,
    enumerate_abi, format_luid, normalize_uuid, visibility_environment,
)

U0 = "01000000-4050-0000-8000-010203040506"
U1 = "00000000-0000-0000-0000-000000000001"
L0 = "0x80000001_0x1234abcd"
L1 = "0x00000000_0x87654321"
D0, D1 = Device(0, U0, L0, 1), Device(1, U1, L1, 1)


class MockABI:
    def __init__(self, rows, *, init_rc=0, count_rc=0, uuid_rc=0, luid_rc=0):
        def init(flags):
            if flags != 0:
                raise AssertionError("hipInit must use zero flags")
            return init_rc

        def count(out):
            C.cast(out, C.POINTER(C.c_int)).contents.value = len(rows)
            return count_rc

        def get_uuid(out, ordinal):
            C.cast(out, C.POINTER(HipUUID)).contents.bytes[:] = rows[ordinal][0]
            return uuid_rc

        def get_luid(out, mask, ordinal):
            C.memmove(out, rows[ordinal][1], 8)
            C.cast(mask, C.POINTER(C.c_uint)).contents.value = rows[ordinal][2]
            return luid_rc

        self.hipInit = init
        self.hipGetDeviceCount = count
        self.hipDeviceGetUuid = get_uuid
        self.hipDeviceGetLuid = get_luid


ROWS = [
    (uuid.UUID(U0).bytes, struct.pack("<II", 0x1234ABCD, 0x80000001), 1),
    (uuid.UUID(U1).bytes, struct.pack("<II", 0x87654321, 0), 1),
]


class ProbeTests(unittest.TestCase):
    def test_abi_buffer_size_byte_order_and_nuls(self):
        self.assertEqual(C.sizeof(HipUUID), 16)
        self.assertEqual(format_luid(ROWS[0][1]), L0)
        self.assertEqual(enumerate_abi(MockABI(ROWS)), (D0, D1))

    def test_bad_native_results_fail_closed(self):
        bad_rows = (
            [], [ROWS[0], ROWS[0]],
            [ROWS[0], (ROWS[1][0], ROWS[0][1], 2)],
            [(bytes(16), ROWS[0][1], 1)],
            [(ROWS[0][0], bytes(8), 1)],
            [(ROWS[0][0], ROWS[0][1], 0)],
        )
        for rows in bad_rows:
            with self.subTest(rows=rows), self.assertRaises(HipProbeError):
                enumerate_abi(MockABI(rows))
        for key in ("init_rc", "count_rc", "uuid_rc", "luid_rc"):
            with self.subTest(key=key), self.assertRaisesRegex(HipProbeError, "HIP status"):
                enumerate_abi(MockABI(ROWS, **{key: 801}))
        lib = MockABI(ROWS)
        del lib.hipDeviceGetLuid
        with self.assertRaisesRegex(HipProbeError, "not export"):
            enumerate_abi(lib)

    def test_local_dll_only(self):
        self.assertEqual(_lexical_dll(r"C:\Toolkit\Bin\amdhip64_7.dll").name, "amdhip64_7.dll")
        for path in ("amdhip64.dll", r"\\server\share\amdhip64_7.dll",
                     r"\\?\C:\Toolkit\amdhip64_7.dll", r"C:\Toolkit\..\amdhip64.dll",
                     r"C:\Toolkit\other.dll", r"C:Toolkit\amdhip64_7.dll"):
            with self.subTest(path=path), self.assertRaises(HipProbeError):
                _lexical_dll(path)

    def test_uuid_formats_and_mask(self):
        self.assertEqual(normalize_uuid(U0.replace("-", "").upper()), U0)
        for value in ("0", "00000000-0000-0000-0000-000000000000", "GPU-" + U0):
            with self.subTest(value=value), self.assertRaises(HipProbeError):
                normalize_uuid(value)
        base = {"Hip_Visible_Devices": "8", "CUDA_DEVICE_ORDER": "PCI_BUS_ID", "KEEP": "x"}
        calls = []
        def sample(env):
            calls.append(env)
            return [Device(0, U1, L1, 1)] if "HIP_VISIBLE_DEVICES" in env else [D0, D1]
        self.assertEqual(_verify(sample, base, U1), D1)
        self.assertEqual(calls, [{"KEEP": "x"}, {"KEEP": "x", "HIP_VISIBLE_DEVICES": "1"}])
        self.assertEqual(base["Hip_Visible_Devices"], "8")
        self.assertEqual(visibility_environment(base), {"KEEP": "x"})

    def test_verify_rejects_changed_mapping_ignored_mask_or_missing_uuid(self):
        for rows in ([D0, D1], [D0], [Device(0, U1, L0, 1)], [Device(0, U1, L1, 2)]):
            with self.subTest(rows=rows), self.assertRaises(HipProbeError):
                _verify(lambda env: rows if "HIP_VISIBLE_DEVICES" in env else [D0, D1], {}, U1)
        with self.assertRaisesRegex(HipProbeError, "absent"):
            _verify(lambda _: [D0], {}, U1)

    def test_child_wire_validation(self):
        raw = json.dumps({"devices": [{"ordinal": 0, "uuid": U0, "luid": L0, "node_mask": 1}]})
        self.assertEqual(_decode_child(raw), (D0,))
        for bad in ("bad", "{}", '{"devices":[]}', '{"devices":[{"ordinal":0}]}'):
            with self.subTest(bad=bad), self.assertRaises(HipProbeError):
                _decode_child(bad)
        with self.assertRaisesRegex(HipProbeError, "device disappeared"):
            _decode_child('{"error":"device disappeared"}')


if __name__ == "__main__":
    unittest.main()
