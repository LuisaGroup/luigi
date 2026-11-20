# NVIDIA provider integration contract

`luigi_app/platforms/nvidia_probe.py` is standalone stdlib code. No probing occurs on import.

```python
provider = NvidiaProvider(smi_path, driver_path, workloads=[absolute_executable])
rows = provider.inventory_load()  # tuple NvidiaDevice(uuid, pci_bus_id, percent, observed_at)
row = provider.verify_device(full_gpu_uuid)
provider.validate_submission(argv)
env = provider.launch_environment(full_gpu_uuid, argv, base_env, busy_at=80.0)
```

Use only for queues explicitly selecting `nvidia`; do not infer provider from
an ordinal or fall back to AMD/global telemetry on error. Map every exception
to a held queue / unknown measurement, never zero utilization. Cache scheduler
measurements only through its existing monotonic freshness/cooldown guard.
The provider itself does not cache measurements. `observed_at` is Unix time.
Linux inventory matching must independently compare canonical PCI BDFs with the
sysfs NVIDIA inventory; incomplete or mismatched inventories must hold generic
GPU queues. Recheck the scheduler CPU/cooldown decision after the launch proof.

## Operator configuration and supported scope

- Explicit absolute installed `nvidia-smi` / `nvidia-smi.exe`, NVIDIA driver
  library (`libcuda.so.1` resolved to its installed file, or `nvcuda.dll`), and
  approved CUDA workload executable paths. No PATH lookup, shell, downloads,
  installation or arbitrary command arguments. SHA-256 pins detect changes
  since configuration; they are not vendor signatures or authentication.
- Linux/Windows physical full-device NVIDIA GPUs, full `GPU-8-4-4-4-12` UUIDs.
  Windows probes are hidden. Unsupported metrics, missing devices, duplicates,
  malformed/stale responses and failed probes hold. MIG current/pending enabled
  or unknown modes hold. `N/A` MIG mode on non-MIG hardware is permitted only
  with a numeric utilization reading; `N/A` utilization always holds.
- Without driver + approved workloads, inventory is discovery-only and launch
  fails closed. No claim of GPU execution validation: all tests are fake.
- Workloads are operator-attested native CUDA applications that honor the mask,
  use the selected system driver, and do not alter environment or spawn in
  another GPU namespace. Arbitrary shell/interpreter wrappers, containers,
  MIG, MPS, vGPU and hostile workloads are unsupported. This is cooperative
  routing, not security isolation. Restrict broker access to trusted operators.
- Launch proof runs a separate isolated Python process in the executable's
  directory with the exact sanitized workload environment. It initializes the
  explicitly pinned CUDA driver, checks that only one CUDA device is visible,
  and compares its full UUID and PCI BDF. It does not create a CUDA context or
  launch a kernel. It does not prove a subsequently replaced executable or a
  workload that intentionally loads a different driver will obey the mask.
- The environment excludes inherited mask, MPS, Python/loader injection and
  PATH variables; only basic OS/user/temp/locale values are retained. Approved
  workloads must work under this documented environment. Drivers/tools that
  need untrusted loader-path overrides are not supported.
- Launch takes fresh utilization readings before and after the driver proof
  and holds at `busy_at`. Scheduler recovery hysteresis remains authoritative.

## Official references

NVIDIA documents selective CSV output, UUID identity stability, unsupported
`N/A` fields, and unsupported utilization on MIG-enabled devices:
https://docs.nvidia.com/deploy/nvidia-smi/

CUDA supports full GPU UUID masks and enumerates only visible devices:
https://docs.nvidia.com/cuda/cuda-programming-guide/05-appendices/environment-variables.html

CUDA driver enumeration and UUID APIs:
https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__DEVICE.html

## Validation performed

`PYTHONDONTWRITEBYTECODE=1 python -m unittest tests.test_nvidia_gpu_probe`
uses fake command output, fake ctypes functions, and temporary inert files only.
No NVIDIA executable, GPU library or GPU workload was actually run. Hardware,
Windows runtime and full scheduler integration validation remain separate.
