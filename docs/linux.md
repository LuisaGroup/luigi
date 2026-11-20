# Linux setup

Luigi runs in a regular user session. No root, service, cgroup setup, setuid,
or network listener is needed. Use CPython 3.10 or newer.

From a checkout, in two terminals:

```sh
python3 luigi.py bootstrap     # prompts for an admin password without echo
python3 luigi.py serve         # starts the local broker in the foreground
```

```sh
python3 luigi.py create cpu
python3 luigi.py create gpu   # optional; see monitoring below
python3 luigi.py queues
python3 luigi.py dashboard    # optional private loopback display
```

Keep the default `${XDG_STATE_HOME:-$HOME/.local/state}/luigi` directory
private (mode 0700). Use a physical path with no symlinks for `--root`.
Linux verifies same UID for both ends of the Unix-domain socket and uses
its application role check and server challenge before sending credentials.
Same-UID processes can bypass the CLI. This is not a hostile-code sandbox.

Enroll an agent with a one-use invitation, using fresh paths in an
operator-private directory for both invitation and token. Agents run
`python3 luigi.py submit --token-file TOKEN --queue cpu -- /absolute/exe args`.
Repeat `--queue` for a task that requires several queues. See the main
README for cancellation, log, replay and recovery semantics.

## Load monitoring and GPU selection

CPU is sampled from Linux procfs. Generic GPU queues gate on the busiest
identified AMD or NVIDIA PCI device; incomplete readings prevent launches.
AMD activity is sampled from the amdgpu sysfs channel. For NVIDIA, opt in
to the trusted vendor utility with `serve --nvidia-smi /physical/path/nvidia-smi`.

For a UUID-bound NVIDIA CUDA queue, also pin the trusted CUDA driver file
with `--cuda-driver` and one or more trusted executables with
`--cuda-workload`. Then use
`create render --kind gpu --gpu-provider nvidia --gpu-uuid GPU-<full UUID>`.
This only covers trusted CUDA programs honoring the selector. It does not
bind Vulkan, OpenGL or programs that ignore the CUDA runtime selection.
Linux AMD monitoring does not identify a stable HIP UUID for dispatch;
such Linux UUID-bound AMD queues fail closed.

If GPU load cannot be measured and you explicitly choose to accept overlap
with external work, start with `serve --no-gpu-monitor`. CPU admission and
ordinary FIFO ordering still apply; the single generic GPU lane is allowed.
This mode deliberately disables UUID binding and refuses spools already
containing UUID-bound queues. It never activates automatically.

## Job lifecycle

The worker uses an isolated child subreaper and a per-job process group.
Parent exit is not completion: in-group descendants are drained before the
receipt finishes. Timeouts and kill requests only signal that original group
while its leader is pinned. A process that deliberately escapes the group is
not killed as a fallback; its still-observed work can retain reservations.
Do not use Luigi as an isolation boundary for untrusted executables.

Use Ctrl+C or SIGTERM to stop accepting work and cancel active Linux groups.
A crash leaves durable reservations in the spool even if the supervisor
was able to clean up its group, so subsequent conflicting jobs stay held.
Verify that all descendants are stopped before admin recovery. Luigi never
signals a stored or reused PID during recovery.
