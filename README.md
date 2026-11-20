# Luigi: local CPU and GPU job queues

Luigi schedules local work on **Windows and Linux**, with named FIFO queues,
per-job timeouts, and an optional [read-only dashboard](docs/dashboard.md).
Use CPython 3.10 or newer; there are no third-party Python dependencies.

Run Luigi as a regular user, not as Windows Administrator or Linux root.
The broker uses a local Windows named pipe or a Unix-domain socket on Linux.
Application roles assume one trusted OS account: same-account processes can
access the spool or bypass the CLI. Luigi is not a sandbox for untrusted
programs. It does not install a service, change system permissions, or expose
the broker over TCP.

## Quick start

Open two terminals in the repository checkout. Run `bootstrap` only once for
a fresh spool. It prompts for an administrator password without echo. Leave
`serve` open in the first terminal.

### Linux (sh / Bash)

**Terminal 1: start the broker**

```sh
python3 luigi.py bootstrap
python3 luigi.py serve
```

**Terminal 2: submit and inspect a CPU job**

```sh
python3 luigi.py create cpu
private_dir=$(mktemp -d "$HOME/.luigi-client.XXXXXX")
python3 luigi.py invite --out "$private_dir/invite"
python3 luigi.py enroll --invite-file "$private_dir/invite" --out "$private_dir/token"
job_python=$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.executable).resolve())')
receipt=$(python3 luigi.py submit --token-file "$private_dir/token" --queue cpu --timeout 120 -- "$job_python" -c 'print(101)')
python3 luigi.py status --token-file "$private_dir/token" "$receipt"
python3 luigi.py wait --token-file "$private_dir/token" "$receipt" --timeout 60
python3 luigi.py log --token-file "$private_dir/token" "$receipt" --stream stdout
```

The default spool is `$XDG_STATE_HOME/luigi`, or `~/.local/state/luigi` when
`XDG_STATE_HOME` is unset. Keep the spool private (mode 0700) and use a
physical, nonsymlink path if specifying `--root`. See [Linux setup](docs/linux.md).

### Windows (PowerShell)

**Terminal 1: start the broker**

```powershell
$python = (Get-Command python.exe -ErrorAction Stop).Source
& $python .\luigi.py bootstrap
& $python .\luigi.py serve
```

**Terminal 2: submit and inspect a CPU job**

```powershell
$python = (Get-Command python.exe -ErrorAction Stop).Source
& $python .\luigi.py create cpu
$privateDir = Join-Path $env:LOCALAPPDATA ("LuigiClient-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $privateDir -ErrorAction Stop | Out-Null
$invite = Join-Path $privateDir "invite"
$token = Join-Path $privateDir "token"
& $python .\luigi.py invite --out $invite
& $python .\luigi.py enroll --invite-file $invite --out $token
$jobPython = & $python -c 'from pathlib import Path; import sys; print(Path(sys.executable).resolve())'
$receipt = & $python .\luigi.py submit --token-file $token --queue cpu --timeout 120 -- $jobPython -c 'print(101)'
& $python .\luigi.py status --token-file $token $receipt
& $python .\luigi.py wait --token-file $token $receipt --timeout 60
& $python .\luigi.py log --token-file $token $receipt --stream stdout
```

The default spool is `%LOCALAPPDATA%\LuigiMultiQueue`. Legacy pilot spools
are not migrated. Submitted executables and explicit working directories
must be physical absolute local paths; UNC paths, mapped drives, and
reparse points (including junctions) are rejected.

On either platform, run `python3 luigi.py dashboard` (Linux) or
`python .\luigi.py dashboard` (Windows) while the broker is running. Enter
the admin password at the hidden prompt and open the printed private
link in a same-machine browser. Do not share or forward the link.

## Queue and job behavior

- The quick start creates a CPU queue only. `create gpu` creates the default
  generic GPU queue; `create NAME --kind cpu|gpu` creates other named queues.
  Names start with a lowercase letter and contain up to 32 lowercase letters,
  digits or hyphens. Always specify `--queue cpu` for CPU-only submissions;
  omitting `--queue` selects `gpu`.
- Repeat `--queue` to request up to 16 distinct queues, for example
  `--queue cpu --queue gpu`. A job starts only when it is first in every
  requested queue and each queue is free and passes its load check. Queues
  are acquired together. Earlier jobs waiting on one queue block later
  jobs on that queue; unrelated queues can proceed. A job can use at most
  one GPU queue.
- `submit --timeout` is the runtime limit (default 1800 seconds, allowed
  1–86400); it starts at launch, not while queued. `wait --timeout` only
  limits the client's wait. A timed-out job keeps its logs. Windows uses
  Job Objects; Linux uses process groups and a subreaper. `draining` means
  the root exited but tracked children still hold the queues. On Linux,
  deliberately escaping the process group is not supported; see
  [Linux job lifecycle](docs/linux.md#job-lifecycle).
- Stop the foreground broker with Ctrl+C. Windows drains active jobs; Linux
  cancels active contained groups. Pending jobs persist. After an abrupt
  stop, verify all relevant work has stopped before using the admin
  `recover RECEIPT --confirmed-stopped` command; there is no auto-replay.
- Jobs have private work/temp directories and separate stdout/stderr.
  Use absolute executable and input paths; `submit --cwd` selects an existing
  working directory. Applications may store caches beside the executable
  independently of CWD and TMP. Use separate application cache roots for
  isolated benchmarks.

An admin can create, inspect, or reset queues, issue invitations, revoke
agents, recover interrupted jobs, and export metadata/log backups. A reset
cancels pending work, not active jobs. Backups require quiescent queues and
exclude credentials and execution scratch. An agent can submit, list,
inspect, wait, read logs, cancel, kill, or replay its own jobs using its
token; a replay gets a new receipt. Admin authentication does not implicitly
authorize killing another owner's job.

Keep invitations and tokens in fresh files in a private local directory;
never put them in Git, command arguments, workload output, or chat. An
invitation expires after five minutes by default and can be used only once.
Agent tokens expire after seven days; use `rotate --token-file ... --out ...`
before expiry. Job environments receive no Luigi credentials and only a
narrow allowlist of runtime variables.

## GPU support

| Capability | Windows | Linux |
| --- | --- | --- |
| Generic GPU load guard | WDDM counters | AMD amdgpu sysfs and opt-in NVIDIA telemetry; every detected PCI GPU must be covered |
| AMD HIP UUID binding | Optional pinned app-local HIP runtime and allowlisted executable | Not available; UUID-bound AMD jobs do not launch |
| NVIDIA CUDA UUID binding | Optional pinned NVIDIA provider | Optional pinned NVIDIA provider |

By default, missing or stale GPU telemetry holds GPU work. Luigi does not
preempt external work or interrupt running jobs when load rises. Windows
supports `serve --gpu-adapter all` (the default) or a known adapter LUID for
generic monitoring. Linux supports only `all`.

If you deliberately accept overlap with outside GPU jobs, start the broker
with `serve --no-gpu-monitor`. One generic GPU queue still runs serially in
FIFO order and the CPU load check still applies. This disables UUID binding
and refuses a spool containing UUID-bound queues; it never activates
automatically.

- **AMD on Windows:** opt in with an absolute physical `--hip-dll` and
  repeatable `--hip-workload` paths at startup. Each approved HIP executable
  must ship a byte-identical app-local runtime. Luigi verifies the hardware
  UUID, WDDM load, and masked visibility before launch.
- **NVIDIA on either platform:** see [NVIDIA setup](docs/nvidia.md).
  Configure `--nvidia-smi`, `--cuda-driver` and repeatable `--cuda-workload`
  paths for UUID-bound execution. Use the full physical `GPU-…` UUID and
  `create NAME --kind gpu --gpu-provider nvidia --gpu-uuid UUID`. NVIDIA
  automated coverage uses fixtures, not a real GPU.

Generic and UUID-bound queues cannot share a spool. Duplicate GPU UUIDs
are rejected. UUID routing applies only to trusted workloads that honor the
HIP or CUDA selector; it is not hardware isolation and does not bind
DirectX, Vulkan, or other APIs.

## Development

Run `python3 -B -m tests.ci` on Linux or `python -B -m tests.ci` on Windows
from the checkout. The [CI workflow](.github/workflows/ci.yml) covers Ubuntu
and Windows on Python 3.10 and 3.13 and requires the corresponding native IPC
tests to pass. Tests do not need GPU hardware. To check the dashboard
JavaScript separately, run `node --check luigi_app/dashboard/assets/dashboard.js`.

## Worker hot updates

See [in-place worker updates](docs/hot-update.md) to drain and switch versions without changing the client endpoint.
