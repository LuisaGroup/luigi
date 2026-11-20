# Luigi: local CPU and GPU job queues

Windows-only local named pipe for the Luigi queue; CPython 3.10+ and no
third-party dependency. **This
is an API convention under one trusted Windows account, not a security
boundary.** Same-SID processes can bypass the CLI, access the spool, or read
credential files. Do not deploy for mutually untrusted jobs or privileged
work. No network listener, no elevation, no firewall/ACL/account changes.
Direct SQLite tooling bundled for engine internals is not an agent interface.

## Setup and CLI

First run only in a fresh, private spool from a standard user's PowerShell:
```powershell
$python = (Get-Command python.exe -ErrorAction Stop).Source
& $python .\luigi.py bootstrap
& $python .\luigi.py serve --gpu-adapter all
```
Bootstrap asks for the new password twice without echo. Keep this PowerShell
open for the foreground broker; Ctrl+C stops intake and waits for active jobs.
Use an interactive PowerShell (not redirection, chat, or command arguments)
for every password prompt. Keep the default identity and spool files under
private LocalAppData; clients verify the broker's HMAC challenge before
sending any credentials. This protects against a local pipe squatter without
the identity file, not deliberate access by a trusted same-SID process.

`all` conservatively gates on the busiest visible GPU engine across adapters;
use an explicit actual adapter LUID only when known. Verify native Windows load
telemetry before production jobs.
On failure or missing telemetry, queued jobs remain held rather than starting.
The broker does not interrupt jobs already running when CPU/GPU load changes.
CPU and GPU workers are independent and enforce one active job per lane;
submitting a GPU workload to the CPU lane is outside the contract.

From another PowerShell (set `$python` in each window):
```powershell
& $python .\luigi.py create cpu
& $python .\luigi.py create gpu
& $python .\luigi.py queues
& $python .\luigi.py invite --out $privateInviteFile
& $python .\luigi.py enroll --invite-file $privateInviteFile --out $privateTokenFile
$jobPython = & $python -c 'from pathlib import Path; import sys; print(Path(sys.executable).resolve())'
$receipt = & $python .\luigi.py submit --token-file $privateTokenFile --queue cpu -- $jobPython -c 'print(101)'
& $python .\luigi.py status --token-file $privateTokenFile $receipt
& $python .\luigi.py wait --token-file $privateTokenFile $receipt --timeout 60
& $python .\luigi.py log --token-file $privateTokenFile $receipt --stream stdout
```
The two file variables must be **fresh paths in the operator's private local
profile**, not Git, shared files, env, tool transcript, or job inputs.
Admin operations prompt for the password without echo. The app stores
scrypt admin verifier and hashed invitations and bearers. Invitation TTL
defaults to five minutes and redemption is single-use. Agent token TTL is
seven days; use `rotate --token-file ... --out ...` before expiry and share
only the agent's bearer file with that agent. Admin `revoke AGENT_ID` ends
that agent's future API access, not its current running jobs. Never include
enrollment secrets or a password in a command's arguments or workload output.
No credentials are injected into job environments; the broker copies a narrow
allowlist of runtime variables, so use an explicit workload wrapper for
application-specific settings.

- Admin only: create/reset lanes, list queue summaries, invite, revoke, recover
  an interrupted lane, make fixed-location metadata/log backups. Reset
  cancels only pending jobs; it does not terminate running jobs. Backup
  excludes auth verifier/tokens and execution scratch, requires both lanes
  quiescent, and is inspection/export only with no restore command.
- Agent only: submit to an admin-created lane, list own, read/wait/log own,
  cancel own queued, kill own running Windows Job Object tree, and replay own
  terminal job to a fresh receipt. The original lane is preserved. Admin
  password is not an implicit cross-owner kill permission. `status` embeds
  hold reason for queued work; inspect the result as JSON.
- Jobs default to their own work and temporary directories and separate stdout
  and stderr. `submit --cwd C:\path` preserves an existing work directory.
  Use absolute executable and input paths. App-managed caches are separate:
  applications may store caches beside the executable, independently of CWD
  or TMP. Use separate application cache roots for isolated cold benchmarks.
- Luigi cannot preempt unmanaged work or detect which application owns the GPU.
  Processes started outside the queue are never adopted. Synthetic tests require
  no GPU.
- Stop from the foreground console with Ctrl+C to drain the active jobs then
  stop, leaving pending work queued. After a crash verify descendants are
  gone, then use `recover UUID --confirmed-stopped` as admin, no auto replay.
- Named pipe uses local-only Win32 pipe mode and bounded byte/JSON framing,
  not pickle and not TCP. Same-SID callers can spoof a pipe or edit files as
  described above; app authorization cannot prevent it.

## Verify

`& $python -m unittest -v` runs synthetic tests. Linux can run the direct
policy/scheduler/guard tests without Windows; the native named-pipe test,
owner kill enforcement, Win32 GPU telemetry and a smoke check on the target
host are separate requirements. Luigi runs in the foreground only; it includes
no service, startup entry, or scheduled task. Pending receipts persist when
the terminal closes, but processing requires a new foreground session. Do not
hand it live GPU work until
native smoke tests and sensor behavior are verified.

Job executable and explicit cwd must be physical absolute local drive paths.
UNC, device paths, mapped network drives, and reparse points (including
package-manager aliases or junctions) are rejected. Resolve trusted local installation
paths yourself before submitting, as shown above; the broker never follows them.

## Named queues and runtime limits (successor)

This uses a new spool, `%LOCALAPPDATA%\LuigiMultiQueue`; pilot spools are
not migrated. `create cpu` and `create gpu` retain their original default
kinds. Other names require `create NAME --kind cpu|gpu`; names contain a
lowercase letter followed by up to 31 lowercase letters, digits or hyphens.
For example:

```powershell
& $python .\luigi.py create build --kind cpu
$receipt = & $python .\luigi.py submit --token-file $privateTokenFile --queue build --queue gpu --timeout 120 -- $jobPython -c 'print(101)'
```

Repeat `--queue` to request up to 16 distinct named queues. Omitting it still
requests `gpu`. A job starts only at the head of every requested queue,
with every requested queue free and its load gate open. A job waiting at
the head of one queue blocks later jobs on that queue, even if it is also
waiting on another queue. Unrelated queues can progress. No reservations
are taken until all requirements can be granted together. Resetting any
one queue cancels pending jobs that requested it, clearing those jobs
from all requested queues. Replay preserves the requested queues; an
interrupted job continues holding all of them pending verified recovery.

`create NAME --kind gpu --gpu-uuid UUID` records a stable, nonzero UUID.
Windows AMD HIP binding is opt-in at broker startup, with an operator-selected
`--hip-dll` absolute local runtime path and one or more `--hip-workload`
absolute local executable paths. Pin an app-local, byte-identical copy of
the runtime alongside each approved executable. An existing mismatched or
ambiguous app-local runtime is rejected. Only approve trusted applications
that load that app-local runtime and honor HIP device visibility. This is
cooperative admission, not an OS restriction, and does not isolate CUDA,
DirectX, Vulkan, or programs that override the environment.

The broker periodically verifies the stable HIP UUID, live ordinal, WDDM LUID
and node mask in fresh processes, then samples only the matching adapter's
WDDM headroom. Before launch it independently checks the actual app-local
runtime and verifies a clean masked child can see only the expected UUID at
ordinal zero. Stale, uncertain or unavailable data keeps jobs queued without
starting their timeout. No fallback to a numeric device setting or aggregate
GPU utilization is used for UUID-bound queues. Duplicate UUIDs are rejected,
as is mixing an unbound GPU queue with a UUID-bound queue, or requesting
multiple GPU queues in one job. Native test coverage and compatibility depend
on the installed AMD runtime exporting the UUID and LUID operations; no native
HIP workload is started as part of synthetic tests.

The runtime timeout is 1800 seconds by default, configurable through
`submit --timeout SECONDS` (integer between 1 and 86400). It starts when
the contained Windows child is released, not when queued or load-held.
This differs from `wait --timeout`, which only limits how long the client
waits. A task timeout terminates its own Windows Job Object, waits for its
process tree to be empty, preserves logs, and reports `timed_out`. `draining`
indicates the root exited while the contained job tree still has children.
A stale or uncertain process tree continues holding its queues.
