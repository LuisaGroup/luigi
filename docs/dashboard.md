# Luigi local dashboard

Read-only local web companion, not a hosted site and not a Windows deployment.
All assets are bundled alongside dashboard.py, using the Python standard library.

## Integration

    from dashboard import open_dashboard
    session = open_dashboard(read_only_snapshot)
    # Trusted CLI gives session.url to the local operator/browser.
    # Do not write it to ordinary logs or automate publishing it.
    ...
    session.close()

Inject one no-argument callback that returns an authorized, time-bounded,
read-only broker snapshot. A separately launched CLI should authenticate to a
dedicated named-pipe snapshot op with the appropriate dashboard visibility role;
it must never expose a general broker proxy, admin password, invitation, or
agent token to the page. A broker-owned integrated launcher can instead inject
its internal read-only snapshot method. The caller, not the dashboard server,
decides who may start an operator-wide view; agent views must already be scoped
to their authorized data. Do not open an operator session from an arbitrary
agent request.

Schema:

    {
      "queues": [{"name": "hip-rx", "queued": 3, "active": 1, "held": 3,
                  "hold_reason": "gpu_busy"}],
      "active": [{"id": "<receipt UUID>", "queues": ["cpu", "hip-rx"],
                  "status": "running", "started_at": "<ISO-8601 with timezone>"}],
      "recent": [{"id": "<receipt UUID>", "queues": ["hip-rx"],
                  "status": "succeeded", "finished_at": "<ISO-8601 with timezone>"}],
      "telemetry": {
        "cpu": {"percent": 20.5, "observed_at": "<ISO-8601 with timezone>", "reason": "ready"},
        "gpu": [{"queue": "hip-rx", "percent": null, "observed_at": null, "reason": "no_sample"}]
      }
    }

Supply allowlisted machine-readable reason codes, e.g. gpu_busy, cpu_stale,
blocked_by_fifo, gpu_mapping_stale, or recovering_1_of_2. An unknown/free-text
reason becomes Unknown rather than leaking paths or exceptions. Counts and
timed telemetry with unknown values should be null, not invented zeroes.
The HTML uses textContent and local assets only. It shows connection state,
last successful refresh, per-sample timestamps and unknown/stale measurements.

## Local session safeguards

The HTTP server listens on 127.0.0.1 and an ephemeral port only. Snapshot GETs
require a random 256-bit capability from the launch URL's fragment. The page
strips the fragment immediately, keeps the capability only in page memory,
and sends it in the Authorization header. Browser reload then requires
reopening from the local launcher. No cookies, persistent browser storage,
request/exception logs, administrative routes, external assets, or CORS.
Strict Host/Origin checks, no-store, restrictive CSP, and no-referrer are set.
Anyone who obtains the launch URL or capability during a session can view its
sanitized data. This is not protection from arbitrary same-user software,
browser extensions, process inspection, or a compromised machine. Close the
session on CLI exit. Do not forward the port or run a reverse proxy over it.

Validate with:
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v
    node --check dashboard.js

The tests use an injected fixture and cloud loopback only. There has been no
Windows broker, authenticated pipe or browser UI deployment/test in this bundle.

## Bundled successor CLI (manual local opening)

Run python luigi.py dashboard in an interactive local terminal while the
successor broker is running. Enter the admin password once at the hidden
prompt. The CLI first authenticates to the named-pipe server and reads one
admin-only snapshot. Only then does it start loopback HTTP and print a private
viewing link to that terminal; it never auto-opens a browser through process
arguments. Paste the link into a browser on the same computer and press Enter
in the Luigi console when done. The broker's password never enters the page,
URL fragment, browser process, or static assets.

For Windows visual QA without live data, run python -m tests.dashboard_fixture in a
local terminal; use healthy, held, or disconnected to switch the synthetic
fixture in the same browser tab. Keep its temporary link local.
