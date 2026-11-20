# In-place worker updates

Use an idle maintenance window once to replace plain `serve` with
`supervise`, using the **same spool**, not a new spool. Do not start both.
Keep the original verified checkout available: its supervisor is the stable
front end. Windows named-pipe and Linux Unix-socket paths, agent tokens, job
receipts, and log locations are unchanged across worker updates. Supervisor
options for adapters and pinned providers match `serve`. The supervisor itself
is updated offline.

1. Build or obtain a trusted Luigi release ZIP with the ABI-1 worker.
   Include `worker_entry.py`, `luigi.py` and the `luigi_app/` Python package
   and dashboard assets at the ZIP root. Do not include spool files, tokens,
   logs, `__pycache__` or development data. Verify the checksum through a
   trusted channel separately from downloading the archive.
2. Run the stable supervisor in the normal non-elevated user session:
   `python luigi.py --root ABSOLUTE_SPOOL supervise`.
3. From another terminal run
   `python luigi.py --root ABSOLUTE_SPOOL hot-update ABSOLUTE_ARCHIVE --sha256 HEX_DIGEST`.
   It prompts for the Luigi admin password and returns after accepting staging.
   Inspect the final outcome with `hot-update-status` using the same root.
   Optionally set `--drain-timeout SECONDS` (default 3600). A deadline abandons
   the update; it never overrides any active task's ordinary runtime limit.

The candidate is extracted into a new private release directory and validated
against a SQLite copy. An incompatible schema is refused before pausing
dispatch. The original database is authoritative throughout: pending jobs
keep their order, receipts, owner and timeout. While old jobs (including
contained descendants) drain, clients can still submit, read status or wait.
Only the claim of new jobs pauses. The physical dispatcher lock prevents
overlap: the old dispatcher releases it, the candidate proves it can acquire
it while paused, the supervisor publishes the selected release atomically, and
then enables new claims. The previous endpoint is retired afterwards. Never
replay RPC requests automatically after a transport failure: an unacknowledged
submission may have been committed.

Before activation an unsuccessful update resumes the old worker. If activation
was attempted but safety cannot be proved, the supervisor reports
`recovery_required` rather than starting a second dispatcher or restoring an
outdated database. Restarting a supervisor uses the committed release pointer
and refuses unresolved running reservations. Keep installed releases and
diagnostic logs under the private spool; this version does not delete older
releases automatically. Hot updates cover compatible worker code only, not
schema migrations, OS service configuration, or arbitrary shutdowns. A short
RPC delay at commit is possible; there is no change to the public endpoint.
