"""Run unittest discovery; required native and fixture smoke tests must pass."""
import os
import sys
import unittest

COMMON = {
    "test_role_denials_and_owner_mutations",
    "test_concurrent_submitters_atomic_fifo_and_no_overlap",
    "test_only_authenticated_snapshot_invokes_provider",
    "test_prompt_once_and_allow_only_read_snapshot",
    "test_gpu_uuid_and_unbound_ambiguity",
    "test_root_cli_entry_from_another_directory",
    "test_unknown_or_malformed_load_never_idle",
    "test_mig_modes_fail_closed_even_with_physical_uuid",
    "test_named_nvidia_requires_fresh_samples_and_cpu",
}
LINUX = {
    "test_root_is_rejected",
    "test_live_and_stale_cleanup_never_unlinks_regular_file",
    "test_private_root_and_key_reject_link_or_permissions",
    "test_partial_frame_respects_deadline_then_serves_cli_and_dashboard_snapshot",
    "test_descendants_drain_before_release",
    "test_timeout_while_draining_kills_group",
    "test_kill_running_group_and_continue_queue",
    "test_mixed_provider_exact_current_bdf_join",
    "test_gpu_default_holds_when_missing_and_explicit_bypass_keeps_cpu_gate",
    "test_no_monitor_cli_refuses_binding_options",
}
WINDOWS = {
    "test_isolated_gate_does_not_import_from_workload_cwd",
    "test_win_pipe_raw_json_admin_then_agent",
    "test_win_timeout_does_not_count_queue_wait_and_stops_children",
    "test_win_kill_is_enforced_by_owner_while_running",
    "test_windows_kill_tree_before_next_job",
}


class RequiredResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.passed = set()

    def addSuccess(self, test):
        super().addSuccess(test)
        self.passed.add(test.id().rsplit(".", 1)[-1])


def main():
    if sys.platform == "linux":
        if os.geteuid() == 0:
            raise SystemExit("CI must run as a non-root user to exercise native Linux socket tests")
        required = COMMON | LINUX
    elif sys.platform == "win32":
        required = COMMON | WINDOWS
    else:
        raise SystemExit(f"CI has no native smoke test list for {sys.platform}")
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_*.py", top_level_dir=".")
    if not suite.countTestCases():
        raise SystemExit("No tests discovered")
    result = unittest.TextTestRunner(verbosity=2, resultclass=RequiredResult).run(suite)
    missing = required - result.passed
    if missing:
        print("Required tests did not pass: " + ", ".join(sorted(missing)), file=sys.stderr)
    return int(not result.wasSuccessful() or bool(missing))


if __name__ == "__main__":
    sys.exit(main())
