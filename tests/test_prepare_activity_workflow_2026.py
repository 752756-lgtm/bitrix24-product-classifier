import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/prepare-activity-plan-2026.yml"
RETIRED_MARKER = (
    ROOT
    / ".github/workflow-triggers"
    / "prepare-activity-plan-2026-bounded-500-skip-1494-deterministic-slice-4.trigger"
)


class PrepareActivityWorkflow2026Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_stable_workflow_is_manual_only_with_direct_inputs(self):
        trigger_header = self.workflow.split("\npermissions:\n", 1)[0]
        self.assertIn("  workflow_dispatch:\n", trigger_header)
        self.assertNotIn("  push:\n", trigger_header)
        self.assertNotIn("schedule:", trigger_header)
        self.assertNotIn("pull_request:", trigger_header)

        dispatch = trigger_header.split("  workflow_dispatch:\n", 1)[1]
        for name, kind in (
            ("writer_run_id", "string"),
            ("max_deals", "string"),
            ("skip_remaining", "string"),
            ("model_workers", "string"),
            ("deterministic_only", "boolean"),
            ("include_category_present", "boolean"),
        ):
            self.assertIn(f"      {name}:\n", dispatch)
            self.assertIn(f"        type: {kind}\n", dispatch)
            self.assertIn("        required: true\n", dispatch)

        for expected in (
            "EXPECTED_WRITER_RUN_ID: ${{ inputs.writer_run_id }}",
            'EXPECTED_WRITER_HEAD_SHA: ""',
            "MAX_DEALS: ${{ inputs.max_deals }}",
            "SKIP_REMAINING: ${{ inputs.skip_remaining }}",
            "MODEL_WORKERS: ${{ inputs.model_workers }}",
            "DETERMINISTIC_ONLY: ${{ inputs.deterministic_only }}",
            "INCLUDE_CATEGORY_PRESENT: ${{ inputs.include_category_present }}",
        ):
            self.assertIn(expected, self.workflow)
        self.assertFalse(RETIRED_MARKER.exists())
        self.assertNotIn("ONE_SHOT_MARKER", self.workflow)
        self.assertNotIn("one_shot", self.workflow)
        self.assertNotIn("github.event_name == 'push'", self.workflow)
        self.assertNotIn("33663228256", self.workflow)
        self.assertNotIn("3bd637ee93dce17a2fa9041450a6bd4c3d1a5368", self.workflow)
        self.assertNotIn(
            "33663228256:3bd637ee93dce17a2fa9041450a6bd4c3d1a5368:"
            "500:1494:1:true:false",
            self.workflow,
        )

    def test_manual_gate_rejects_non_dispatch_stale_and_malformed_inputs(self):
        gate = self.workflow.split(
            "      - name: Fail closed unless this is an exact manual invocation\n",
            1,
        )[1].split(
            "\n      - name: Require main dispatch and terminal current writer plan",
            1,
        )[0]
        for required in (
            '[ "${GITHUB_EVENT_NAME}" != "workflow_dispatch" ]',
            '[ "${GITHUB_REF}" != "refs/heads/main" ]',
            '[ "$(git rev-parse HEAD)" != "${GITHUB_SHA}" ]',
            '[[ ! "${MAX_DEALS}" =~ ^[0-9]+$ ]]',
            '[[ ! "${SKIP_REMAINING}" =~ ^[0-9]+$ ]]',
            '[[ ! "${MODEL_WORKERS}" =~ ^[1-5]$ ]]',
            'case "${DETERMINISTIC_ONLY}:${INCLUDE_CATEGORY_PRESENT}" in',
            '[[ ! "${EXPECTED_WRITER_RUN_ID}" =~ ^[0-9]+$ ]]',
        ):
            self.assertIn(required, gate)
        self.assertNotIn("GITHUB_EVENT_PATH", gate)
        self.assertNotIn("GITHUB_RUN_ATTEMPT", gate)

    def test_checkout_is_full_main_without_persisted_credentials(self):
        checkout = self.workflow.split(
            "      - name: Checkout main without persisted credentials\n", 1
        )[1].split(
            "\n      - name: Fail closed unless this is an exact manual invocation",
            1,
        )[0]
        self.assertIn("          ref: main\n", checkout)
        self.assertIn("          fetch-depth: 0\n", checkout)
        self.assertIn("          persist-credentials: false\n", checkout)

    def test_terminal_gate_binds_run_to_exact_2026_completion_tag_head_and_ancestor(self):
        terminal = self.workflow.split(
            "      - name: Require main dispatch and terminal current writer plan\n",
            1,
        )[1].split("\n      - name: Validate inputs and required secrets", 1)[0]
        for expected in (
            '"${api}/actions/runs/${EXPECTED_WRITER_RUN_ID}"',
            "actions/workflows/precision-backfill-2026.yml/runs",
            "classifier/data/precision-2026.allowlist",
            "classifier/data/precision-2026-taxonomy.json",
            'completion_marker="precision-2026-complete-${fingerprint}"',
            'if [ "${tag_row_count}" != "1" ]; then',
            '[ "${tag_ref}" != "refs/tags/${completion_marker}" ]',
            'exact.get("head_sha") != expected_head',
            'exact.get("path") != ".github/workflows/precision-backfill-2026.yml"',
            "if tag_target != expected_head:",
            'git merge-base --is-ancestor "${tag_target_sha}" "${GITHUB_SHA}"',
        ):
            self.assertIn(expected, terminal)
        self.assertIn("group: bitrix24-category-backfill-global-hosted", self.workflow)
        self.assertNotIn("resolve_terminal_writer", self.workflow)
        self.assertNotIn("precision-backfill-2025.yml", self.workflow)
        self.assertNotIn("precision-2025.", self.workflow)

    def test_scan_audit_keeps_independent_bounded_retry_and_safe_diagnostics(self):
        audit = self.workflow.split(
            "      - name: Read-only full-year discovery audit\n", 1
        )[1].split("\n      - name: Create public-assets-only draft pull request", 1)[0]
        for expected in (
            "for audit_attempt in 1 2; do",
            'audit_state="${SCAN_STATE}.${audit_attempt}"',
            'audit_status="${SCAN_STATUS}.${audit_attempt}"',
            'audit_plan_key="${PRIVATE_DIR}/scan-${audit_attempt}.key"',
            'audit_lock="${PRIVATE_DIR}/scan-${audit_attempt}.lock"',
            'PRECISION_STATE_PATH="${audit_state}"',
            'PRECISION_STATUS_PATH="${audit_status}"',
            'PRECISION_PLAN_KEY_PATH="${audit_plan_key}"',
            'PRECISION_LOCK_PATH="${audit_lock}"',
            'if [ "${audit_attempt}" -eq 1 ]; then',
            "sleep 60",
            'if [ "${audit_rc}" -ne 0 ]; then',
            '"failure_stage": "full_year_discovery_audit"',
            '"failure_code": "scan_worker_exit_nonzero"',
            '"status_available": False',
            '"plan_total": nonnegative_int(status.get("plan_total"))',
            '"scan_complete": exact_bool(status.get("scan_complete"))',
            '"permanent_error",',
            'print("audit_summary=" + json.dumps(summary, sort_keys=True))',
        ):
            self.assertIn(expected, audit)
        self.assertEqual(audit.count("python -m classifier.precision_worker --scan-only"), 1)
        self.assertEqual(audit.count('python - "${audit_status}" <<\'PY\''), 2)
        self.assertNotIn('status.get("last_error")', audit)
        self.assertNotIn('print(Path("${SCAN_LOG}")', audit)
        self.assertNotIn("PRIVATE_LOG", audit)

    def test_publisher_and_writer_remain_bound_to_2026_assets(self):
        for expected in (
            "--year 2026",
            "--parent-map classifier/data/precision-2026-parent-map.json",
            'PRECISION_YEAR: "2026"',
            'git commit -m "Prepare reviewed activity evidence plan for 2026"',
            "git add --intent-to-add --",
        ):
            self.assertIn(expected, self.workflow)

        writer = (
            ROOT / ".github/workflows/precision-backfill-2026.yml"
        ).read_text(encoding="utf-8")
        writer_triggers = writer.split("\npermissions:\n", 1)[0]
        self.assertIn("group: bitrix24-category-backfill-global-hosted", writer)
        self.assertIn('"classifier/data/precision-2026.allowlist"', writer_triggers)
        self.assertIn('"classifier/data/precision-2026-taxonomy.json"', writer_triggers)
        self.assertNotIn('"classifier/precision_worker.py"', writer_triggers)
        self.assertNotIn("schedule:", writer_triggers)
        self.assertNotIn("cron:", writer_triggers)

    def test_2026_parent_map_is_bound_to_2026(self):
        parent_map = json.loads(
            (ROOT / "classifier/data/precision-2026-parent-map.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(parent_map["year"], 2026)


if __name__ == "__main__":
    unittest.main()
