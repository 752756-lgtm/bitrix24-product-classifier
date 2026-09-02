import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/prepare-activity-plan-2026.yml"
MARKER_RELATIVE = Path(
    ".github/workflow-triggers/"
    "prepare-activity-plan-2026-bounded-500-skip-996-deterministic-slice-3.trigger"
)
MARKER_PATH = ROOT / MARKER_RELATIVE
MARKER_SHA256 = "537c0d4af85de639d6fb30201d57c1d1b5f1fffc4f805cbf04f8ede34389ba83"


class PrepareActivityBoundedOneShotWorkflow2026Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_only_manual_and_exact_marker_push_can_trigger(self):
        trigger_header = self.workflow.split("\npermissions:\n", 1)[0]
        self.assertIn("  workflow_dispatch:\n", trigger_header)
        self.assertIn(
            "  push:\n"
            "    branches:\n"
            "      - main\n"
            "    paths:\n"
            f'      - "{MARKER_RELATIVE.as_posix()}"\n',
            trigger_header,
        )
        self.assertEqual(trigger_header.count("  push:\n"), 1)
        self.assertNotIn("schedule:", trigger_header)
        self.assertNotIn("pull_request:", trigger_header)

        checkout = self.workflow.split(
            "      - name: Checkout main without persisted credentials\n", 1
        )[1].split(
            "\n      - name: Fail closed unless this is an exact authorized invocation",
            1,
        )[0]
        self.assertIn("          ref: main\n", checkout)
        self.assertIn("          fetch-depth: 0\n", checkout)
        self.assertIn("          persist-credentials: false\n", checkout)

    def test_marker_payload_and_hash_are_byte_exact(self):
        self.assertEqual(
            MARKER_PATH.read_bytes(),
            b"prepare-activity-plan-2026\n"
            b"mode=fixed-terminal-writer\n"
            b"writer_run_id=33657165406\n"
            b"max_deals=500\n"
            b"skip_remaining=996\n"
            b"model_workers=1\n"
            b"deterministic_only=true\n"
            b"include_category_present=false\n"
            b"one_shot=true\n"
            b"slice=3\n",
        )
        self.assertEqual(hashlib.sha256(MARKER_PATH.read_bytes()).hexdigest(), MARKER_SHA256)
        self.assertIn(f"ONE_SHOT_MARKER_SHA256: {MARKER_SHA256}", self.workflow)

    def test_push_has_fixed_slice_inputs_and_manual_inputs_are_preserved(self):
        dispatch = self.workflow.split("  workflow_dispatch:\n", 1)[1].split(
            "  # Temporary one-shot bridge", 1
        )[0]
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
            "EXPECTED_WRITER_RUN_ID: ${{ github.event_name == 'push' && '33657165406' || inputs.writer_run_id }}",
            "EXPECTED_WRITER_HEAD_SHA: ${{ github.event_name == 'push' && '54dec3c579c9906ea0d2849f1bfcdcbe7a16bf8f' || '' }}",
            "MAX_DEALS: ${{ github.event_name == 'push' && '500' || inputs.max_deals }}",
            "SKIP_REMAINING: ${{ github.event_name == 'push' && '996' || inputs.skip_remaining }}",
            "MODEL_WORKERS: ${{ github.event_name == 'push' && '1' || inputs.model_workers }}",
            "DETERMINISTIC_ONLY: ${{ github.event_name == 'push' && 'true' || inputs.deterministic_only }}",
            "INCLUDE_CATEGORY_PRESENT: ${{ github.event_name == 'push' && 'false' || inputs.include_category_present }}",
            '"33657165406:54dec3c579c9906ea0d2849f1bfcdcbe7a16bf8f:500:996:1:true:false"',
            '--max-deals "${MAX_DEALS}"',
            '--skip-remaining "${SKIP_REMAINING}"',
        ):
            self.assertIn(expected, self.workflow)
        self.assertEqual(
            self.workflow.count(
                "OPENAI_API_KEY: ${{ github.event_name == 'workflow_dispatch' && "
                "!inputs.deterministic_only && secrets.OPENAI_API_KEY || '' }}"
            ),
            2,
        )

    def test_push_gate_rejects_replay_deletion_and_non_add_events(self):
        gate = self.workflow.split(
            "      - name: Fail closed unless this is an exact authorized invocation\n",
            1,
        )[1].split(
            "\n      - name: Require main dispatch and terminal current writer plan",
            1,
        )[0]
        for required in (
            '[ "${GITHUB_RUN_ATTEMPT}" != "1" ]',
            'event.get("deleted") is not False',
            'event.get("forced") is not False',
            'if "inputs" in event:',
            'if before == "0" * 40:',
            'if after != expected_after:',
            'git merge-base --is-ancestor "${before}" "${GITHUB_SHA}"',
            'git cat-file -e "${before}:${ONE_SHOT_MARKER_PATH}"',
            'marker_change="$(git diff --name-status --no-renames',
            'if [ "${marker_change}" != $\'A\\t\'"${ONE_SHOT_MARKER_PATH}" ]; then',
            '[ -L "${ONE_SHOT_MARKER_PATH}" ]',
            'if [ "${marker_sha256}" != "${ONE_SHOT_MARKER_SHA256}" ]; then',
        ):
            self.assertIn(required, gate)
        self.assertIn("One-shot marker already existed before this push", gate)
        self.assertIn("One-shot marker was not newly added by this push", gate)

    def test_terminal_gate_and_publisher_stay_bound_to_2026(self):
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
            'exact.get("path") != ".github/workflows/precision-backfill-2026.yml"',
            'exact.get("head_sha") != expected_head',
            'if tag_target != expected_head:',
            'if [ "${tag_row_count}" != "1" ]; then',
            '[ "${tag_ref}" != "refs/tags/${completion_marker}" ]',
            'git merge-base --is-ancestor "${tag_target_sha}" "${GITHUB_SHA}"',
        ):
            self.assertIn(expected, terminal)
        self.assertIn("group: bitrix24-category-backfill-global-hosted", self.workflow)
        self.assertIn("--year 2026", self.workflow)
        self.assertIn("--parent-map classifier/data/precision-2026-parent-map.json", self.workflow)
        self.assertIn('PRECISION_YEAR: "2026"', self.workflow)
        self.assertIn(
            "git add --intent-to-add -- \\\n"
            "            classifier/data/precision-2026.allowlist \\\n"
            "            classifier/data/precision-2026-taxonomy.json",
            self.workflow,
        )
        self.assertNotIn("resolve_terminal_writer", self.workflow)
        self.assertNotIn("precision-backfill-2025.yml", self.workflow)
        self.assertNotIn("precision-2025.", self.workflow)

    def test_scan_audit_has_one_bounded_retry_and_aggregate_only_diagnostics(self):
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

    def test_writer_trigger_surface_remains_narrow_and_shared_locked(self):
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
