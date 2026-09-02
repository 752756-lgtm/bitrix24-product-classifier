import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/prepare-activity-plan-2026.yml"
MARKER_RELATIVE = Path(
    ".github/workflow-triggers/"
    "prepare-activity-plan-2026-bounded-500-skip-0-deterministic-retry-5.trigger"
)
MARKER_PATH = ROOT / MARKER_RELATIVE
MARKER_SHA256 = "896f50df4426504a893c791fee4350f6e1d22911dbc3dc944fb29a8f110308e1"


class PrepareActivityBoundedOneShotWorkflowTests(unittest.TestCase):
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
        self.assertNotIn("cron:", self.workflow)

    def test_marker_payload_and_hash_are_byte_exact(self):
        self.assertEqual(
            MARKER_PATH.read_bytes(),
            b"prepare-activity-plan-2026\n"
            b"mode=auto-resolve-terminal-writer\n"
            b"max_deals=500\n"
            b"skip_remaining=0\n"
            b"model_workers=1\n"
            b"deterministic_only=true\n"
            b"include_category_present=false\n"
            b"one_shot=true\n"
            b"retry=5\n",
        )
        self.assertEqual(hashlib.sha256(MARKER_PATH.read_bytes()).hexdigest(), MARKER_SHA256)
        self.assertIn(f"ONE_SHOT_MARKER_SHA256: {MARKER_SHA256}", self.workflow)

    def test_push_has_fixed_bounded_inputs_and_manual_inputs_are_preserved(self):
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
            "EXPECTED_WRITER_RUN_ID: ${{ github.event_name == 'workflow_dispatch' && inputs.writer_run_id || '' }}",
            "MAX_DEALS: ${{ github.event_name == 'push' && '500' || inputs.max_deals }}",
            "SKIP_REMAINING: ${{ github.event_name == 'push' && '0' || inputs.skip_remaining }}",
            "MODEL_WORKERS: ${{ github.event_name == 'push' && '1' || inputs.model_workers }}",
            "DETERMINISTIC_ONLY: ${{ github.event_name == 'push' && 'true' || inputs.deterministic_only }}",
            "INCLUDE_CATEGORY_PRESENT: ${{ github.event_name == 'push' && 'false' || inputs.include_category_present }}",
            '"500:0:1:true:false"',
            '--model-workers "${MODEL_WORKERS}"',
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
        )[1].split("\n      - name: Require terminal current writer plan", 1)[0]
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
        # If an old workflow definition were evaluated for cleanup, the marker
        # exists in `before` and its deletion is not an A-only change.
        self.assertIn("One-shot marker already existed before this push", gate)
        self.assertIn("One-shot marker was not newly added by this push", gate)

    def test_auto_resolution_is_bound_to_current_fingerprint_tag_and_run_sha(self):
        gate = self.workflow.split(
            "      - name: Require terminal current writer plan\n", 1
        )[1].split("\n      - name: Validate inputs and required secrets", 1)[0]
        for fingerprint_path in (
            "classifier/precision_worker.py",
            "classifier/precision_plan.py",
            "classifier/bitrix.py",
            "classifier/http.py",
            "classifier/data/precision-2025.allowlist",
            "classifier/data/precision-2025-taxonomy.json",
        ):
            self.assertIn(f"sha256sum {fingerprint_path}", gate)
        for required in (
            'completion_marker="precision-2025-complete-${fingerprint}"',
            '"${api}/git/ref/tags/${completion_marker}"',
            "actions/workflows/precision-backfill-2025.yml/runs?branch=main&status=success&exclude_pull_requests=true&per_page=100",
            "for writer_status in in_progress queued requested waiting pending; do",
            "python -m tools.resolve_terminal_writer",
            '--mode "${INVOCATION_MODE}"',
            '--expected-run-id "${EXPECTED_WRITER_RUN_ID}"',
            '--marker "${completion_marker}"',
            'git merge-base --is-ancestor "${tag_target_sha}" "${GITHUB_SHA}"',
        ):
            self.assertIn(required, gate)

    def test_manual_run_id_still_uses_the_exact_run_endpoint(self):
        gate = self.workflow.split(
            "      - name: Require terminal current writer plan\n", 1
        )[1].split("\n      - name: Validate inputs and required secrets", 1)[0]
        self.assertIn("manual)", gate)
        self.assertIn(
            '"${api}/actions/runs/${EXPECTED_WRITER_RUN_ID}" > "${exact}"',
            gate,
        )
        self.assertIn("--expected-run-id", gate)

    def test_2026_writer_is_dormant_until_signed_assets_are_published(self):
        writer = (
            ROOT / ".github/workflows/precision-backfill-2026.yml"
        ).read_text(encoding="utf-8")
        trigger_header = writer.split("\npermissions:\n", 1)[0]
        self.assertIn("group: bitrix24-category-backfill-global-hosted", writer)
        self.assertIn('"classifier/data/precision-2026.allowlist"', trigger_header)
        self.assertIn('"classifier/data/precision-2026-taxonomy.json"', trigger_header)
        self.assertNotIn('"classifier/precision_worker.py"', trigger_header)
        self.assertNotIn('"classifier/precision_plan.py"', trigger_header)
        self.assertNotIn('"classifier/bitrix.py"', trigger_header)
        self.assertNotIn('"classifier/http.py"', trigger_header)
        self.assertNotIn('".github/workflows/precision-backfill-2026.yml"', trigger_header)
        self.assertNotIn("schedule:", trigger_header)
        self.assertNotIn("cron:", trigger_header)
        self.assertIn(
            "PRECISION_ALLOWLIST_PATH: classifier/data/precision-2026.allowlist",
            writer,
        )
        self.assertIn(
            "PRECISION_TAXONOMY_PATH: classifier/data/precision-2026-taxonomy.json",
            writer,
        )

    def test_2026_parent_map_is_bound_to_2026(self):
        parent_map = json.loads(
            (ROOT / "classifier/data/precision-2026-parent-map.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(parent_map["year"], 2026)


    def test_publisher_marks_new_2026_assets_for_diff(self):
        self.assertIn(
            "git add --intent-to-add -- \\\n"
            "            classifier/data/precision-2026.allowlist \\\n"
            "            classifier/data/precision-2026-taxonomy.json",
            self.workflow,
        )


if __name__ == "__main__":
    unittest.main()
