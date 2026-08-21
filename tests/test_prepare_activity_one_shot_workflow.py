import hashlib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/prepare-activity-plan-2025.yml"
MARKER_RELATIVE = Path(
    ".github/workflow-triggers/"
    "prepare-activity-plan-2025-32509765967.trigger"
)
MARKER_PATH = ROOT / MARKER_RELATIVE
MARKER_SHA256 = "d1e73e8db0b1166ced9ed72d9cd13700112babfa533380c13b676cea3c35f20f"


class PrepareActivityOneShotWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_push_trigger_is_limited_to_main_and_the_exact_marker_path(self):
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

    def test_push_inputs_are_fixed_while_dispatch_inputs_remain_typed(self):
        dispatch = self.workflow.split("  workflow_dispatch:\n", 1)[1].split(
            "  # Temporary one-shot bridge", 1
        )[0]
        for name, kind in (
            ("writer_run_id", "string"),
            ("max_deals", "string"),
            ("deterministic_only", "boolean"),
            ("include_category_present", "boolean"),
        ):
            self.assertIn(f"      {name}:\n", dispatch)
            self.assertIn(f"        type: {kind}\n", dispatch)
            self.assertIn("        required: true\n", dispatch)

        expected_effective_values = (
            "EXPECTED_WRITER_RUN_ID: ${{ github.event_name == 'push' && "
            "'32509765967' || inputs.writer_run_id }}",
            "MAX_DEALS: ${{ github.event_name == 'push' && '0' || "
            "inputs.max_deals }}",
            "DETERMINISTIC_ONLY: ${{ github.event_name == 'push' && "
            "'false' || inputs.deterministic_only }}",
            "INCLUDE_CATEGORY_PRESENT: ${{ github.event_name == 'push' && "
            "'false' || inputs.include_category_present }}",
        )
        for expected in expected_effective_values:
            self.assertIn(expected, self.workflow)
        self.assertIn(
            '"32509765967:0:false:false"',
            self.workflow,
        )

    def test_push_gate_rejects_repeat_stale_or_non_add_marker_events(self):
        self.assertIn(
            "if: >-\n"
            "      github.event_name == 'workflow_dispatch' ||\n"
            "      github.event_name == 'push'",
            self.workflow,
        )
        gate = self.workflow.split(
            "      - name: Fail closed unless this is an exact authorized invocation\n",
            1,
        )[1].split("\n      - name: Require main dispatch", 1)[0]
        for required in (
            '[ "${GITHUB_REF}" != "refs/heads/main" ]',
            '[ "$(git rev-parse HEAD)" != "${GITHUB_SHA}" ]',
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
            'echo "::error::Unsupported workflow event"',
        ):
            self.assertIn(required, gate)

    def test_marker_payload_is_byte_exact_and_hash_pinned(self):
        marker = MARKER_PATH.read_bytes()
        self.assertEqual(
            marker,
            b"prepare-activity-plan-2025\n"
            b"writer_run_id=32509765967\n"
            b"max_deals=0\n"
            b"deterministic_only=false\n"
            b"include_category_present=false\n"
            b"one_shot=true\n",
        )
        self.assertEqual(hashlib.sha256(marker).hexdigest(), MARKER_SHA256)
        self.assertIn(
            f"ONE_SHOT_MARKER_SHA256: {MARKER_SHA256}",
            self.workflow,
        )

    def test_cleanup_cannot_repeat_the_expensive_push_run(self):
        self.assertIn(
            "Cleanup removes this stanza and the marker in the\n"
            "  # same commit; even an old-workflow evaluation rejects the marker deletion.",
            self.workflow,
        )
        self.assertIn(
            'if git cat-file -e "${before}:${ONE_SHOT_MARKER_PATH}"',
            self.workflow,
        )
        self.assertIn(
            'if [ "${marker_change}" != $\'A\\t\'"${ONE_SHOT_MARKER_PATH}" ]; then',
            self.workflow,
        )

    def test_writer_concurrency_and_terminal_gates_are_preserved(self):
        for required in (
            "group: bitrix24-category-backfill-2025-hosted",
            "cancel-in-progress: false",
            'completion_marker="precision-2025-complete-${fingerprint}"',
            'exact.get("status") != "completed"',
            'exact.get("conclusion") != "success"',
            'exact.get("head_branch") != "main"',
            'exact.get("path") != ".github/workflows/precision-backfill-2025.yml"',
            "if active:",
            'if [ "$(git rev-parse origin/main)" != "${GITHUB_SHA}" ]; then',
        ):
            self.assertIn(required, self.workflow)


if __name__ == "__main__":
    unittest.main()
