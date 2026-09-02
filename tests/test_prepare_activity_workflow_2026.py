import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/prepare-activity-plan-2026.yml"


class PrepareActivityWorkflow2026Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_stable_workflow_is_manual_only_and_bound_to_2026_writer(self):
        trigger_header = self.workflow.split("\npermissions:\n", 1)[0]
        self.assertIn("  workflow_dispatch:\n", trigger_header)
        self.assertNotIn("  push:\n", trigger_header)
        self.assertNotIn("schedule:", trigger_header)
        self.assertNotIn("pull_request:", trigger_header)
        self.assertIn(
            "group: bitrix24-category-backfill-global-hosted",
            self.workflow,
        )
        for expected in (
            "actions/workflows/precision-backfill-2026.yml/runs",
            'exact.get("path") != ".github/workflows/precision-backfill-2026.yml"',
            "classifier/data/precision-2026.allowlist",
            "classifier/data/precision-2026-taxonomy.json",
            "git add --intent-to-add --",
            'completion_marker="precision-2026-complete-${fingerprint}"',
            "--year 2026",
            "--parent-map classifier/data/precision-2026-parent-map.json",
            'PRECISION_YEAR: "2026"',
            'git commit -m "Prepare reviewed activity evidence plan for 2026"',
        ):
            self.assertIn(expected, self.workflow)
        self.assertNotIn("ONE_SHOT_MARKER", self.workflow)
        self.assertNotIn("resolve_terminal_writer", self.workflow)
        self.assertNotIn("precision-backfill-2025.yml", self.workflow)
        self.assertNotIn("precision-2025.", self.workflow)


if __name__ == "__main__":
    unittest.main()
