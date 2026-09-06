import unittest
from pathlib import Path
from tools.model_pilot_2026 import public_summary
from classifier.activity_prepare import DIAGNOSTIC_FORMAT, PreparationStats


class ModelPilotTests(unittest.TestCase):
    def payload(self):
        return dict(format=DIAGNOSTIC_FORMAT, outcome='success', failure_stage='complete',
                    failure_code='none', stats=PreparationStats().as_dict())

    def test_only_known_counters_leave_runner(self):
        p = self.payload()
        p['last_error'] = 'private body'
        p['stats']['client_text'] = 'private body'
        self.assertNotIn('private body', str(public_summary(p)))

    def test_invalid_counters_are_rejected(self):
        for value in (-1, True, '123', None):
            p = self.payload()
            p['stats']['remaining'] = value
            with self.assertRaises(ValueError):
                public_summary(p)

    def test_unknown_failure_is_not_printed(self):
        p = self.payload()
        p['failure_code'] = 'private body'
        with self.assertRaises(ValueError):
            public_summary(p)

    def test_workflow_is_read_only_and_bounded(self):
        root = Path(__file__).resolve().parents[1]
        w = (root / '.github/workflows/synthetic-canary-2026.yml').read_text()
        self.assertNotIn('BITRIX_WEBHOOK_URL', w)
        self.assertFalse((root / '.github/workflows/model-pilot-2026.yml').exists())
        s = (root / 'tools/model_pilot_2026.py').read_text()
        for forbidden in ('contents: write', 'pull-requests: write', 'upload-artifact', '--apply'):
            self.assertNotIn(forbidden, w + s)
        self.assertIn('bitrix24-category-backfill-global-hosted', w)
        self.assertIn('test "$GITHUB_RUN_ATTEMPT" = 1', w)
        self.assertIn('33979634087', w)
        self.assertIn('"--max-deals", "30"', s)
        self.assertIn('"--model-workers", "1"', s)
        self.assertNotIn('--deterministic-only', s)
