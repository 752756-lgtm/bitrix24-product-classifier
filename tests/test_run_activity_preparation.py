import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from classifier.activity_prepare import DIAGNOSTIC_FORMAT, PreparationStats
from tools.model_pilot_2026 import public_summary
from tools.run_activity_preparation import main


class PreparationProgressTests(unittest.TestCase):
    def payload(self, outcome="running", stage="model_classification", code="none"):
        return dict(format=DIAGNOSTIC_FORMAT, outcome=outcome, failure_stage=stage,
                    failure_code=code, stats=PreparationStats().as_dict())

    def test_running_requires_opt_in_and_strips_private_values(self):
        payload = self.payload()
        payload["private_text"] = "customer secret"
        payload["stats"]["private_text"] = "customer secret"
        with self.assertRaises(ValueError):
            public_summary(payload)
        safe = public_summary(payload, allow_running=True)
        self.assertEqual(safe["outcome"], "running")
        self.assertNotIn("customer secret", json.dumps(safe))
        for key in ("outcome", "failure_stage", "failure_code"):
            malformed = dict(payload, **{key: "customer secret"})
            with self.assertRaises(ValueError):
                public_summary(malformed, allow_running=True)

    def test_outcome_code_consistency(self):
        for outcome, code in (("running", "model_timeout"), ("success", "model_timeout"), ("failure", "none")):
            with self.assertRaises(ValueError):
                public_summary(self.payload(outcome=outcome, code=code), allow_running=True)

    def run_wrapper(self, terminal_payload, returncode, *, running=False):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            diagnostic, logfile = root / "diagnostic.json", root / "private.log"
            diagnostic.write_text(json.dumps(terminal_payload))
            diagnostic.chmod(0o600)
            args = ["--diagnostics", str(diagnostic), "--max-deals", "200", "--skip-remaining=0"]
            child = Mock()
            child.returncode = returncode
            child.wait.side_effect = ([subprocess.TimeoutExpired("private command", 60), returncode]
                                      if running else [returncode])
            child.poll.return_value = returncode
            output = io.StringIO()
            with patch.dict(os.environ, RUNNER_TEMP=temp, PRIVATE_LOG=str(logfile)), \
                    patch("tools.run_activity_preparation.subprocess.Popen", return_value=child) as popen, \
                    contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                result = main(args)
            self.assertEqual(popen.call_args.args[0], [sys.executable, "-m", "classifier.activity_prepare", *args])
            self.assertEqual(popen.call_args.kwargs["stderr"], subprocess.STDOUT)
            self.assertEqual(popen.call_args.kwargs["stdout"].name, str(logfile))
            self.assertEqual(logfile.stat().st_mode & 0o777, 0o600)
            self.assertEqual(child.wait.call_args.kwargs, {"timeout": 60})
            return result, output.getvalue()

    def test_child_failure_propagates_without_stale_success(self):
        result, output = self.run_wrapper(self.payload("success", "complete"), 7)
        self.assertEqual(result, 7)
        self.assertNotIn('"success"', output)
        self.assertEqual(json.loads(output)["outcome"], "failure")

    def test_zero_exit_requires_complete_success(self):
        result, output = self.run_wrapper(self.payload(), 0, running=True)
        self.assertEqual(result, 1)
        rows = [json.loads(line) for line in output.splitlines()]
        self.assertEqual([row["outcome"] for row in rows], ["running", "failure"])
        result, output = self.run_wrapper(self.payload("success", "complete"), 0, running=True)
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output)["outcome"], "success")

    def test_valid_failure_keeps_code_and_exit_status(self):
        result, output = self.run_wrapper(self.payload("failure", code="model_rate_limited"), 3)
        self.assertEqual(result, 3)
        self.assertEqual(json.loads(output)["code"], "model_rate_limited")

    def test_invalid_private_diagnostic_never_escapes_or_masks_child_failure(self):
        payload = self.payload("failure", code="customer secret")
        result, output = self.run_wrapper(payload, 9)
        self.assertEqual(result, 9)
        self.assertNotIn("customer secret", output)
        self.assertEqual(json.loads(output)["code"], "unexpected_error")

    def test_invalid_arguments_are_not_echoed(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output), \
                patch("tools.run_activity_preparation.subprocess.Popen") as popen:
            self.assertEqual(main(["--private-customer", "customer secret"]), 1)
        popen.assert_not_called()
        self.assertNotIn("customer secret", output.getvalue())
