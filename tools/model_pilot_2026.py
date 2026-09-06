"""Bounded read-only model pilot; only validated counters leave runner temp."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classifier.activity_prepare import (
    DIAGNOSTIC_FAILURE_CODES, DIAGNOSTIC_FORMAT, DIAGNOSTIC_STAGES,
    PreparationStats,
)


def public_summary(payload):
    if not isinstance(payload, dict) or payload.get("format") != DIAGNOSTIC_FORMAT:
        raise ValueError("Invalid diagnostic")
    stage, code = payload.get("failure_stage"), payload.get("failure_code")
    outcome = payload.get("outcome")
    if stage not in DIAGNOSTIC_STAGES or code not in DIAGNOSTIC_FAILURE_CODES:
        raise ValueError("Invalid diagnostic enum")
    if outcome not in ("success", "failure"):
        raise ValueError("Invalid outcome")
    stats = payload.get("stats")
    if not isinstance(stats, dict):
        raise ValueError("Invalid counters")
    safe = {}
    for key in PreparationStats().as_dict():
        value = stats.get(key)
        if type(value) is not int or value < 0:
            raise ValueError("Invalid counter")
        safe[key] = value
    return {"outcome": outcome, "stage": stage, "code": code, "stats": safe}


def main():
    os.umask(0o077)
    # Fixed scope. No arbitrary command, offset, model or output upload inputs.
    with tempfile.TemporaryDirectory(prefix="model-pilot-2026-", dir=os.environ["RUNNER_TEMP"]) as tmp:
        root = Path(tmp)
        args = [sys.executable, "-m", "classifier.activity_prepare",
                "--year", "2026", "--max-deals", "30", "--skip-remaining", "0",
                "--model-workers", "1", "--api-call-cap", "3000",
                "--parent-map", "classifier/data/precision-2026-parent-map.json"]
        for flag in ("plan", "products", "checkpoint", "status", "diagnostics"):
            args.extend(["--" + flag, str(root / (flag + ".json"))])
        with (root / "private.log").open("w") as stream:
            result = subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, check=False)
        try:
            safe = public_summary(json.loads((root / "diagnostics.json").read_text()))
        except (ValueError, OSError, TypeError):
            safe = {"outcome": "failure", "stage": "diagnostic_unavailable",
                    "code": "diagnostic_unavailable", "stats": {}}
        output = json.dumps(safe, sort_keys=True, ensure_ascii=True)
        print(output)
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write("\n2026 read-only model pilot (30 deals; no CRM writes)\n\n```json\n" + output + "\n```\n")
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
