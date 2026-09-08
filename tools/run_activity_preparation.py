"""Run activity preparation with private logs and allowlisted progress only."""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classifier.activity_prepare import require_private_path
from tools.model_pilot_2026 import public_summary


def read_summary(path):
    mode = path.lstat().st_mode
    if not stat.S_ISREG(mode) or mode & 0o077:
        raise ValueError("Invalid private diagnostic")
    with path.open(encoding="utf-8") as stream:
        content = stream.read(65_537)
    if len(content) > 65_536:
        raise ValueError("Oversized diagnostic")
    return public_summary(json.loads(content), allow_running=True)


def emit(summary):
    print(json.dumps(summary, sort_keys=True, ensure_ascii=True), flush=True)


def unavailable():
    # Fixed literals: exceptions, paths and child output must stay private.
    emit({"outcome": "failure", "stage": "bootstrap", "code": "unexpected_error", "stats": {}})


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    child = None
    os.umask(0o077)
    try:
        parser = argparse.ArgumentParser(add_help=False, exit_on_error=False, allow_abbrev=False)
        parser.add_argument("--diagnostics", required=True)
        # argparse's normal error method prints argv; suppress it entirely.
        def invalid_arguments(message):
            raise ValueError("Invalid wrapper arguments")
        parser.error = invalid_arguments
        parsed, _ = parser.parse_known_args(args)
        diagnostic = Path(parsed.diagnostics)
        private_log = Path(os.environ["PRIVATE_LOG"])
        require_private_path(diagnostic)
        require_private_path(private_log)
        if diagnostic.resolve() == private_log.resolve():
            raise ValueError("Private paths collide")
        # Exclusive creation prevents overwriting private files or following a symlink.
        with private_log.open("x", encoding="utf-8") as stream:
            child = subprocess.Popen(
                [sys.executable, "-m", "classifier.activity_prepare", *args],
                stdout=stream, stderr=subprocess.STDOUT,
            )
            while True:
                try:
                    returncode = child.wait(timeout=60)
                    break
                except subprocess.TimeoutExpired:
                    safe = read_summary(diagnostic)
                    # Terminal output is withheld until the process actually exits.
                    if safe["outcome"] == "running":
                        emit(safe)
        safe = read_summary(diagnostic)
        if returncode != 0:
            if safe["outcome"] == "failure":
                emit(safe)
            else:
                unavailable()
            return returncode if returncode > 0 else 128 - returncode
        if safe["outcome"] != "success" or safe["stage"] != "complete":
            raise ValueError("Missing successful terminal diagnostic")
        emit(safe)
        return 0
    except Exception:
        try:
            if child is not None and child.poll() is None:
                child.kill()
                child.wait()
        except Exception:
            # Cleanup errors must not reveal paths or subprocess arguments.
            unavailable()
            return 1
        unavailable()
        # Preserve a completed subprocess failure even when diagnostics are bad.
        if child is not None and child.returncode:
            return child.returncode if child.returncode > 0 else 128 - child.returncode
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
