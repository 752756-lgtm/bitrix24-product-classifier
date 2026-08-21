from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


WORKFLOW_PATH = ".github/workflows/precision-backfill-2025.yml"
ACTIVE_STATUSES = frozenset(
    {"in_progress", "queued", "requested", "waiting", "pending"}
)
SHA_RE = re.compile(r"[0-9a-f]{40}")


class ResolutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class WriterResolution:
    run_id: int
    tag_target_sha: str


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ResolutionError("GitHub metadata is unavailable or malformed") from exc


def _terminal_main_run(row: object) -> bool:
    return (
        isinstance(row, dict)
        and type(row.get("id")) is int
        and row["id"] > 0
        and row.get("status") == "completed"
        and row.get("conclusion") == "success"
        and row.get("head_branch") == "main"
        and row.get("path") == WORKFLOW_PATH
    )


def _created_at(row: dict[str, Any]) -> datetime:
    raw = row.get("created_at")
    if not isinstance(raw, str) or not raw.endswith("Z"):
        raise ResolutionError("Writer candidate timestamp is malformed")
    try:
        value = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise ResolutionError("Writer candidate timestamp is malformed") from exc
    if value.tzinfo is None:
        raise ResolutionError("Writer candidate timestamp is malformed")
    return value


def resolve_terminal_writer(
    *,
    mode: str,
    expected_run_id: str,
    marker: str,
    tag_payload: object,
    exact_payload: object | None,
    candidates_payload: object | None,
    active_payloads: list[object],
) -> WriterResolution:
    if not re.fullmatch(r"precision-2025-complete-[0-9a-f]{64}", marker):
        raise ResolutionError("Completion marker name is malformed")
    if not isinstance(tag_payload, dict):
        raise ResolutionError("Completion marker response is malformed")
    target = tag_payload.get("object")
    if not isinstance(target, dict):
        raise ResolutionError("Completion marker target is malformed")
    target_sha = target.get("sha")
    if (
        tag_payload.get("ref") != f"refs/tags/{marker}"
        or target.get("type") != "commit"
        or not isinstance(target_sha, str)
        or SHA_RE.fullmatch(target_sha) is None
    ):
        raise ResolutionError("Completion marker target is malformed")

    if len(active_payloads) != len(ACTIVE_STATUSES):
        raise ResolutionError("Active writer checks are incomplete")
    for payload in active_payloads:
        runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
        if not isinstance(runs, list) or any(not isinstance(row, dict) for row in runs):
            raise ResolutionError("Active writer response is malformed")
        total_count = payload.get("total_count")
        if type(total_count) is not int or total_count < len(runs):
            raise ResolutionError("Active writer response is malformed")
        for row in runs:
            status = row.get("status")
            if (
                row.get("path") != WORKFLOW_PATH
                or row.get("head_branch") != "main"
                or status not in ACTIVE_STATUSES
            ):
                raise ResolutionError("Active writer identity is malformed")
        if runs or total_count > 0:
            raise ResolutionError("A precision writer is still active")
    # Empty filtered responses do not carry their requested status. The caller
    # supplies exactly one response for every member of ACTIVE_STATUSES.

    if mode == "manual":
        if not expected_run_id.isdigit() or int(expected_run_id) <= 0:
            raise ResolutionError("Manual writer run ID is malformed")
        if not _terminal_main_run(exact_payload):
            raise ResolutionError("Manual writer run is not terminal-safe")
        assert isinstance(exact_payload, dict)
        if exact_payload["id"] != int(expected_run_id):
            raise ResolutionError("Manual writer run identity does not match")
        selected = exact_payload
    elif mode == "one_shot":
        if expected_run_id:
            raise ResolutionError("One-shot mode cannot accept a writer run ID")
        rows = (
            candidates_payload.get("workflow_runs")
            if isinstance(candidates_payload, dict)
            else None
        )
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ResolutionError("Writer candidate response is malformed")
        total_count = candidates_payload.get("total_count")
        if type(total_count) is not int or total_count < len(rows):
            raise ResolutionError("Writer candidate response is malformed")
        if any(not _terminal_main_run(row) for row in rows):
            raise ResolutionError("Writer candidate identity is malformed")
        if any(
            not isinstance(row.get("head_sha"), str)
            or SHA_RE.fullmatch(row["head_sha"]) is None
            for row in rows
        ):
            raise ResolutionError("Writer candidate head SHA is malformed")
        eligible = [row for row in rows if row.get("head_sha") == target_sha]
        if not eligible:
            raise ResolutionError(
                "No successful writer run matches the completion marker"
            )
        selected = max(
            eligible,
            key=lambda row: (_created_at(row), int(row["id"])),
        )
    else:
        raise ResolutionError("Unsupported writer resolution mode")

    return WriterResolution(int(selected["id"]), target_sha)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve a terminal writer from non-sensitive GitHub metadata"
    )
    parser.add_argument("--mode", choices=("manual", "one_shot"), required=True)
    parser.add_argument("--expected-run-id", default="")
    parser.add_argument("--marker", required=True)
    parser.add_argument("--tag-json", type=Path, required=True)
    parser.add_argument("--exact-json", type=Path, required=True)
    parser.add_argument("--candidates-json", type=Path, required=True)
    parser.add_argument("--active-json", type=Path, action="append", required=True)
    args = parser.parse_args()

    try:
        resolution = resolve_terminal_writer(
            mode=args.mode,
            expected_run_id=args.expected_run_id,
            marker=args.marker,
            tag_payload=_read_json(args.tag_json),
            exact_payload=(
                _read_json(args.exact_json) if args.mode == "manual" else None
            ),
            candidates_payload=(
                _read_json(args.candidates_json)
                if args.mode == "one_shot"
                else None
            ),
            active_payloads=[_read_json(path) for path in args.active_json],
        )
    except ResolutionError:
        raise SystemExit("Terminal writer resolution failed") from None
    print(f"{resolution.run_id}\t{resolution.tag_target_sha}")


if __name__ == "__main__":
    main()
