import unittest

from tools.resolve_terminal_writer import (
    ACTIVE_STATUSES,
    WORKFLOW_PATH,
    ResolutionError,
    resolve_terminal_writer,
)


MARKER = "precision-2025-complete-" + "a" * 64
TAG_TARGET = "b" * 40


def tag_payload(*, marker=MARKER, sha=TAG_TARGET, object_type="commit"):
    return {
        "ref": f"refs/tags/{marker}",
        "object": {"type": object_type, "sha": sha},
    }


def writer_run(
    run_id,
    *,
    head_sha=TAG_TARGET,
    created_at="2026-08-21T20:00:00Z",
    status="completed",
    conclusion="success",
    head_branch="main",
    path=WORKFLOW_PATH,
):
    return {
        "id": run_id,
        "status": status,
        "conclusion": conclusion,
        "head_branch": head_branch,
        "head_sha": head_sha,
        "path": path,
        "created_at": created_at,
    }


def inactive_payloads():
    return [
        {"total_count": 0, "workflow_runs": []}
        for _status in ACTIVE_STATUSES
    ]


def candidates_payload(rows):
    return {"total_count": len(rows), "workflow_runs": rows}


class TerminalWriterResolutionTests(unittest.TestCase):
    def test_one_shot_selects_latest_success_for_exact_tag_target(self):
        resolution = resolve_terminal_writer(
            mode="one_shot",
            expected_run_id="",
            marker=MARKER,
            tag_payload=tag_payload(),
            exact_payload=None,
            candidates_payload=candidates_payload(
                [
                    writer_run(
                        101,
                        created_at="2026-08-21T19:00:00Z",
                    ),
                    writer_run(
                        102,
                        created_at="2026-08-21T21:00:00Z",
                    ),
                    writer_run(
                        999,
                        head_sha="c" * 40,
                        created_at="2026-08-21T22:00:00Z",
                    ),
                ]
            ),
            active_payloads=inactive_payloads(),
        )
        self.assertEqual(resolution.run_id, 102)
        self.assertEqual(resolution.tag_target_sha, TAG_TARGET)

    def test_one_shot_requires_exact_terminal_main_workflow_identity(self):
        mutations = (
            {"status": "in_progress"},
            {"conclusion": "failure"},
            {"head_branch": "feature"},
            {"path": ".github/workflows/other.yml"},
            {"id": True},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                row = writer_run(101)
                row.update(mutation)
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id="",
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=None,
                        candidates_payload=candidates_payload([row]),
                        active_payloads=inactive_payloads(),
                    )

    def test_one_shot_rejects_missing_match_malformed_time_or_explicit_id(self):
        cases = (
            {
                "expected_run_id": "123",
                "rows": [writer_run(101)],
            },
            {
                "expected_run_id": "",
                "rows": [writer_run(101, head_sha="c" * 40)],
            },
            {
                "expected_run_id": "",
                "rows": [writer_run(101, created_at="not-a-time")],
            },
        )
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id=case["expected_run_id"],
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=None,
                        candidates_payload=candidates_payload(case["rows"]),
                        active_payloads=inactive_payloads(),
                    )

    def test_manual_keeps_numeric_exact_run_behavior(self):
        exact = writer_run(123, head_sha="c" * 40)
        resolution = resolve_terminal_writer(
            mode="manual",
            expected_run_id="123",
            marker=MARKER,
            tag_payload=tag_payload(),
            exact_payload=exact,
            candidates_payload=None,
            active_payloads=inactive_payloads(),
        )
        self.assertEqual(resolution.run_id, 123)
        self.assertEqual(resolution.tag_target_sha, TAG_TARGET)

        for invalid in ("", "0", "abc", "124"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="manual",
                        expected_run_id=invalid,
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=exact,
                        candidates_payload=None,
                        active_payloads=inactive_payloads(),
                    )

    def test_any_active_writer_status_fails_closed(self):
        for index, status in enumerate(sorted(ACTIVE_STATUSES)):
            payloads = inactive_payloads()
            payloads[index] = {
                "total_count": 1,
                "workflow_runs": [
                    writer_run(
                        200 + index,
                        status=status,
                        conclusion=None,
                    )
                ]
            }
            with self.subTest(status=status):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id="",
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=None,
                        candidates_payload=candidates_payload([writer_run(101)]),
                        active_payloads=payloads,
                    )

    def test_tag_target_and_active_check_set_are_exact(self):
        invalid_tags = (
            tag_payload(marker="precision-2025-complete-" + "c" * 64),
            tag_payload(sha="not-a-sha"),
            tag_payload(object_type="tag"),
            [],
        )
        for value in invalid_tags:
            with self.subTest(value=value):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id="",
                        marker=MARKER,
                        tag_payload=value,
                        exact_payload=None,
                        candidates_payload=candidates_payload([writer_run(101)]),
                        active_payloads=inactive_payloads(),
                    )

        with self.assertRaises(ResolutionError):
            resolve_terminal_writer(
                mode="one_shot",
                expected_run_id="",
                marker=MARKER,
                tag_payload=tag_payload(),
                exact_payload=None,
                candidates_payload=candidates_payload([writer_run(101)]),
                active_payloads=inactive_payloads()[:-1],
            )

        for malformed_active in (
            {"workflow_runs": []},
            {"total_count": True, "workflow_runs": []},
            {"total_count": 1, "workflow_runs": []},
            {
                "total_count": 0,
                "workflow_runs": [
                    writer_run(202, status="queued", conclusion=None)
                ],
            },
        ):
            inconsistent_active = inactive_payloads()
            inconsistent_active[0] = malformed_active
            with self.subTest(malformed_active=malformed_active):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id="",
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=None,
                        candidates_payload=candidates_payload([writer_run(101)]),
                        active_payloads=inconsistent_active,
                    )

        malformed_candidate = writer_run(101)
        malformed_candidate["head_sha"] = "not-a-sha"
        for malformed_candidates in (
            {"workflow_runs": [writer_run(101)]},
            {"total_count": True, "workflow_runs": [writer_run(101)]},
            candidates_payload([malformed_candidate]),
        ):
            with self.subTest(malformed_candidates=malformed_candidates):
                with self.assertRaises(ResolutionError):
                    resolve_terminal_writer(
                        mode="one_shot",
                        expected_run_id="",
                        marker=MARKER,
                        tag_payload=tag_payload(),
                        exact_payload=None,
                        candidates_payload=malformed_candidates,
                        active_payloads=inactive_payloads(),
                    )


if __name__ == "__main__":
    unittest.main()
