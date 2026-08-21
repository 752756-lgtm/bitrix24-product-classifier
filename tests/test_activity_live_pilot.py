from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from tools.activity_live_pilot import (
    PilotError,
    ReadOnlyRateLimitedClient,
    _assert_aggregate_report,
    _hydrate_activity_bindings,
    _private_write,
    _validate_operational_limits,
    list_activities_batched,
    run_pilot,
)


PRIVATE_SENTINEL = "PRIVATE-ACTIVITY-EVIDENCE-SENTINEL"


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


def pilot_client(transport: object) -> ReadOnlyRateLimitedClient:
    clock = Clock()
    return ReadOnlyRateLimitedClient(
        transport,
        1.2,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


def activity_row(deal_id: int, kind: str) -> dict[str, object]:
    activity_id = deal_id * 10 + (1 if kind == "email" else 2)
    return {
        "ID": str(activity_id),
        "OWNER_ID": "777",
        "OWNER_TYPE_ID": "3",
        "TYPE_ID": "4" if kind == "email" else "2",
        "DIRECTION": "1",
        "PROVIDER_ID": "CRM_EMAIL" if kind == "email" else "VOXIMPLANT_CALL",
        "PROVIDER_TYPE_ID": "EMAIL" if kind == "email" else "CALL",
        "SUBJECT": f"{PRIVATE_SENTINEL}-{kind}-{deal_id}",
        "DESCRIPTION_TYPE": "3",
        "CREATED": "2025-04-01T10:00:00+03:00",
        "LAST_UPDATED": "2025-04-01T10:01:00+03:00",
        "START_TIME": "2025-04-01T10:00:00+03:00",
        "END_TIME": "2025-04-01T10:01:00+03:00",
        "COMPLETED": "Y",
    }


def activity_bindings(deal_id: int) -> list[dict[str, int]]:
    return [
        {"entityTypeId": 2, "entityId": deal_id},
        {"entityTypeId": 3, "entityId": 777},
    ]


class FakeTransport:
    def __init__(self):
        self.deal_ids = list(range(31001, 31021))
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.by_activity_id: dict[str, tuple[int, str, dict[str, object]]] = {}
        for deal_id in self.deal_ids:
            for kind in ("email", "call"):
                row = activity_row(deal_id, kind)
                self.by_activity_id[str(row["ID"])] = (deal_id, kind, row)

    def _activity_detail(self, activity_id: str) -> dict[str, object]:
        _deal_id, kind, row = self.by_activity_id[activity_id]
        detail = dict(row)
        detail["DESCRIPTION"] = (
            f"{PRIVATE_SENTINEL}-email-body" if kind == "email" else ""
        )
        return detail

    def _transcript(self, activity_id: str) -> dict[str, str]:
        if activity_id not in self.by_activity_id:
            raise AssertionError("unknown activity")
        return {"transcription": f"{PRIVATE_SENTINEL}-call-text"}

    def _batch_command(self, command: str):
        parsed = urlsplit(command)
        params = parse_qs(parsed.query)
        if parsed.path == "crm.activity.list":
            deal_id = int(params["filter[BINDINGS][0][OWNER_ID]"][0])
            return [activity_row(deal_id, "email"), activity_row(deal_id, "call")]
        if parsed.path == "crm.activity.binding.list":
            activity_id = params["activityId"][0]
            deal_id, _kind, _row = self.by_activity_id[activity_id]
            start = int(params["start"][0])
            return activity_bindings(deal_id)[start:start + 50]
        if parsed.path == "crm.activity.get":
            return self._activity_detail(params["id"][0])
        if parsed.path == "crm.activity.call.getTranscript":
            return self._transcript(params["activityId"][0])
        raise AssertionError(f"unexpected batch method: {parsed.path}")

    def call(self, method: str, params: dict[str, object]):
        self.calls.append((method, params))
        if method == "crm.category.list":
            return {"categories": []}
        if method == "crm.status.list":
            return [
                {"NAME": "Дубль", "STATUS_ID": "DUPLICATE"},
                {"NAME": "Реклама / Спам", "STATUS_ID": "SPAM"},
                {"NAME": "Поставщик", "STATUS_ID": "SUPPLIER"},
                {"NAME": "Документы / Акты", "STATUS_ID": "DOCS"},
                {"NAME": "Доставка", "STATUS_ID": "DELIVERY"},
            ]
        if method == "crm.deal.list":
            return [
                {
                    "ID": str(deal_id),
                    "STAGE_ID": "NEW",
                    "UF_CRM_1776320088319": "",
                    "UF_CRM_1568228872884": "",
                }
                for deal_id in self.deal_ids
            ]
        if method == "crm.activity.list":
            deal_id = int(params["filter"]["BINDINGS"][0]["OWNER_ID"])
            return [activity_row(deal_id, "email"), activity_row(deal_id, "call")]
        if method == "crm.activity.get":
            return self._activity_detail(str(params["id"]))
        if method == "crm.activity.binding.list":
            activity_id = str(params["activityId"])
            deal_id, _kind, _row = self.by_activity_id[activity_id]
            start = int(params["start"])
            return activity_bindings(deal_id)[start:start + 50]
        if method == "crm.activity.call.getTranscript":
            return self._transcript(str(params["activityId"]))
        if method == "batch":
            return {
                "result": {
                    name: self._batch_command(command)
                    for name, command in params["cmd"].items()
                },
                "result_error": {},
            }
        raise AssertionError(f"unexpected method: {method}")


class ActivityLivePilotTests(unittest.TestCase):
    def test_read_only_client_rejects_direct_and_batched_mutations(self):
        transport = FakeTransport()
        client = ReadOnlyRateLimitedClient(transport, 0)
        with self.assertRaisesRegex(PilotError, "Non-read-only"):
            client.call("crm.deal.update", {"id": 1, "fields": {}})
        with self.assertRaisesRegex(PilotError, "Non-read-only"):
            client.call(
                "batch",
                {"cmd": {"write": "crm.deal.update?id=1&fields[TITLE]=bad"}},
            )
        self.assertEqual(transport.calls, [])

    def test_full_pilot_persists_only_aggregate_shapes(self):
        transport = FakeTransport()
        client = pilot_client(transport)
        report = run_pilot(
            client,
            year=2025,
            sample_size=20,
            max_discovery_pages=5,
            activity_batch_size=20,
            max_activity_pages=5,
            direct_compare_count=3,
            max_emails=10,
            max_calls=10,
        )
        serialized = json.dumps(report, ensure_ascii=False, sort_keys=True)
        self.assertNotIn(PRIVATE_SENTINEL, serialized)
        self.assertNotIn(str(transport.deal_ids[0]), serialized)
        first_activity_id = str(activity_row(transport.deal_ids[0], "email")["ID"])
        self.assertNotIn(first_activity_id, serialized)
        self.assertTrue(report["safety"]["read_only"])
        self.assertFalse(report["safety"]["raw_text_persisted"])
        self.assertTrue(report["verdict"]["ready_for_limited_private_activity_plan"])
        self.assertEqual(report["activity_list"]["incoming_email_rows"], 20)
        self.assertEqual(report["activity_list"]["call_rows"], 20)
        self.assertEqual(report["activity_get"]["email_body_nonempty"], 10)
        self.assertEqual(report["call_text_probe"]["nonempty_call_texts"], 10)
        self.assertEqual(report["activity_list"]["rows_found_only_through_binding"], 40)
        self.assertFalse(
            any(method == "crm.deal.update" for method, _params in transport.calls)
        )

    def test_private_report_is_atomic_mode_0600_and_limited_to_tmp(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            output = Path(directory) / "report.json"
            with patch(
                "tools.activity_live_pilot.os.replace", wraps=os.replace
            ) as replace:
                _private_write(output, {"safe": True})
            self.assertIsInstance(replace.call_args.kwargs["src_dir_fd"], int)
            self.assertEqual(
                replace.call_args.kwargs["src_dir_fd"],
                replace.call_args.kwargs["dst_dir_fd"],
            )
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(output.read_text()), {"safe": True})
        with self.assertRaisesRegex(PilotError, "under /tmp"):
            _private_write(Path("/workspace/not-private.json"), {"safe": True})

    def test_aggregate_report_rejects_raw_identity_or_binding_keys(self):
        _assert_aggregate_report(
            {"activity_bindings_hydrated": 2, "deal_or_activity_ids_persisted": False}
        )
        for private_value in (
            {"BINDINGS": [{"entityId": 1}]},
            {"entityId": 1},
            {"OWNER_ID": 1},
            {"activity_id": 1},
            {"id": 1},
        ):
            with self.subTest(private_value=private_value):
                with self.assertRaisesRegex(PilotError, "Private evidence key"):
                    _assert_aggregate_report(private_value)

    def test_rate_spacing_is_measured_without_triggering_quota(self):
        clock = Clock()
        transport = FakeTransport()
        client = ReadOnlyRateLimitedClient(
            transport, 1.2, sleep=clock.sleep, monotonic=clock.monotonic
        )
        client.call("crm.category.list", {"entityTypeId": 2})
        clock.value += 0.1
        client.call("crm.category.list", {"entityTypeId": 2})
        self.assertEqual(client.spacing_report()["api_call_count"], 2)
        self.assertTrue(client.spacing_report()["spacing_verified"])
        self.assertAlmostEqual(
            client.spacing_report()["minimum_observed_start_gap_seconds"], 1.2
        )

    def test_api_call_budget_fails_before_transport(self):
        transport = FakeTransport()
        client = ReadOnlyRateLimitedClient(transport, 0, max_calls=2)
        client.call("crm.category.list", {"entityTypeId": 2})
        client.call("crm.category.list", {"entityTypeId": 2})
        with self.assertRaisesRegex(PilotError, "budget"):
            client.call("crm.category.list", {"entityTypeId": 2})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(client.spacing_report()["api_call_count"], 2)
        self.assertEqual(client.spacing_report()["configured_api_call_cap"], 2)

        class CountingTransport:
            def __init__(self):
                self.calls = 0

            def call(self, method: str, params: dict[str, object]):
                self.calls += 1
                return {"result": {}, "result_error": []}

        operations_transport = CountingTransport()
        operations_client = ReadOnlyRateLimitedClient(
            operations_transport,
            0,
            max_operations=2,
        )
        operations_client.call(
            "batch",
            {
                "cmd": {
                    "one": "crm.category.list?entityTypeId=2",
                    "two": "crm.status.list?filter[ENTITY_ID]=DEAL_STAGE",
                }
            },
        )
        with self.assertRaisesRegex(PilotError, "operation budget"):
            operations_client.call("crm.category.list", {"entityTypeId": 2})
        self.assertEqual(operations_transport.calls, 1)
        operation_report = operations_client.spacing_report()
        self.assertEqual(operation_report["api_operation_count"], 2)
        self.assertEqual(operation_report["configured_api_operation_cap"], 2)

    def test_activity_get_must_match_the_production_v5_snapshot_shape(self):
        class MissingMetadataTransport(FakeTransport):
            def _activity_detail(self, activity_id: str) -> dict[str, object]:
                detail = super()._activity_detail(activity_id)
                detail.pop("PROVIDER_TYPE_ID")
                return detail

        class MismatchedBindingsTransport(FakeTransport):
            def _activity_detail(self, activity_id: str) -> dict[str, object]:
                detail = super()._activity_detail(activity_id)
                deal_id, _kind, _row = self.by_activity_id[activity_id]
                detail["BINDINGS"] = [
                    {"OWNER_TYPE_ID": "2", "OWNER_ID": str(deal_id)},
                    {"OWNER_TYPE_ID": "3", "OWNER_ID": "999"},
                ]
                return detail

        for transport_type in (MissingMetadataTransport, MismatchedBindingsTransport):
            with self.subTest(transport=transport_type.__name__):
                with self.assertRaisesRegex(PilotError, "activity-v5"):
                    run_pilot(
                        pilot_client(transport_type()),
                        year=2025,
                        sample_size=20,
                        max_discovery_pages=5,
                        activity_batch_size=20,
                        max_activity_pages=5,
                        direct_compare_count=3,
                        max_emails=10,
                        max_calls=10,
                    )

    def test_malformed_transcript_object_cannot_produce_ready_verdict(self):
        class MalformedTranscriptTransport(FakeTransport):
            def _transcript(self, activity_id: str) -> dict[str, str]:
                if activity_id not in self.by_activity_id:
                    raise AssertionError("unknown activity")
                return {}

        with self.assertRaisesRegex(PilotError, "transcript response shape"):
            run_pilot(
                pilot_client(MalformedTranscriptTransport()),
                year=2025,
                sample_size=20,
                max_discovery_pages=5,
                activity_batch_size=20,
                max_activity_pages=5,
                direct_compare_count=3,
                max_emails=10,
                max_calls=10,
            )

    def test_operational_limits_reject_quota_and_response_size_bypass(self):
        safe = {
            "min_interval": 1.2,
            "sample_size": 20,
            "max_discovery_pages": 100,
            "activity_batch_size": 20,
            "max_activity_pages": 20,
            "direct_compare_count": 3,
            "max_emails": 10,
            "max_calls": 10,
        }
        _validate_operational_limits(**safe)
        for field, unsafe_value in (
            ("min_interval", 0.0),
            ("min_interval", float("inf")),
            ("max_discovery_pages", 601),
            ("max_activity_pages", 21),
            ("direct_compare_count", 6),
            ("max_emails", 11),
        ):
            values = dict(safe)
            values[field] = unsafe_value
            with self.subTest(field=field):
                with self.assertRaises(PilotError):
                    _validate_operational_limits(**values)

    def test_activity_index_cannot_exceed_the_v5_protocol_memory_cap(self):
        class OverCapTransport:
            def __init__(self):
                self.rows = []
                for offset in range(101):
                    row = activity_row(31001, "email")
                    row["ID"] = str(500001 + offset)
                    self.rows.append(row)

            def call(self, method: str, params: dict[str, object]):
                if method != "batch":
                    raise AssertionError(f"unexpected method: {method}")
                results = {}
                for name, command in params["cmd"].items():
                    parsed = urlsplit(command)
                    query = parse_qs(parsed.query)
                    if parsed.path == "crm.activity.list":
                        last_id = int(query["filter[>ID]"][0])
                        results[name] = [
                            row for row in self.rows if int(row["ID"]) > last_id
                        ][:50]
                    elif parsed.path == "crm.activity.binding.list":
                        start = int(query["start"][0])
                        results[name] = activity_bindings(31001)[start:start + 50]
                    else:
                        raise AssertionError(f"unexpected method: {parsed.path}")
                return {"result": results, "result_error": {}}

        with self.assertRaisesRegex(PilotError, "protocol cap"):
            list_activities_batched(
                pilot_client(OverCapTransport()),
                [31001],
                batch_size=1,
                max_pages=5,
            )

    def test_binding_hydration_is_paginated_exact_and_fail_closed(self):
        row = activity_row(31001, "email")
        exact = [
            {"entityTypeId": 2, "entityId": 31001},
            *(
                {"entityTypeId": 3, "entityId": value}
                for value in range(1, 100)
            ),
        ]

        class BindingTransport:
            def __init__(self, values):
                self.values = list(values)
                self.starts = []

            def call(self, method: str, params: dict[str, object]):
                if method != "batch":
                    raise AssertionError(f"unexpected method: {method}")
                results = {}
                for name, command in params["cmd"].items():
                    parsed = urlsplit(command)
                    if parsed.path != "crm.activity.binding.list":
                        raise AssertionError(f"unexpected command: {command}")
                    query = parse_qs(parsed.query)
                    start = int(query["start"][0])
                    self.starts.append(start)
                    results[name] = self.values[start:start + 50]
                return {"result": results, "result_error": []}

        transport = BindingTransport(exact)
        hydrated, binding_only, metrics = _hydrate_activity_bindings(
            pilot_client(transport),
            31001,
            [row],
        )
        self.assertEqual(len(hydrated[0]["BINDINGS"]), 100)
        self.assertEqual(transport.starts, [0, 50, 100])
        self.assertEqual(binding_only, 1)
        self.assertEqual(metrics["pages"], 3)
        self.assertEqual(metrics["maximum_for_one_activity"], 100)

        class DirectFallbackBindingTransport(BindingTransport):
            def __init__(self, values):
                super().__init__(values)
                self.direct_starts = []

            def call(self, method: str, params: dict[str, object]):
                if method == "batch":
                    return {
                        "result": {},
                        "result_error": {
                            name: {"error": "ERROR_BATCH_METHOD_NOT_ALLOWED"}
                            for name in params["cmd"]
                        },
                    }
                if method == "crm.activity.binding.list":
                    start = int(params["start"])
                    self.direct_starts.append(start)
                    return self.values[start:start + 50]
                raise AssertionError(f"unexpected method: {method}")

        fallback = DirectFallbackBindingTransport(exact)
        fallback_hydrated, _binding_only, fallback_metrics = (
            _hydrate_activity_bindings(
                pilot_client(fallback),
                31001,
                [row],
            )
        )
        self.assertEqual(len(fallback_hydrated[0]["BINDINGS"]), 100)
        self.assertEqual(fallback.direct_starts, [0, 50, 100])
        self.assertFalse(fallback_metrics["batch_supported"])

        class MissingBindingResultTransport:
            def call(self, method: str, params: dict[str, object]):
                if method == "batch":
                    return {"result": {}, "result_error": []}
                raise AssertionError(f"unexpected method: {method}")

        with self.assertRaisesRegex(PilotError, "omitted"):
            _hydrate_activity_bindings(
                pilot_client(MissingBindingResultTransport()),
                31001,
                [row],
            )

        invalid = (
            ("overflow", [*exact, {"entityTypeId": 4, "entityId": 999}]),
            ("duplicate", [*exact[:-1], exact[0]]),
            ("wrong-type", [{"entityTypeId": "2", "entityId": 31001}]),
            ("zero", [{"entityTypeId": 2, "entityId": 0}]),
            (
                "alias-conflict",
                [
                    {
                        "entityTypeId": 2,
                        "entityId": 31001,
                        "OWNER_ID": 1,
                    }
                ],
            ),
        )
        for name, values in invalid:
            with self.subTest(name=name):
                with self.assertRaises(PilotError):
                    _hydrate_activity_bindings(
                        pilot_client(BindingTransport(values)),
                        31001,
                        [row],
                    )

    def test_direct_cli_bootstraps_repo_imports_before_live_validation(self):
        script = Path(__file__).resolve().parents[1] / "tools/activity_live_pilot.py"
        environment = dict(os.environ)
        environment.pop("BITRIX_WEBHOOK_URL", None)
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--confirm-writer-terminal",
                "--min-interval",
                "0",
            ],
            cwd="/tmp",
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("API call interval is outside the safe bounds", result.stderr)
        self.assertNotIn("ModuleNotFoundError", result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
