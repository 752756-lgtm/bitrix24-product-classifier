from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from tools.activity_live_pilot import (
    PilotError,
    ReadOnlyRateLimitedClient,
    _private_write,
    run_pilot,
)


PRIVATE_SENTINEL = "PRIVATE-ACTIVITY-EVIDENCE-SENTINEL"


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
        "BINDINGS": [
            {"OWNER_TYPE_ID": "2", "OWNER_ID": str(deal_id)},
            {"OWNER_TYPE_ID": "3", "OWNER_ID": "777"},
        ],
    }


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
        client = ReadOnlyRateLimitedClient(transport, 0)
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
            _private_write(output, {"safe": True})
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(output.read_text()), {"safe": True})
        with self.assertRaisesRegex(PilotError, "under /tmp"):
            _private_write(Path("/workspace/not-private.json"), {"safe": True})

    def test_rate_spacing_is_measured_without_triggering_quota(self):
        class Clock:
            def __init__(self):
                self.value = 0.0

            def monotonic(self):
                return self.value

            def sleep(self, seconds):
                self.value += seconds

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


if __name__ == "__main__":
    unittest.main()
