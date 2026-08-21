import json
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from classifier.precision_plan import (
    FORBIDDEN_CATEGORY_IDS,
    PLAN_FORMAT,
    PLAN_FORMAT_V2,
    PLAN_FORMAT_V3,
    PLAN_FORMAT_V4,
    PLAN_VERSION,
    ApprovedPlan,
    PlanEntry,
    activity_index_guard_fingerprint,
    activity_locator_fingerprint,
    canonical_activity_evidence,
    canonical_activity_index,
    canonical_activity_index_value,
    canonical_deal_text_evidence,
    canonical_product_evidence,
    category_desired_fingerprint,
    derive_plan_key,
    desired_fingerprint,
    entries_digest,
    key_check,
    manifest_mac,
    portal_identity,
    subcategory_guard_fingerprint,
    transition_fingerprint,
)
from classifier.precision_worker import (
    ACTIVITY_PAGE_SIZE,
    ActivityEvidenceUnavailable,
    CATEGORY_FIELD,
    SUBCATEGORY_FIELD,
    EXCLUDED_STAGE_NAMES,
    MAX_ACTIVITY_DISCOVERY_PAGES,
    MAX_UNCONFIRMED_ATTEMPTS,
    PermanentWorkerError,
    PrecisionWorker,
    State,
    cached_plan_key,
    chunks,
    is_transient_error,
    load_taxonomy,
    normalized,
    normalize_stage_name,
    persist_plan_key,
    retry_delay,
    single_instance,
)
from tools.generate_precision_plan_assets import (
    _has_private_activity_text,
    _private_activity_texts,
)


TEST_WEBHOOK = "https://example.bitrix24.test/rest/1/test-token/"
TEST_PORTAL = portal_identity(TEST_WEBHOOK)
TEST_KEY = derive_plan_key(TEST_WEBHOOK, TEST_PORTAL)


def approved_plan(rows):
    transitions = set()
    desired_modes = {}
    subcategory_guards = {}
    activity_index_guards = {}
    activity_locators = {}
    plan_entries = {}
    for row in rows:
        deal_id, original, desired = row[:3]
        evidence_mode = row[3] if len(row) > 3 else "fields"
        evidence = row[4] if len(row) > 4 else ""
        transition = transition_fingerprint(
            TEST_KEY,
            deal_id,
            original[0],
            original[1],
            desired[0],
            desired[1],
            evidence_mode,
            evidence,
        )
        transitions.add(transition)
        if evidence_mode.startswith("category_"):
            target = category_desired_fingerprint(TEST_KEY, deal_id, desired[0])
            subcategory_guards[target] = subcategory_guard_fingerprint(
                TEST_KEY,
                deal_id,
                original[1],
            )
        else:
            target = desired_fingerprint(TEST_KEY, deal_id, desired[0], desired[1])
            subcategory_guards[target] = "-"
        index_guard = row[5] if len(row) > 5 else "-"
        locators = tuple(row[6]) if len(row) > 6 else ()
        desired_modes[target] = evidence_mode
        activity_index_guards[target] = index_guard
        activity_locators[target] = locators
        plan_entries[target] = PlanEntry(
            transition=transition,
            target=target,
            evidence_mode=evidence_mode,
            subcategory_guard=subcategory_guards[target],
            activity_index_guard=index_guard,
            activity_locators=locators,
        )
    return ApprovedPlan(
        2025,
        len(rows),
        frozenset(transitions),
        desired_modes,
        "digest",
        TEST_KEY,
        subcategory_guards,
        activity_index_guards,
        activity_locators,
        plan_entries,
    )


def signed_allowlist(
    deal_id=42,
    portal=TEST_PORTAL,
    *,
    plan_format=PLAN_FORMAT,
    version=PLAN_VERSION,
    evidence_mode="fields",
    evidence="",
    activity_index_guard="-",
    activity_locators=(),
):
    transition = transition_fingerprint(
        TEST_KEY, deal_id, "old", "broad", "1821", "2071", evidence_mode, evidence
    )
    category_only = evidence_mode.startswith("category_")
    desired = (
        category_desired_fingerprint(TEST_KEY, deal_id, "1821")
        if category_only
        else desired_fingerprint(TEST_KEY, deal_id, "1821", "2071")
    )
    columns = [transition, desired, evidence_mode]
    if (plan_format, version) in ((PLAN_FORMAT_V4, 4), (PLAN_FORMAT, PLAN_VERSION)):
        columns.append(
            subcategory_guard_fingerprint(TEST_KEY, deal_id, "broad")
            if category_only
            else "-"
        )
    if (plan_format, version) == (PLAN_FORMAT, PLAN_VERSION):
        columns.extend(
            [
                activity_index_guard,
                ",".join(activity_locators) if activity_locators else "-",
            ]
        )
    content_lines = ["\t".join(columns)]
    header = {
        "format": plan_format,
        "version": version,
        "year": 2025,
        "count": 1,
        "portal": portal,
        "key_check": key_check(TEST_KEY),
        "entries_digest": entries_digest(content_lines),
    }
    header["manifest_mac"] = manifest_mac(TEST_KEY, header)
    return header, content_lines


class FakeBitrix:
    def __init__(self, live_deals=None, responses=None):
        self.live_deals = live_deals or {}
        self.responses = responses or {}
        self.calls = []

    def call(self, method, params=None):
        self.calls.append((method, params))
        if method == "crm.deal.list":
            deal_ids = {int(value) for value in params["filter"]["@ID"]}
            rows = []
            for deal_id in sorted(deal_ids):
                live = self.live_deals.get(deal_id)
                if live is None:
                    continue
                rows.append(
                    {
                        "ID": str(deal_id),
                        CATEGORY_FIELD: live[0],
                        SUBCATEGORY_FIELD: live[1],
                        "STAGE_ID": live[2],
                        "TITLE": live[3] if len(live) > 3 else "",
                        "COMMENTS": live[4] if len(live) > 4 else "",
                    }
                )
            return rows
        key = (method, (params or {}).get("filter", {}).get("ENTITY_ID"))
        if key in self.responses:
            return self.responses[key]
        if method in self.responses:
            return self.responses[method]
        raise AssertionError(f"Unexpected Bitrix method: {method}")


def make_worker(state, bitrix, directory, plan=None, pairs=(("cat-new", "sub-new"),)):
    plan = plan or approved_plan([(1, ("cat-old", "sub-old"), ("cat-new", "sub-new"))])
    return PrecisionWorker(
        state=state,
        bitrix=bitrix,
        approved_plan=plan,
        expected_categories={pair[0]: pair[0] for pair in pairs},
        expected_subcategories={pair[1]: pair[1] for pair in pairs},
        allowed_pairs=tuple(pairs),
        identity={"test": True},
        year=2025,
        batch_size=20,
        write_interval=60,
        status_path=Path(directory) / "status.json",
    )


def enqueue(
    state,
    deal_id,
    original=("cat-old", "sub-old"),
    desired=("cat-new", "sub-new"),
    status="pending",
    evidence_mode="fields",
):
    state.enqueue(
        deal_id, original, desired, "NEW", status, evidence_mode, "unit_test"
    )
    state.commit()
    return state.db.execute("SELECT * FROM queue WHERE deal_id=?", (deal_id,)).fetchone()


def activity_row(
    activity_id=501,
    *,
    deal_id=42,
    kind="email",
    subject="Запрос на тельфер",
    description="<p>Нужен тельфер 2 т</p>\r\n",
    transcription="",
    owner_type_id="3",
    owner_id="77",
):
    activity_type = "4" if kind == "email" else "2"
    direction = "1" if kind == "email" else "2"
    row = {
        "ID": str(activity_id),
        "OWNER_TYPE_ID": owner_type_id,
        "OWNER_ID": owner_id,
        "TYPE_ID": activity_type,
        "DIRECTION": direction,
        "PROVIDER_ID": "IMOPENLINES" if kind == "call" else "CRM_EMAIL",
        "PROVIDER_TYPE_ID": "CALL" if kind == "call" else "EMAIL",
        "SUBJECT": subject,
        "DESCRIPTION": description,
        "DESCRIPTION_TYPE": "3",
        "CREATED": "2025-04-01T10:00:00+03:00",
        "LAST_UPDATED": "2025-04-01T10:01:00+03:00",
        "START_TIME": "2025-04-01T10:00:00+03:00",
        "END_TIME": "2025-04-01T10:01:00+03:00",
        "COMPLETED": "Y",
        "BINDINGS": [
            {"OWNER_TYPE_ID": "3", "OWNER_ID": "77"},
            {"OWNER_TYPE_ID": "2", "OWNER_ID": str(deal_id)},
        ],
    }
    if kind == "call":
        row["transcription"] = transcription or "Клиенту нужен тельфер 2 т"
    return row


def activity_plan(
    index_rows,
    selected_rows,
    *,
    deal_id=1,
    original=("cat-old", "sub-old"),
    desired=("cat-new", "sub-new"),
    category_only=False,
):
    mode = "category_activities" if category_only else "activities"
    index = canonical_activity_index(deal_id, index_rows)
    evidence = canonical_activity_evidence(deal_id, index_rows, selected_rows)
    guard = activity_index_guard_fingerprint(TEST_KEY, deal_id, index)
    locators = sorted(
        activity_locator_fingerprint(
            TEST_KEY,
            deal_id,
            "email" if str(row["TYPE_ID"]) == "4" else "call",
            row["ID"],
        )
        for row in selected_rows
    )
    return approved_plan(
        [
            (
                deal_id,
                original,
                desired,
                mode,
                evidence,
                guard,
                locators,
            )
        ]
    )


class ActivityRuntimeBitrix:
    def __init__(
        self,
        *,
        live,
        activity_indexes,
        details,
        transcripts=None,
        detail_errors=None,
    ):
        self.live = dict(live)
        self.activity_indexes = list(activity_indexes)
        self.details = {str(key): value for key, value in details.items()}
        self.transcripts = {str(key): value for key, value in (transcripts or {}).items()}
        self.detail_errors = detail_errors or {}
        self.calls = []
        self.activity_list_count = 0
        self.update_commands = []
        self.bindings = {}
        index_rows = [
            item for page in self.activity_indexes for item in page
        ]
        for row in [*self.details.values(), *index_rows]:
            if isinstance(row, dict) and row.get("ID") is not None:
                self._remember_bindings(row)

    def _remember_bindings(self, row):
        values = row.get("BINDINGS", row.get("bindings", []))
        if not isinstance(values, list):
            return
        converted = []
        for binding in values:
            if not isinstance(binding, dict):
                continue
            entity_type = binding.get(
                "entityTypeId",
                binding.get("OWNER_TYPE_ID", binding.get("ownerTypeId")),
            )
            entity_id = binding.get(
                "entityId",
                binding.get("OWNER_ID", binding.get("ownerId")),
            )
            if entity_type is not None and entity_id is not None:
                converted.append(
                    {"entityTypeId": int(entity_type), "entityId": int(entity_id)}
                )
        self.bindings[str(row["ID"])] = converted

    def _activity_page(self, version, last_id):
        for row in version:
            self._remember_bindings(row)
        return [
            {key: value for key, value in row.items() if key != "BINDINGS"}
            for row in version
            if int(row["ID"]) > last_id
        ][:50]

    def _deal_rows(self, deal_ids):
        rows = []
        for deal_id in sorted(deal_ids):
            live = self.live.get(deal_id)
            if live is None:
                continue
            rows.append(
                {
                    "ID": str(deal_id),
                    CATEGORY_FIELD: live[0],
                    SUBCATEGORY_FIELD: live[1],
                    "STAGE_ID": live[2],
                    "TITLE": live[3] if len(live) > 3 else "",
                    "COMMENTS": live[4] if len(live) > 4 else "",
                }
            )
        return rows

    @staticmethod
    def _command_id(command):
        values = parse_qs(urlsplit(command).query)
        return str((values.get("id") or values.get("activityId") or [""])[0])

    def call(self, method, params=None):
        params = params or {}
        self.calls.append((method, params))
        if method == "crm.deal.list":
            return self._deal_rows({int(value) for value in params["filter"]["@ID"]})
        if method == "crm.activity.list":
            version = self.activity_indexes[
                min(self.activity_list_count, len(self.activity_indexes) - 1)
            ]
            self.activity_list_count += 1
            last_id = int(params["filter"][">ID"])
            return self._activity_page(version, last_id)
        if method == "crm.activity.binding.list":
            activity_id = str(params["activityId"])
            start = int(params["start"])
            return self.bindings.get(activity_id, [])[start:start + 50]
        if method == "crm.activity.call.getTranscript":
            return self.transcripts.get(str(params["activityId"]))
        if method == "batch":
            results = {}
            errors = {}
            for name, command in params["cmd"].items():
                if command.startswith("crm.activity.list?"):
                    values = parse_qs(urlsplit(command).query)
                    version = self.activity_indexes[
                        min(self.activity_list_count, len(self.activity_indexes) - 1)
                    ]
                    self.activity_list_count += 1
                    last_id = int(values["filter[>ID]"][0])
                    results[name] = self._activity_page(version, last_id)
                elif command.startswith("crm.activity.binding.list?"):
                    activity_id = self._command_id(command)
                    start = int(
                        parse_qs(urlsplit(command).query).get("start", ["0"])[0]
                    )
                    results[name] = self.bindings.get(activity_id, [])[
                        start:start + 50
                    ]
                elif command.startswith("crm.activity.get?"):
                    activity_id = self._command_id(command)
                    if activity_id in self.detail_errors:
                        errors[name] = self.detail_errors[activity_id]
                    elif activity_id in self.details:
                        results[name] = self.details[activity_id]
                    else:
                        errors[name] = {"error": "NOT_FOUND"}
                elif command.startswith("crm.activity.call.getTranscript?"):
                    activity_id = self._command_id(command)
                    results[name] = self.transcripts.get(activity_id)
                elif command.startswith("crm.deal.update?"):
                    self.update_commands.append(command)
                    values = parse_qs(urlsplit(command).query)
                    deal_id = int(values["id"][0])
                    old = self.live[deal_id]
                    category = values[f"fields[{CATEGORY_FIELD}]"][0]
                    subcategory = (
                        values.get(f"fields[{SUBCATEGORY_FIELD}]", [old[1]])[0]
                    )
                    self.live[deal_id] = (
                        category,
                        subcategory,
                        old[2],
                        old[3] if len(old) > 3 else "",
                        old[4] if len(old) > 4 else "",
                    )
                    results[name] = True
                else:
                    raise AssertionError(f"Unexpected batch command: {command}")
            return {"result": results, "result_error": errors}
        raise AssertionError(f"Unexpected Bitrix method: {method}")


class PrecisionWorkerTests(unittest.TestCase):
    def test_persisted_plan_key_roundtrip_and_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "precision-plan.key"
            persist_plan_key(path, TEST_KEY)
            self.assertEqual(cached_plan_key(path), TEST_KEY)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_cached_plan_key_rejects_invalid_value(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "precision-plan.key"
            for value in ("not-hex\n", "00\n"):
                with self.subTest(value=value.strip()):
                    path.write_text(value)
                    with self.assertRaises(PermanentWorkerError):
                        cached_plan_key(path)

    def test_helpers_and_retry_cap(self):
        self.assertEqual(normalize_stage_name("  Реклама / СПАМ  "), "реклама спам")
        self.assertEqual(normalize_stage_name("Документы — Акты"), "документы акты")
        self.assertEqual(normalized(False), "")
        self.assertEqual(normalized(0), "0")
        self.assertEqual(list(chunks(range(5), 2)), [[0, 1], [2, 3], [4]])
        self.assertTrue(is_transient_error(RuntimeError("OVERLOAD_LIMIT")))
        self.assertFalse(is_transient_error(ValueError("invalid field")))
        with patch("classifier.precision_worker.random.uniform", return_value=1.2):
            self.assertEqual(retry_delay(999, quota=True), 900.0)

    def test_product_evidence_canonicalizes_numeric_values_and_row_order(self):
        first = [
            {
                "productId": 2,
                "productName": "Таль",
                "price": "100.000",
                "quantity": "2.00",
            },
            {
                "PRODUCT_ID": "1",
                "PRODUCT_NAME": "Тележка",
                "PRICE": "12.5000",
                "QUANTITY": 1,
            },
        ]
        reordered = [
            {
                "product_id": "1",
                "name": "Тележка",
                "price": 12.5,
                "quantity": "1.000",
            },
            {
                "product_id": "2",
                "name": "Таль",
                "price": 100,
                "quantity": 2,
            },
        ]
        expected = '[["1","Тележка","12.5","1"],["2","Таль","100","2"]]'
        self.assertEqual(canonical_product_evidence(first), expected)
        self.assertEqual(canonical_product_evidence(reordered), expected)

    def test_deal_text_evidence_is_exact_and_unambiguous(self):
        title = "  Тельфер <b>2 т</b>\r\n"
        comments = "Комментарий Ёж\n"
        expected = (
            '["bitrix24-deal-text-v1","  Тельфер <b>2 т</b>\\r\\n",'
            '"Комментарий Ёж\\n"]'
        )
        self.assertEqual(canonical_deal_text_evidence(title, comments), expected)
        self.assertEqual(
            canonical_deal_text_evidence(None, False),
            canonical_deal_text_evidence(False, None),
        )
        baseline = canonical_deal_text_evidence("Title", "<p>A\r\nB</p>")
        for changed in (
            canonical_deal_text_evidence("Title ", "<p>A\r\nB</p>"),
            canonical_deal_text_evidence("title", "<p>A\r\nB</p>"),
            canonical_deal_text_evidence("Title", "<p>A\nB</p>"),
            canonical_deal_text_evidence("Title", "A\r\nB"),
            canonical_deal_text_evidence("Title", "<p>A\r\nC</p>"),
        ):
            self.assertNotEqual(baseline, changed)
        self.assertNotEqual(
            canonical_deal_text_evidence("ab", "c"),
            canonical_deal_text_evidence("a", "bc"),
        )

    def test_activity_canonicalization_is_exact_stable_and_deal_bound(self):
        email = activity_row(502)
        call = activity_row(501, kind="call")
        index_a = canonical_activity_index(42, [email, call])
        index_b = canonical_activity_index(42, [call, email])
        self.assertEqual(index_a, index_b)
        evidence = canonical_activity_evidence(42, [email, call], [email, call])
        for field, value in (
            ("SUBJECT", "Запрос на тельфер "),
            ("DESCRIPTION", "<p>Нужен тельфер 2 т</p>\n"),
            ("PROVIDER_ID", "CHANGED"),
        ):
            changed = [dict(email), dict(call)]
            changed[0][field] = value
            self.assertNotEqual(
                evidence,
                canonical_activity_evidence(42, changed, [changed[0], changed[1]]),
            )
        changed_call = dict(call)
        changed_call["transcription"] += " "
        self.assertNotEqual(
            evidence,
            canonical_activity_evidence(42, [email, call], [email, changed_call]),
        )
        self.assertNotEqual(
            activity_locator_fingerprint(TEST_KEY, 42, "email", 502),
            activity_locator_fingerprint(TEST_KEY, 43, "email", 502),
        )

    def test_activity_scope_requires_deal_binding_and_supported_direction(self):
        binding_only = activity_row(owner_type_id="3", owner_id="77")
        value = canonical_activity_index_value(42, [binding_only])
        self.assertEqual(value[2][0][1:3], ["3", "77"])
        missing_binding = dict(binding_only)
        missing_binding["BINDINGS"] = [{"OWNER_TYPE_ID": "3", "OWNER_ID": "77"}]
        with self.assertRaisesRegex(ValueError, "scope"):
            canonical_activity_index_value(42, [missing_binding])
        outgoing_email = dict(binding_only)
        outgoing_email["DIRECTION"] = "2"
        with self.assertRaisesRegex(ValueError, "scope"):
            canonical_activity_index_value(42, [outgoing_email])
        call_snapshot = activity_row(501, kind="call")
        with self.assertRaisesRegex(ValueError, "index"):
            canonical_activity_evidence(42, [binding_only], [call_snapshot])
        whitespace_call = activity_row(502, kind="call", transcription=" \r\n ")
        with self.assertRaisesRegex(ValueError, "расшифровки"):
            canonical_activity_evidence(42, [whitespace_call], [whitespace_call])
        explicit_empty = dict(binding_only)
        explicit_empty["LAST_UPDATED"] = None
        canonical_activity_index_value(42, [explicit_empty])
        missing_metadata = dict(explicit_empty)
        missing_metadata.pop("LAST_UPDATED")
        with self.assertRaisesRegex(ValueError, "обязательные поля"):
            canonical_activity_index_value(42, [missing_metadata])
        provider_none = dict(binding_only)
        provider_none["PROVIDER_ID"] = None
        provider_false = dict(binding_only)
        provider_false["PROVIDER_ID"] = False
        self.assertNotEqual(
            canonical_activity_index(42, [provider_none]),
            canonical_activity_index(42, [provider_false]),
        )
        subject_none = dict(binding_only)
        subject_none["SUBJECT"] = None
        subject_false = dict(binding_only)
        subject_false["SUBJECT"] = False
        self.assertEqual(
            canonical_activity_index(42, [subject_none]),
            canonical_activity_index(42, [subject_false]),
        )

    def test_v5_activity_rows_require_strict_guards_and_sorted_locators(self):
        email = activity_row()
        index = canonical_activity_index(42, [email])
        guard = activity_index_guard_fingerprint(TEST_KEY, 42, index)
        locator = activity_locator_fingerprint(TEST_KEY, 42, "email", 501)
        other_locator = activity_locator_fingerprint(TEST_KEY, 42, "email", 502)
        evidence = canonical_activity_evidence(42, [email], [email])
        header, lines = signed_allowlist(
            evidence_mode="activities",
            evidence=evidence,
            activity_index_guard=guard,
            activity_locators=(locator,),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.allowlist"

            def load_line(columns):
                content = ["\t".join(columns)]
                signed = dict(header)
                signed.pop("manifest_mac", None)
                signed["entries_digest"] = entries_digest(content)
                signed["manifest_mac"] = manifest_mac(TEST_KEY, signed)
                path.write_text(json.dumps(signed) + "\n" + content[0] + "\n")
                return ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

            loaded = load_line(lines[0].split("\t"))
            entry = loaded.entry_for_target(42, ("1821", "2071"), "activities")
            self.assertEqual(entry.activity_index_guard, guard)
            self.assertEqual(entry.activity_locators, (locator,))
            columns = lines[0].split("\t")
            malformed = (
                columns[:4] + ["-", locator],
                columns[:4] + [guard, "-"],
                columns[:4] + [guard, f"{locator},{locator}"],
                columns[:4] + [guard, ",".join(sorted((locator, other_locator), reverse=True))],
                columns[:4] + [guard, "g" * 32],
                columns[:4] + [guard, ",".join([locator] * 9)],
            )
            for bad in malformed:
                with self.subTest(columns=bad[4:]):
                    with self.assertRaises(ValueError):
                        load_line(bad)

            non_activity = signed_allowlist()[1][0].split("\t")
            non_activity[4:] = [guard, locator]
            with self.assertRaisesRegex(ValueError, "activity поля"):
                load_line(non_activity)

            category_header, category_lines = signed_allowlist(
                evidence_mode="category_activities",
                evidence=evidence,
                activity_index_guard=guard,
                activity_locators=(locator,),
            )
            header = category_header
            category_loaded = load_line(category_lines[0].split("\t"))
            category_entry = category_loaded.entry_for_target(
                42,
                ("1821", "broad"),
                "category_activities",
            )
            self.assertIsNotNone(category_entry)
            self.assertRegex(category_entry.subcategory_guard, r"^[0-9a-f]{32}$")

    def test_plan_recovers_only_approved_target_and_transition(self):
        plan = approved_plan([(42, ("old", "broad"), ("1821", "2071"))])
        pairs = (("1821", "2071"), ("1823", "2285"))
        self.assertEqual(
            plan.target_for(42, pairs),
            ("1821", "2071", "fields", True),
        )
        self.assertIsNone(plan.target_for(43, pairs))
        self.assertTrue(plan.approves_transition(42, ("old", "broad"), ("1821", "2071")))
        self.assertFalse(plan.approves_transition(42, ("manual", "change"), ("1821", "2071")))

    def test_allowlist_loader_rejects_wrong_key_portal_and_tampered_manifest(self):
        header, content_lines = signed_allowlist()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.allowlist"
            path.write_text(
                json.dumps(header) + "\n" + "\n".join(content_lines) + "\n"
            )
            loaded = ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)
            self.assertEqual(loaded.count, 1)
            with self.assertRaisesRegex(ValueError, "Ключ"):
                ApprovedPlan.load(
                    path, derive_plan_key("wrong", TEST_PORTAL), 2025, TEST_PORTAL
                )
            with self.assertRaisesRegex(ValueError, "другого портала"):
                ApprovedPlan.load(
                    path,
                    TEST_KEY,
                    2025,
                    portal_identity("https://other.bitrix24.test/rest/1/token/"),
                )

            tampered = dict(header)
            tampered["count"] = 2
            path.write_text(
                json.dumps(tampered) + "\n" + "\n".join(content_lines) + "\n"
            )
            with self.assertRaisesRegex(ValueError, "Подпись заголовка"):
                ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

    def test_allowlist_loader_supports_v2_through_v5_with_protocol_gated_modes(self):
        evidence = canonical_deal_text_evidence("Заголовок", "Комментарий")
        cases = [
            signed_allowlist(plan_format=PLAN_FORMAT_V2, version=2),
            signed_allowlist(
                plan_format=PLAN_FORMAT_V3,
                version=3,
                evidence_mode="deal_text",
                evidence=evidence,
            ),
            signed_allowlist(
                plan_format=PLAN_FORMAT_V4,
                version=4,
                evidence_mode="deal_text",
                evidence=evidence,
            ),
            signed_allowlist(
                plan_format=PLAN_FORMAT_V4,
                version=4,
                evidence_mode="category_fields",
            ),
            signed_allowlist(evidence_mode="deal_text", evidence=evidence),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.allowlist"
            for header, content_lines in cases:
                path.write_text(json.dumps(header) + "\n" + "\n".join(content_lines) + "\n")
                self.assertEqual(
                    ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL).count, 1
                )

            header, content_lines = signed_allowlist(
                plan_format=PLAN_FORMAT_V2,
                version=2,
                evidence_mode="deal_text",
                evidence=evidence,
            )
            path.write_text(json.dumps(header) + "\n" + "\n".join(content_lines) + "\n")
            with self.assertRaisesRegex(ValueError, "Некорректный источник"):
                ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

            header, content_lines = signed_allowlist(
                plan_format=PLAN_FORMAT_V3,
                version=3,
                evidence_mode="category_fields",
            )
            path.write_text(json.dumps(header) + "\n" + "\n".join(content_lines) + "\n")
            with self.assertRaisesRegex(ValueError, "Некорректный источник"):
                ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

            for plan_format, version in (("unknown", 4), (PLAN_FORMAT, 99)):
                header, content_lines = signed_allowlist(
                    plan_format=plan_format, version=version
                )
                path.write_text(json.dumps(header) + "\n" + "\n".join(content_lines) + "\n")
                with self.assertRaisesRegex(ValueError, "Неподдерживаемый формат"):
                    ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

    def test_allowlist_loader_enforces_protocol_columns_and_guard_shapes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.allowlist"

            def write_signed(header, content_lines):
                updated = dict(header)
                updated.pop("manifest_mac", None)
                updated["entries_digest"] = entries_digest(content_lines)
                updated["manifest_mac"] = manifest_mac(TEST_KEY, updated)
                path.write_text(
                    json.dumps(updated) + "\n" + "\n".join(content_lines) + "\n"
                )

            for plan_format, version in (
                (PLAN_FORMAT_V2, 2),
                (PLAN_FORMAT_V3, 3),
            ):
                header, lines = signed_allowlist(
                    plan_format=plan_format,
                    version=version,
                )
                write_signed(header, lines)
                self.assertEqual(
                    ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL).count,
                    1,
                )
                columns = lines[0].split("\t")
                for malformed in (
                    ["\t".join(columns[:2])],
                    ["\t".join(columns + ["-"])],
                ):
                    write_signed(header, malformed)
                    with self.assertRaisesRegex(ValueError, "Некорректная строка"):
                        ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

            full_header, full_lines = signed_allowlist(
                plan_format=PLAN_FORMAT_V4,
                version=4,
            )
            write_signed(full_header, full_lines)
            loaded = ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)
            self.assertEqual(set(loaded.subcategory_guards.values()), {"-"})
            full_columns = full_lines[0].split("\t")
            for malformed in (
                ["\t".join(full_columns[:3])],
                ["\t".join(full_columns + ["extra"])],
            ):
                write_signed(full_header, malformed)
                with self.assertRaisesRegex(ValueError, "Некорректная строка"):
                    ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)
            full_columns[3] = "0" * 32
            write_signed(full_header, ["\t".join(full_columns)])
            with self.assertRaisesRegex(ValueError, "защита подкатегории"):
                ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

            category_header, category_lines = signed_allowlist(
                plan_format=PLAN_FORMAT_V4,
                version=4,
                evidence_mode="category_fields",
            )
            write_signed(category_header, category_lines)
            category_loaded = ApprovedPlan.load(
                path,
                TEST_KEY,
                2025,
                TEST_PORTAL,
            )
            self.assertTrue(
                category_loaded.category_guard_matches(42, "1821", "broad")
            )
            category_columns = category_lines[0].split("\t")
            for invalid_guard in ("-", "0" * 31, "g" * 32):
                invalid = [*category_columns[:3], invalid_guard]
                write_signed(category_header, ["\t".join(invalid)])
                with self.assertRaisesRegex(ValueError, "защита подкатегории"):
                    ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

    def test_category_target_and_subcategory_guard_use_separate_domains(self):
        category_target = category_desired_fingerprint(TEST_KEY, 42, "1821")
        full_target = desired_fingerprint(TEST_KEY, 42, "1821", "legacy-a")
        guard_a = subcategory_guard_fingerprint(TEST_KEY, 42, "legacy-a")
        guard_b = subcategory_guard_fingerprint(TEST_KEY, 42, "legacy-b")
        self.assertNotEqual(category_target, full_target)
        self.assertNotEqual(category_target, guard_a)
        self.assertNotEqual(guard_a, guard_b)
        self.assertEqual(
            category_target,
            category_desired_fingerprint(TEST_KEY, 42, "1821"),
        )

    def test_generator_keeps_deal_text_private_and_rejects_empty_comments(self):
        sentinel_title = "SENTINEL-TITLE-9da4"
        sentinel_comment = "SENTINEL-COMMENT-b72f"
        row = {
            "deal_id": 42,
            "current_category_id": "old",
            "current_subcategory_id": "broad",
            "category_id": "1821",
            "subcategory_id": "2071",
            "category": "Грузоподъёмное оборудование",
            "subcategory": "Тельферы электрические канатные",
            "reason": "deal_text",
            "title": sentinel_title,
            "comments": sentinel_comment,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_plan = root / "private.json"
            products = root / "products.json"
            allowlist = root / "public.allowlist"
            taxonomy = root / "taxonomy.json"
            private_plan.write_text(json.dumps([row], ensure_ascii=False))
            private_plan.chmod(0o600)
            products.write_text("{}")
            command = [
                sys.executable,
                "tools/generate_precision_plan_assets.py",
                "--plan", str(private_plan),
                "--allowlist", str(allowlist),
                "--taxonomy", str(taxonomy),
                "--products", str(products),
                "--year", "2025",
            ]
            env = {
                **os.environ,
                "BITRIX_WEBHOOK_URL": TEST_WEBHOOK,
                "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            }
            result = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            public = allowlist.read_text()
            public_taxonomy = taxonomy.read_text()
            self.assertNotIn(sentinel_title, public + public_taxonomy + result.stdout)
            self.assertNotIn(sentinel_comment, public + public_taxonomy + result.stdout)
            header, line = public.splitlines()
            self.assertEqual(json.loads(header)["format"], PLAN_FORMAT)
            self.assertRegex(
                line,
                r"^[0-9a-f]{32}\t[0-9a-f]{32}\tdeal_text\t-\t-\t-$",
            )

            private_plan.write_text(
                json.dumps([{**row, "comments": ""}], ensure_ascii=False)
            )
            rejected = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("нет комментария", rejected.stderr)

    def test_generator_recomputes_v5_activity_guards_without_leaking_raw_evidence(self):
        email = activity_row(
            987654321,
            subject="SENTINEL-ACTIVITY-SUBJECT-19f3",
            description="SENTINEL-ACTIVITY-BODY-a08d\r\n",
        )
        index_row = dict(email)
        index_row.pop("DESCRIPTION")
        get_row = dict(email)
        get_row.pop("BINDINGS")
        row = {
            "deal_id": 42,
            "current_category_id": "old",
            "current_subcategory_id": "broad",
            "category_id": "1821",
            "subcategory_id": "2071",
            "category": "Грузоподъёмное оборудование",
            "subcategory": "Тельферы электрические канатные",
            "reason": "activities",
            "activity_index": [index_row],
            "selected_activities": [get_row],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_plan = root / "private.json"
            products = root / "products.json"
            allowlist = root / "public.allowlist"
            taxonomy = root / "taxonomy.json"
            private_plan.write_text(json.dumps([row], ensure_ascii=False))
            private_plan.chmod(0o600)
            products.write_text("{}")
            result = subprocess.run(
                [
                    sys.executable,
                    "tools/generate_precision_plan_assets.py",
                    "--plan", str(private_plan),
                    "--allowlist", str(allowlist),
                    "--taxonomy", str(taxonomy),
                    "--products", str(products),
                    "--year", "2025",
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={
                    **os.environ,
                    "BITRIX_WEBHOOK_URL": TEST_WEBHOOK,
                    "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
                },
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            public = allowlist.read_text() + taxonomy.read_text() + result.stdout + result.stderr
            self.assertNotIn(email["SUBJECT"], public)
            self.assertNotIn(email["DESCRIPTION"], public)
            self.assertNotIn(email["ID"], public)
            columns = allowlist.read_text().splitlines()[1].split("\t")
            self.assertEqual(len(columns), 6)
            self.assertEqual(columns[2:4], ["activities", "-"])
            self.assertRegex(columns[4], r"^[0-9a-f]{32}$")
            self.assertRegex(columns[5], r"^[0-9a-f]{32}$")
            loaded = ApprovedPlan.load(allowlist, TEST_KEY, 2025, TEST_PORTAL)
            entry = loaded.entry_for_target(42, ("1821", "2071"), "activities")
            self.assertIsNotNone(entry)
            self.assertEqual(entry.activity_index_guard, columns[4])
            self.assertEqual(entry.activity_locators, (columns[5],))

            short_index = activity_row(987654322, subject="S7")
            positional_index = canonical_activity_index_value(42, [short_index])
            self.assertIn("S7", _private_activity_texts(positional_index))
            self.assertFalse(
                _has_private_activity_text(
                    {"1", "a", "-", "S7", "year", "count", "format"},
                    allowlist.read_text(),
                    taxonomy.read_text(),
                    result.stdout,
                )
            )
            self.assertTrue(
                _has_private_activity_text(
                    {"S7"},
                    allowlist.read_text().rstrip("\n") + "\tS7\n",
                    taxonomy.read_text(),
                    result.stdout,
                )
            )
            leaked_taxonomy = json.loads(taxonomy.read_text())
            leaked_taxonomy["categories"]["1821"] = "S7"
            self.assertTrue(
                _has_private_activity_text(
                    {"S7"},
                    allowlist.read_text(),
                    json.dumps(leaked_taxonomy, ensure_ascii=False),
                    result.stdout,
                )
            )

    def test_state_persists_and_rejects_other_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "worker.sqlite3"
            state = State(path)
            state.bind_identity({"portal": "one", "year": 2025})
            enqueue(state, 1, status="retry_wait")
            state.close()
            reopened = State(path)
            try:
                self.assertEqual(reopened.counts(), {"retry_wait": 1})
                with self.assertRaises(PermanentWorkerError):
                    reopened.bind_identity({"portal": "two", "year": 2025})
            finally:
                reopened.close()

    def test_state_uses_protocol_neutral_source_label(self):
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                state.enqueue(
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "NEW",
                    "pending",
                    "fields",
                )
                state.commit()
                row = state.db.execute(
                    "SELECT source FROM queue WHERE deal_id=1"
                ).fetchone()
                self.assertEqual(row["source"], "approved_plan")
            finally:
                state.close()

    def test_deal_text_values_are_not_persisted_in_state_or_status(self):
        sentinel_title = "PRIVATE-TITLE-f3be"
        sentinel_comments = "PRIVATE-COMMENTS-18c9"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    canonical_deal_text_evidence(sentinel_title, sentinel_comments),
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="deal_text")
                worker = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker.write_status()
            finally:
                state.close()
            persisted = b"".join(
                path.read_bytes() for path in root.iterdir() if path.is_file()
            )
            self.assertNotIn(sentinel_title.encode(), persisted)
            self.assertNotIn(sentinel_comments.encode(), persisted)

    def test_waiting_instance_acquires_lock_after_handoff(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "worker.lock"
            marker = Path(directory) / "acquired"
            script = (
                "import sys\n"
                "from pathlib import Path\n"
                "from classifier.precision_worker import single_instance\n"
                "with single_instance(Path(sys.argv[1]), wait_for_lock=True):\n"
                "    Path(sys.argv[2]).write_text('ok')\n"
            )
            with single_instance(lock_path):
                process = subprocess.Popen(
                    [sys.executable, "-c", script, str(lock_path), str(marker)]
                )
                time.sleep(0.15)
                self.assertIsNone(process.poll())
                self.assertFalse(marker.exists())
            process.wait(timeout=3)
            self.assertEqual(process.returncode, 0)
            self.assertEqual(marker.read_text(), "ok")

    def test_live_guard_covers_all_safe_states(self):
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                item = enqueue(state, 1)
                worker = make_worker(state, FakeBitrix(), directory)
                worker.excluded_stage_ids = {"EXCLUDED"}
                self.assertEqual(
                    worker.classify_live(item, ("cat-new", "sub-new", "NEW", "")),
                    "verified",
                )
                self.assertEqual(
                    worker.classify_live(item, ("cat-old", "sub-old", "NEW", "")),
                    "write",
                )
                self.assertEqual(
                    worker.classify_live(item, ("manual", "manual", "NEW", "")),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", "sub-old", "EXCLUDED", "")
                    ),
                    "excluded_stage",
                )
                self.assertEqual(worker.classify_live(item, None), "missing")
            finally:
                state.close()

    def test_title_evidence_change_becomes_conflict(self):
        original_title = "Тельфер электрический канатный 2 т"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "title",
                    original_title,
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                item = enqueue(state, 1, evidence_mode="title")
                worker = make_worker(state, FakeBitrix(), directory, plan=plan)
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", "sub-old", "NEW", original_title)
                    ),
                    "write",
                )
                self.assertEqual(
                    worker.classify_live(
                        item,
                        ("cat-old", "sub-old", "NEW", "Станок для гибки арматуры"),
                    ),
                    "conflict",
                )
            finally:
                state.close()

    def test_deal_text_live_guard_is_exact_and_still_idempotent(self):
        title = "Тельфер электрический канатный 2 т"
        comments = "<p>Нужна поставка\r\nдо пятницы</p>"
        evidence = canonical_deal_text_evidence(title, comments)
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    evidence,
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                item = enqueue(state, 1, evidence_mode="deal_text")
                worker = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker.excluded_stage_ids = {"EXCLUDED"}
                original = ("cat-old", "sub-old", "NEW", title, comments)
                self.assertEqual(worker.classify_live(item, original), "write")
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", "sub-old", "NEW", title + " ", comments)
                    ),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", "sub-old", "NEW", title, comments + " ")
                    ),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(item, ("cat-old", "sub-old", "NEW", title, "")),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(item, ("manual", "manual", "NEW", title, comments)),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", "sub-old", "EXCLUDED", title, comments)
                    ),
                    "excluded_stage",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-new", "sub-new", "NEW", "changed", "changed")
                    ),
                    "verified",
                )
            finally:
                state.close()

    def test_fetch_live_selects_comments_and_chunks_ids(self):
        live = {
            deal_id: ("cat-old", "sub-old", "NEW", f"Title {deal_id}", f"Comment {deal_id}")
            for deal_id in range(1, 102)
        }
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                fake = FakeBitrix(live)
                worker = make_worker(state, fake, directory)
                result = worker.fetch_live(list(live))
                self.assertEqual(result, live)
                calls = [params for method, params in fake.calls if method == "crm.deal.list"]
                self.assertEqual([len(params["filter"]["@ID"]) for params in calls], [50, 50, 1])
                self.assertTrue(all("COMMENTS" in params["select"] for params in calls))
            finally:
                state.close()

    def test_scan_fetches_comments_only_for_deal_text_and_uses_full_snapshot(self):
        fresh_title = "Свежий TITLE"
        fresh_comments = "Свежий COMMENTS"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    canonical_deal_text_evidence(fresh_title, fresh_comments),
                ),
                (
                    2,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "title",
                    "Title-only",
                ),
                (
                    3,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    canonical_deal_text_evidence("Missing", "Missing comment"),
                ),
            ]
        )

        class ScanBitrix:
            def __init__(self):
                self.calls = []

            def call(self, method, params=None):
                self.calls.append((method, params))
                if method != "crm.deal.list":
                    raise AssertionError(method)
                if "@ID" in params["filter"]:
                    return [
                        {
                            "ID": "1",
                            CATEGORY_FIELD: "cat-old",
                            SUBCATEGORY_FIELD: "sub-old",
                            "STAGE_ID": "NEW",
                            "TITLE": fresh_title,
                            "COMMENTS": fresh_comments,
                        }
                    ]
                return [
                    {
                        "ID": "1", CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "sub-old", "STAGE_ID": "NEW",
                        "TITLE": "Устаревший TITLE",
                    },
                    {
                        "ID": "2", CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "sub-old", "STAGE_ID": "NEW",
                        "TITLE": "Title-only",
                    },
                    {
                        "ID": "3", CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "sub-old", "STAGE_ID": "NEW",
                        "TITLE": "Missing",
                    },
                ]

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                fake = ScanBitrix()
                worker = make_worker(state, fake, directory, plan=plan)
                worker.scan_deals()
                statuses = {
                    int(row["deal_id"]): row["status"]
                    for row in state.db.execute("SELECT deal_id,status FROM queue")
                }
                self.assertEqual(statuses, {1: "pending", 2: "pending", 3: "missing"})
                calls = [params for method, params in fake.calls if method == "crm.deal.list"]
                self.assertNotIn("COMMENTS", calls[0]["select"])
                self.assertEqual(calls[1]["filter"]["@ID"], [1, 3])
                self.assertIn("COMMENTS", calls[1]["select"])
            finally:
                state.close()

    def test_category_only_rescan_discovers_changed_subcategory_as_conflict(self):
        signed_subcategory = "signed-legacy-subcategory"
        plan = approved_plan(
            [
                (
                    deal_id,
                    ("cat-old", signed_subcategory),
                    ("cat-new", signed_subcategory),
                    "category_fields",
                    "",
                )
                for deal_id in (1, 2, 3, 4)
            ]
        )

        class CategoryScanBitrix:
            def call(self, method, params=None):
                if method != "crm.deal.list" or "@ID" in params["filter"]:
                    raise AssertionError((method, params))
                return [
                    {
                        "ID": "1",
                        CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "changed-subcategory",
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                    {
                        "ID": "2",
                        CATEGORY_FIELD: "cat-new",
                        SUBCATEGORY_FIELD: "changed-after-category-update",
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                    {
                        "ID": "3",
                        CATEGORY_FIELD: "cat-new",
                        SUBCATEGORY_FIELD: "changed-but-excluded",
                        "STAGE_ID": "EXCLUDED",
                        "TITLE": "",
                    },
                    {
                        "ID": "4",
                        CATEGORY_FIELD: "cat-new",
                        SUBCATEGORY_FIELD: signed_subcategory,
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                ]

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                worker = PrecisionWorker(
                    state,
                    CategoryScanBitrix(),
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                worker.excluded_stage_ids = {"EXCLUDED"}
                worker.scan_deals()
                statuses = {
                    int(row["deal_id"]): row["status"]
                    for row in state.db.execute(
                        "SELECT deal_id,status FROM queue ORDER BY deal_id"
                    )
                }
                self.assertEqual(
                    statuses,
                    {
                        1: "conflict",
                        2: "conflict",
                        3: "excluded_stage",
                        4: "verified",
                    },
                )
                status = json.loads((Path(directory) / "status.json").read_text())
                self.assertEqual(status["discovered"], 4)
                self.assertEqual(status["undiscovered"], 0)
                self.assertTrue(status["scan_complete"])
            finally:
                state.close()

    def test_category_deal_text_guard_conflict_does_not_fetch_private_comments(self):
        title = "Signed title"
        comments = "PRIVATE-COMMENTS-MUST-NOT-BE-FETCHED"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "signed-subcategory"),
                    ("cat-new", "signed-subcategory"),
                    "category_deal_text",
                    canonical_deal_text_evidence(title, comments),
                )
            ]
        )

        class GuardFirstScanBitrix:
            def __init__(self):
                self.calls = []

            def call(self, method, params=None):
                self.calls.append((method, params))
                if "@ID" in params["filter"]:
                    raise AssertionError("COMMENTS must not be fetched after guard mismatch")
                return [
                    {
                        "ID": "1",
                        CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "changed-subcategory",
                        "STAGE_ID": "NEW",
                        "TITLE": title,
                    }
                ]

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                bitrix = GuardFirstScanBitrix()
                worker = PrecisionWorker(
                    state,
                    bitrix,
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                worker.scan_deals()
                self.assertEqual(state.counts(), {"conflict": 1})
                self.assertEqual(len(bitrix.calls), 1)
                self.assertNotIn("COMMENTS", bitrix.calls[0][1]["select"])
            finally:
                state.close()

    def test_changed_product_row_becomes_conflict(self):
        approved_rows = [
            {
                "productId": "10",
                "productName": "Тельфер электрический канатный",
                "price": "100000.00",
                "quantity": "1.0",
            }
        ]
        approved_evidence = canonical_product_evidence(approved_rows)
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "products",
                    approved_evidence,
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                item = enqueue(state, 1, evidence_mode="products")
                worker = make_worker(state, FakeBitrix(), directory, plan=plan)
                live = ("cat-old", "sub-old", "NEW", "Название не участвует")
                self.assertEqual(
                    worker.classify_live(item, live, approved_evidence), "write"
                )
                changed = canonical_product_evidence(
                    [{**approved_rows[0], "quantity": "2"}]
                )
                self.assertEqual(worker.classify_live(item, live, changed), "conflict")
                self.assertEqual(worker.classify_live(item, live, None), "conflict")
            finally:
                state.close()

    def test_recover_inflight_verifies_requeues_or_stops(self):
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                for deal_id in range(1, 6):
                    enqueue(state, deal_id, status="inflight")
                fake = FakeBitrix(
                    {
                        1: ("cat-new", "sub-new", "NEW"),
                        2: ("cat-old", "sub-old", "NEW"),
                        3: ("manual", "manual", "NEW"),
                        4: ("cat-old", "sub-old", "EXCLUDED"),
                    }
                )
                worker = make_worker(state, fake, directory)
                worker.approved_plan = approved_plan(
                    [
                        (deal_id, ("cat-old", "sub-old"), ("cat-new", "sub-new"))
                        for deal_id in range(1, 6)
                    ]
                )
                worker.excluded_stage_ids = {"EXCLUDED"}
                worker.recover_inflight()
                statuses = {
                    int(row["deal_id"]): row["status"]
                    for row in state.db.execute("SELECT deal_id,status FROM queue ORDER BY deal_id")
                }
                self.assertEqual(
                    statuses,
                    {1: "verified", 2: "pending", 3: "conflict", 4: "excluded_stage", 5: "missing"},
                )
            finally:
                state.close()

    def test_fetch_product_evidence_groups_rows_and_paginates(self):
        first_page = [
            {
                "ownerId": 1 if index % 2 == 0 else 2,
                "productId": index + 1,
                "productName": f"Товар {index + 1}",
                "price": f"{index + 1}.00",
                "quantity": "1.0",
            }
            for index in range(50)
        ]
        second_page = [
            {
                "ownerId": 1,
                "productId": 51,
                "productName": "Товар 51",
                "price": "51.0",
                "quantity": "2.00",
            },
            {
                "ownerId": 3,
                "productId": 52,
                "productName": "Товар 52",
                "price": "52",
                "quantity": 3,
            },
        ]

        class ProductRowsBitrix:
            def __init__(self):
                self.starts = []

            def call(self, method, params=None):
                self.assert_method(method)
                start = params.get("start", 0)
                self.starts.append(start)
                return {
                    "productRows": first_page if start == 0 else second_page
                }

            @staticmethod
            def assert_method(method):
                if method != "crm.item.productrow.list":
                    raise AssertionError(f"Unexpected Bitrix method: {method}")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                fake = ProductRowsBitrix()
                worker = make_worker(state, fake, directory)
                result = worker.fetch_product_evidence([1, 2, 3])
                expected_rows = first_page + second_page
                for deal_id in (1, 2, 3):
                    self.assertEqual(
                        result[deal_id],
                        canonical_product_evidence(
                            [
                                row
                                for row in expected_rows
                                if row["ownerId"] == deal_id
                            ]
                        ),
                    )
                self.assertEqual(fake.starts, [0, 50])
            finally:
                state.close()

    def test_excluded_stages_are_collected_from_all_funnels(self):
        default_rows = [
            {"NAME": "Дубль", "STATUS_ID": "DUPLICATE"},
            {"NAME": "Поставщик", "STATUS_ID": "SUPPLIER"},
        ]
        extra_rows = [
            {"NAME": "Дубль", "STATUS_ID": "DUPLICATE"},
            {"NAME": "Реклама / Спам", "STATUS_ID": "SPAM"},
            {"NAME": "Документы / Акты", "STATUS_ID": "DOCS"},
            {"NAME": "Доставка", "STATUS_ID": "DELIVERY"},
        ]
        fake = FakeBitrix(
            responses={
                "crm.category.list": {"categories": [{"id": 7}]},
                ("crm.status.list", "DEAL_STAGE"): default_rows,
                ("crm.status.list", "DEAL_STAGE_7"): extra_rows,
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                worker = make_worker(state, fake, directory)
                values = worker._resolve_excluded_stages()
                self.assertEqual(
                    values,
                    {"DUPLICATE", "SUPPLIER", "C7:DUPLICATE", "C7:SPAM", "C7:DOCS", "C7:DELIVERY"},
                )
                self.assertEqual(EXCLUDED_STAGE_NAMES, {"дубль", "реклама спам", "поставщик", "документы акты", "доставка"})
            finally:
                state.close()

    def test_category_list_pagination_discovers_extra_funnel(self):
        class PaginatedCategoryBitrix:
            def __init__(self):
                self.calls = []

            def call(self, method, params=None):
                self.calls.append((method, params))
                if method == "crm.category.list":
                    start = params.get("start", 0)
                    if start == 0:
                        return {"categories": [{"id": value} for value in range(1, 51)]}
                    if start == 50:
                        return {"categories": [{"id": 77}]}
                    return {"categories": []}
                if method == "crm.status.list":
                    if params["filter"]["ENTITY_ID"] == "DEAL_STAGE":
                        return [
                            {"NAME": "Дубль", "STATUS_ID": "DUPLICATE"},
                            {"NAME": "Реклама / Спам", "STATUS_ID": "SPAM"},
                            {"NAME": "Поставщик", "STATUS_ID": "SUPPLIER"},
                            {"NAME": "Документы / Акты", "STATUS_ID": "DOCS"},
                            {"NAME": "Доставка", "STATUS_ID": "DELIVERY"},
                        ]
                    return [{"NAME": "Новая", "STATUS_ID": "NEW"}]
                raise AssertionError(f"Unexpected Bitrix method: {method}")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                fake = PaginatedCategoryBitrix()
                worker = make_worker(state, fake, directory)
                worker._resolve_excluded_stages()
                category_starts = [
                    params["start"]
                    for method, params in fake.calls
                    if method == "crm.category.list"
                ]
                status_entities = {
                    params["filter"]["ENTITY_ID"]
                    for method, params in fake.calls
                    if method == "crm.status.list"
                }
                self.assertEqual(category_starts, [0, 50])
                self.assertIn("DEAL_STAGE_77", status_entities)
            finally:
                state.close()

    def test_malformed_category_list_fails_closed(self):
        fake = FakeBitrix(
            responses={
                "crm.category.list": {},
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                worker = make_worker(state, fake, directory)
                with self.assertRaisesRegex(
                    PermanentWorkerError, "ответ без списка categories"
                ):
                    worker._resolve_excluded_stages()
            finally:
                state.close()

    def test_empty_extra_funnel_fails_closed(self):
        fake = FakeBitrix(
            responses={
                "crm.category.list": {"categories": [{"id": 7}]},
                ("crm.status.list", "DEAL_STAGE"): [
                    {"NAME": "Дубль", "STATUS_ID": "DUPLICATE"},
                    {"NAME": "Реклама / Спам", "STATUS_ID": "SPAM"},
                    {"NAME": "Поставщик", "STATUS_ID": "SUPPLIER"},
                    {"NAME": "Документы / Акты", "STATUS_ID": "DOCS"},
                    {"NAME": "Доставка", "STATUS_ID": "DELIVERY"},
                ],
                ("crm.status.list", "DEAL_STAGE_7"): [],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                worker = make_worker(state, fake, directory)
                with self.assertRaisesRegex(
                    PermanentWorkerError, "DEAL_STAGE_7 не вернула ни одного этапа"
                ):
                    worker._resolve_excluded_stages()
            finally:
                state.close()

    def test_missing_required_stage_fails_closed(self):
        fake = FakeBitrix(
            responses={
                "crm.category.list": {"categories": []},
                ("crm.status.list", "DEAL_STAGE"): [{"NAME": "Дубль", "STATUS_ID": "DUPLICATE"}],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                worker = make_worker(state, fake, directory)
                with self.assertRaises(PermanentWorkerError):
                    worker._resolve_excluded_stages()
            finally:
                state.close()

    def test_batch_error_parser_keeps_per_command_error(self):
        errors = PrecisionWorker._batch_errors(
            {
                "result_error": {
                    "deal_17": {"error": "ERROR_CORE", "error_description": "bad field"},
                    "unrelated": {"error": "ignored"},
                }
            }
        )
        self.assertEqual(set(errors), {17})
        self.assertIn("bad field", str(errors[17]))

    def test_product_evidence_is_fetched_before_final_live_guard(self):
        events = []
        product_evidence = canonical_product_evidence(
            [
                {
                    "productId": "10",
                    "productName": "Тельфер",
                    "price": "100",
                    "quantity": "1",
                }
            ]
        )

        class OrderedWorker(PrecisionWorker):
            def wait_for_write_slot(self):
                events.append("wait")

            def fetch_product_evidence(self, deal_ids):
                events.append("products")
                return {1: product_evidence}

            def fetch_live(self, deal_ids):
                events.append("live")
                if events.count("live") == 1:
                    return {1: ("cat-old", "sub-old", "NEW", "")}
                return {1: ("cat-new", "sub-new", "NEW", "")}

            def call(self, method, params=None):
                events.append(method)
                return {"result": {"deal_1": True}, "result_error": {}}

            def sleep(self, seconds):
                events.append("verify_wait")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="products")
                plan = approved_plan(
                    [
                        (
                            1,
                            ("cat-old", "sub-old"),
                            ("cat-new", "sub-new"),
                            "products",
                            product_evidence,
                        )
                    ]
                )
                base = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker = OrderedWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(events[:4], ["wait", "products", "live", "batch"])
                self.assertEqual(state.counts(), {"verified": 1})
            finally:
                state.close()

    def test_deal_text_change_at_final_guard_prevents_batch(self):
        events = []
        title = "Точный заголовок"
        comments = "Точный комментарий"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    canonical_deal_text_evidence(title, comments),
                )
            ]
        )

        class GuardWorker(PrecisionWorker):
            def wait_for_write_slot(self):
                events.append("wait")

            def fetch_live(self, deal_ids):
                events.append("live_with_comments")
                return {
                    1: ("cat-old", "sub-old", "NEW", title, comments + " изменён")
                }

            def call(self, method, params=None):
                events.append(method)
                raise AssertionError("batch must not be called after evidence conflict")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="deal_text")
                base = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker = GuardWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(events, ["wait", "live_with_comments"])
                self.assertEqual(state.counts(), {"conflict": 1})
            finally:
                state.close()

    def test_deal_text_final_guard_runs_after_wait_and_immediately_before_batch(self):
        events = []
        title = "Точный заголовок"
        comments = "Точный комментарий"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "deal_text",
                    canonical_deal_text_evidence(title, comments),
                )
            ]
        )

        class OrderedDealTextWorker(PrecisionWorker):
            fetch_count = 0

            def wait_for_write_slot(self):
                events.append("wait")

            def fetch_live(self, deal_ids):
                self.fetch_count += 1
                events.append("live_with_comments")
                current = (
                    ("cat-old", "sub-old")
                    if self.fetch_count == 1
                    else ("cat-new", "sub-new")
                )
                return {1: (*current, "NEW", title, comments)}

            def call(self, method, params=None):
                events.append(method)
                return {"result": {"deal_1": True}, "result_error": {}}

            def sleep(self, seconds):
                events.append("verify_wait")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="deal_text")
                base = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker = OrderedDealTextWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(events[:3], ["wait", "live_with_comments", "batch"])
                self.assertEqual(state.counts(), {"verified": 1})
            finally:
                state.close()

    def test_run_exit_when_complete_returns_without_hour_sleep(self):
        events = []

        class BoundedWorker(PrecisionWorker):
            def initialize(self):
                events.append("initialize")

            def process_one_batch(self):
                events.append("process")
                return False

            def write_status(self, error=""):
                events.append(("status", error))

            def sleep(self, seconds):
                raise AssertionError(f"bounded worker must not sleep for {seconds} seconds")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, status="verified")
                base = make_worker(state, FakeBitrix(), directory)
                worker = BoundedWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )

                worker.run(exit_when_complete=True)

                self.assertEqual(events[:2], ["initialize", "process"])
                self.assertEqual(state.counts(), {"verified": 1})
                self.assertEqual(events.count(("status", "")), 2)
            finally:
                state.close()

    def test_timeout_after_successful_write_is_verified_not_retried(self):
        class TimeoutWorker(PrecisionWorker):
            fetch_count = 0

            def wait_for_write_slot(self):
                pass

            def fetch_live(self, deal_ids):
                self.fetch_count += 1
                value = (
                    ("cat-old", "sub-old", "NEW", "")
                    if self.fetch_count == 1
                    else ("cat-new", "sub-new", "NEW", "")
                )
                return {1: value}

            def call(self, method, params=None):
                raise TimeoutError("timed out after server accepted request")

            def sleep(self, seconds):
                pass

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1)
                base = make_worker(state, FakeBitrix(), directory)
                worker = TimeoutWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(state.counts(), {"verified": 1})
            finally:
                state.close()

    def test_partial_batch_errors_split_permanent_and_retry(self):
        class PartialWorker(PrecisionWorker):
            def wait_for_write_slot(self):
                pass

            def fetch_live(self, deal_ids):
                return {
                    deal_id: ("cat-old", "sub-old", "NEW", "")
                    for deal_id in deal_ids
                }

            def call(self, method, params=None):
                return {
                    "result_error": {
                        "deal_1": {"error": "ERROR_ARGUMENT", "error_description": "bad field"},
                        "deal_2": {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "wait"},
                    }
                }

            def sleep(self, seconds):
                pass

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1)
                enqueue(state, 2)
                plan = approved_plan(
                    [
                        (deal_id, ("cat-old", "sub-old"), ("cat-new", "sub-new"))
                        for deal_id in (1, 2)
                    ]
                )
                base = make_worker(state, FakeBitrix(), directory, plan=plan)
                worker = PartialWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(state.counts(), {"permanent_error": 1, "retry_wait": 1})
            finally:
                state.close()

    def test_unconfirmed_write_stops_after_retry_limit(self):
        class UnconfirmedWorker(PrecisionWorker):
            def wait_for_write_slot(self):
                pass

            def fetch_live(self, deal_ids):
                return {
                    deal_id: ("cat-old", "sub-old", "NEW", "")
                    for deal_id in deal_ids
                }

            def call(self, method, params=None):
                return {"result": {"deal_1": True}, "result_error": {}}

            def sleep(self, seconds):
                pass

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1)
                state.db.execute(
                    "UPDATE queue SET attempts=? WHERE deal_id=1",
                    (MAX_UNCONFIRMED_ATTEMPTS - 1,),
                )
                state.commit()
                base = make_worker(state, FakeBitrix(), directory)
                worker = UnconfirmedWorker(
                    state, base.bitrix, base.approved_plan, base.expected_categories,
                    base.expected_subcategories, base.allowed_pairs, base.identity,
                    2025, 20, 60, Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()

                worker.process_one_batch()

                row = state.db.execute(
                    "SELECT status,attempts FROM queue WHERE deal_id=1"
                ).fetchone()
                self.assertEqual(row["status"], "permanent_error")
                self.assertEqual(row["attempts"], MAX_UNCONFIRMED_ATTEMPTS)
            finally:
                state.close()

    def test_v5_wire_protocol_is_not_accepted_by_the_v4_worker_gate(self):
        header, _ = signed_allowlist(evidence_mode="category_fields")
        protocol = (str(header["format"]), int(header["version"]))
        legacy_v4_protocols = {
            (PLAN_FORMAT_V2, 2),
            (PLAN_FORMAT_V3, 3),
            (PLAN_FORMAT_V4, 4),
        }
        self.assertEqual(protocol, (PLAN_FORMAT, PLAN_VERSION))
        self.assertNotIn(protocol, legacy_v4_protocols)

        old_header, old_lines = signed_allowlist(
            plan_format=PLAN_FORMAT_V4,
            version=4,
            evidence_mode="activities",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.allowlist"
            path.write_text(
                json.dumps(old_header) + "\n" + "\n".join(old_lines) + "\n"
            )
            with self.assertRaisesRegex(ValueError, "Некорректный источник"):
                ApprovedPlan.load(path, TEST_KEY, 2025, TEST_PORTAL)

    def test_category_only_target_resolution_reports_exact_subcategory_guard(self):
        for deal_id, subcategory in ((41, ""), (42, "legacy-subcategory-991")):
            with self.subTest(subcategory=subcategory or "blank"):
                plan = approved_plan(
                    [
                        (
                            deal_id,
                            ("cat-old", subcategory),
                            ("cat-new", subcategory),
                            "category_fields",
                            "",
                        )
                    ]
                )
                self.assertEqual(
                    plan.target_for(
                        deal_id,
                        (("cat-new", "unrelated-subcategory"),),
                        current=("cat-old", subcategory),
                        allowed_categories=("cat-new",),
                    ),
                    ("cat-new", subcategory, "category_fields", True),
                )
                self.assertEqual(
                    plan.target_for(
                        deal_id,
                        (),
                        current=("cat-new", subcategory),
                        allowed_categories=("cat-new",),
                    ),
                    ("cat-new", subcategory, "category_fields", True),
                )
                self.assertIsNone(
                    plan.target_for(
                        deal_id,
                        (),
                        current=("cat-new", subcategory),
                        allowed_categories=(),
                    )
                )
                changed_subcategory = subcategory + "changed"
                self.assertEqual(
                    plan.target_for(
                        deal_id,
                        (("cat-new", subcategory),),
                        current=("cat-old", changed_subcategory),
                        allowed_categories=("cat-new",),
                    ),
                    ("cat-new", changed_subcategory, "category_fields", False),
                )
                self.assertIsNone(
                    plan.target_for(
                        deal_id,
                        (),
                        current=("cat-old", subcategory),
                        allowed_categories=("different-live-category",),
                    )
                )

        forbidden = next(iter(FORBIDDEN_CATEGORY_IDS))
        forbidden_plan = approved_plan(
            [
                (
                    43,
                    ("cat-old", "legacy"),
                    (forbidden, "legacy"),
                    "category_fields",
                    "",
                )
            ]
        )
        self.assertIsNone(
            forbidden_plan.target_for(
                43,
                ((forbidden, "legacy"),),
                current=("cat-old", "legacy"),
                allowed_categories=(forbidden,),
            )
        )

    def test_category_only_live_guard_requires_signed_subcategory_and_text(self):
        title = "Точный заголовок category-only"
        comments = "Точный комментарий category-only"
        subcategory = "legacy-subcategory-55"
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", subcategory),
                    ("cat-new", subcategory),
                    "category_deal_text",
                    canonical_deal_text_evidence(title, comments),
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                item = enqueue(
                    state,
                    1,
                    original=("cat-old", subcategory),
                    desired=("cat-new", subcategory),
                    evidence_mode="category_deal_text",
                )
                worker = PrecisionWorker(
                    state,
                    FakeBitrix(),
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", subcategory, "NEW", title, comments)
                    ),
                    "write",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-old", subcategory, "NEW", title, comments + "!")
                    ),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(
                        item, ("cat-new", subcategory, "NEW", "changed", "changed")
                    ),
                    "verified",
                )
                self.assertEqual(
                    worker.classify_live(
                        item,
                        ("cat-new", subcategory + "-concurrent", "NEW", title, comments),
                    ),
                    "conflict",
                )
            finally:
                state.close()

    def test_category_only_title_and_product_evidence_are_guarded(self):
        title = "Точный category title"
        product_rows = [
            {
                "productId": "12",
                "productName": "Точный category product",
                "price": "100",
                "quantity": "1",
            }
        ]
        product_evidence = canonical_product_evidence(product_rows)
        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", "legacy"),
                    ("cat-new", "legacy"),
                    "category_title",
                    title,
                ),
                (
                    2,
                    ("cat-old", "legacy"),
                    ("cat-new", "legacy"),
                    "category_products",
                    product_evidence,
                ),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                title_item = enqueue(
                    state,
                    1,
                    original=("cat-old", "legacy"),
                    desired=("cat-new", "legacy"),
                    evidence_mode="category_title",
                )
                product_item = enqueue(
                    state,
                    2,
                    original=("cat-old", "legacy"),
                    desired=("cat-new", "legacy"),
                    evidence_mode="category_products",
                )
                worker = PrecisionWorker(
                    state,
                    FakeBitrix(),
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                live = ("cat-old", "legacy", "NEW")
                self.assertEqual(
                    worker.classify_live(title_item, (*live, title, "")),
                    "write",
                )
                self.assertEqual(
                    worker.classify_live(title_item, (*live, title + "!", "")),
                    "conflict",
                )
                self.assertEqual(
                    worker.classify_live(
                        product_item, (*live, "unused", ""), product_evidence
                    ),
                    "write",
                )
                changed_product = canonical_product_evidence(
                    [{**product_rows[0], "quantity": "2"}]
                )
                self.assertEqual(
                    worker.classify_live(
                        product_item, (*live, "unused", ""), changed_product
                    ),
                    "conflict",
                )
            finally:
                state.close()

    def test_category_only_batch_sends_only_category_and_preserves_blank_subcategory(self):
        commands = {}

        class CategoryOnlyWorker(PrecisionWorker):
            fetch_count = 0

            def wait_for_write_slot(self):
                pass

            def fetch_live(self, deal_ids):
                self.fetch_count += 1
                category = "cat-old" if self.fetch_count == 1 else "cat-new"
                return {1: (category, "", "NEW", "", "")}

            def call(self, method, params=None):
                commands.update(params["cmd"])
                return {"result": {"deal_1": True}, "result_error": {}}

            def sleep(self, seconds):
                pass

        plan = approved_plan(
            [(1, ("cat-old", ""), ("cat-new", ""), "category_fields", "")]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(
                    state,
                    1,
                    original=("cat-old", ""),
                    desired=("cat-new", ""),
                    evidence_mode="category_fields",
                )
                worker = CategoryOnlyWorker(
                    state,
                    FakeBitrix(),
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(state.counts(), {"verified": 1})
                command = commands["deal_1"]
                self.assertIn(f"fields%5B{CATEGORY_FIELD}%5D=cat-new", command)
                self.assertNotIn(SUBCATEGORY_FIELD, command)
                self.assertNotIn(f"fields%5B{SUBCATEGORY_FIELD}%5D", command)
            finally:
                state.close()

    def test_category_only_after_write_concurrent_subcategory_change_conflicts(self):
        subcategory = "legacy-subcategory-77"

        class ConcurrentSubcategoryWorker(PrecisionWorker):
            fetch_count = 0

            def wait_for_write_slot(self):
                pass

            def fetch_live(self, deal_ids):
                self.fetch_count += 1
                if self.fetch_count == 1:
                    current = ("cat-old", subcategory)
                else:
                    current = ("cat-new", subcategory + "-manual-change")
                return {1: (*current, "NEW", "", "")}

            def call(self, method, params=None):
                return {"result": {"deal_1": True}, "result_error": {}}

            def sleep(self, seconds):
                pass

        plan = approved_plan(
            [
                (
                    1,
                    ("cat-old", subcategory),
                    ("cat-new", subcategory),
                    "category_fields",
                    "",
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(
                    state,
                    1,
                    original=("cat-old", subcategory),
                    desired=("cat-new", subcategory),
                    evidence_mode="category_fields",
                )
                worker = ConcurrentSubcategoryWorker(
                    state,
                    FakeBitrix(),
                    plan,
                    {"cat-new": "Новая категория"},
                    {},
                    (),
                    {"test": True},
                    2025,
                    20,
                    60,
                    Path(directory) / "status.json",
                )
                worker.metadata_resolved_at = time.time()
                worker.process_one_batch()
                self.assertEqual(state.counts(), {"conflict": 1})
            finally:
                state.close()

    def test_v4_mixed_plan_uses_full_target_and_category_guard_protocols(self):
        private_subcategory = "PRIVATE-MIXED-SUBCATEGORY-40c1"
        rows = [
            {
                "deal_id": 201,
                "current_category_id": "old-full",
                "current_subcategory_id": "old-full-sub",
                "category_id": "1821",
                "subcategory_id": "2071",
                "category": "Грузоподъёмное оборудование",
                "subcategory": "Тельферы электрические канатные",
                "reason": "existing_precise_subcategory",
            },
            {
                "deal_id": 202,
                "current_category_id": "old-category",
                "current_subcategory_id": private_subcategory,
                "category_id": "1823",
                "category": "Станки для обработки арматуры",
                "reason": "existing_precise_subcategory",
                "category_only": True,
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan_path = root / "private.json"
            products_path = root / "products.json"
            allowlist_path = root / "public.allowlist"
            taxonomy_path = root / "taxonomy.json"
            plan_path.write_text(json.dumps(rows, ensure_ascii=False))
            plan_path.chmod(0o600)
            products_path.write_text("{}")
            result = subprocess.run(
                [
                    sys.executable,
                    "tools/generate_precision_plan_assets.py",
                    "--plan", str(plan_path),
                    "--allowlist", str(allowlist_path),
                    "--taxonomy", str(taxonomy_path),
                    "--products", str(products_path),
                    "--year", "2025",
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={
                    **os.environ,
                    "BITRIX_WEBHOOK_URL": TEST_WEBHOOK,
                    "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
                },
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn(private_subcategory, allowlist_path.read_text())
            lines = allowlist_path.read_text().splitlines()
            by_mode = {line.split("\t")[2]: line.split("\t") for line in lines[1:]}
            self.assertEqual(len(by_mode["fields"]), 6)
            self.assertEqual(by_mode["fields"][3], "-")
            self.assertEqual(by_mode["fields"][4:], ["-", "-"])
            self.assertEqual(
                by_mode["fields"][1],
                desired_fingerprint(TEST_KEY, 201, "1821", "2071"),
            )
            self.assertEqual(len(by_mode["category_fields"]), 6)
            self.assertEqual(
                by_mode["category_fields"][1],
                category_desired_fingerprint(TEST_KEY, 202, "1823"),
            )
            self.assertEqual(
                by_mode["category_fields"][3],
                subcategory_guard_fingerprint(
                    TEST_KEY,
                    202,
                    private_subcategory,
                ),
            )
            self.assertEqual(by_mode["category_fields"][4:], ["-", "-"])

            loaded = ApprovedPlan.load(
                allowlist_path,
                TEST_KEY,
                2025,
                TEST_PORTAL,
            )
            self.assertTrue(loaded.requires_full_pairs)
            taxonomy = json.loads(taxonomy_path.read_text())
            self.assertEqual(taxonomy["pairs"], [["1821", "2071"]])
            self.assertEqual(
                loaded.target_for(
                    201,
                    (("1821", "2071"),),
                    current=("old-full", "old-full-sub"),
                    allowed_categories=("1821", "1823"),
                ),
                ("1821", "2071", "fields", True),
            )
            self.assertEqual(
                loaded.target_for(
                    202,
                    (("1821", "2071"),),
                    current=("old-category", "changed-private-subcategory"),
                    allowed_categories=("1821", "1823"),
                ),
                (
                    "1823",
                    "changed-private-subcategory",
                    "category_fields",
                    False,
                ),
            )
            load_taxonomy(
                taxonomy_path,
                2025,
                require_pairs=loaded.requires_full_pairs,
            )

    def test_category_only_generator_keeps_subcategory_and_evidence_private(self):
        sentinels = {
            "PRIVATE-SUBCATEGORY-6ac8",
            "PRIVATE-TITLE-f105",
            "PRIVATE-PRODUCT-341e",
            "PRIVATE-COMMENTS-277b",
        }
        rows = [
            {
                "deal_id": 101,
                "current_category_id": "",
                "current_subcategory_id": "PRIVATE-SUBCATEGORY-6ac8",
                "category_id": "1821",
                "category": "Грузоподъёмное оборудование",
                "reason": "existing_precise_subcategory",
                "category_only": True,
            },
            {
                "deal_id": 102,
                "current_category_id": "",
                "current_subcategory_id": "",
                "category_id": "1821",
                "category": "Грузоподъёмное оборудование",
                "reason": "title_rule",
                "title": "PRIVATE-TITLE-f105",
                "category_only": True,
            },
            {
                "deal_id": 103,
                "current_category_id": "legacy-category",
                "current_subcategory_id": "legacy-subcategory",
                "category_id": "1821",
                "category": "Грузоподъёмное оборудование",
                "reason": "product_value",
                "category_only": True,
            },
            {
                "deal_id": 104,
                "current_category_id": "legacy-category",
                "current_subcategory_id": "legacy-subcategory",
                "category_id": "1821",
                "category": "Грузоподъёмное оборудование",
                "reason": "deal_text",
                "title": "Обычный заголовок",
                "comments": "PRIVATE-COMMENTS-277b",
                "category_only": True,
            },
        ]
        products = {
            "103": [
                {
                    "productId": "7",
                    "productName": "PRIVATE-PRODUCT-341e",
                    "price": "100",
                    "quantity": "1",
                }
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_plan = root / "private.json"
            product_path = root / "products.json"
            allowlist = root / "public.allowlist"
            taxonomy = root / "taxonomy.json"
            private_plan.write_text(json.dumps(rows, ensure_ascii=False))
            private_plan.chmod(0o600)
            product_path.write_text(json.dumps(products, ensure_ascii=False))
            command = [
                sys.executable,
                "tools/generate_precision_plan_assets.py",
                "--plan", str(private_plan),
                "--allowlist", str(allowlist),
                "--taxonomy", str(taxonomy),
                "--products", str(product_path),
                "--year", "2025",
            ]
            env = {
                **os.environ,
                "BITRIX_WEBHOOK_URL": TEST_WEBHOOK,
                "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            }
            result = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            public = allowlist.read_text() + taxonomy.read_text() + result.stdout
            for sentinel in sentinels:
                self.assertNotIn(sentinel, public)
            header, *content_lines = allowlist.read_text().splitlines()
            self.assertEqual(json.loads(header)["format"], PLAN_FORMAT)
            self.assertEqual(
                {line.split("\t")[2] for line in content_lines},
                {
                    "category_fields",
                    "category_title",
                    "category_products",
                    "category_deal_text",
                },
            )
            self.assertTrue(
                all(
                    re.fullmatch(r"[0-9a-f]{32}", line.split("\t")[3])
                    for line in content_lines
                )
            )
            taxonomy_payload = json.loads(taxonomy.read_text())
            self.assertEqual(
                taxonomy_payload["categories"],
                {"1821": "Грузоподъёмное оборудование"},
            )
            self.assertEqual(taxonomy_payload["subcategories"], {})
            self.assertEqual(taxonomy_payload["pairs"], [])
            self.assertEqual(
                load_taxonomy(taxonomy, 2025, require_pairs=False)[:3],
                (
                    {"1821": "Грузоподъёмное оборудование"},
                    {},
                    (),
                ),
            )

            forbidden_rows = [{**rows[0], "category_id": "6901"}]
            private_plan.write_text(json.dumps(forbidden_rows, ensure_ascii=False))
            rejected = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("запрещённая категория", rejected.stderr)

            mismatched_subcategory = [
                {
                    **rows[0],
                    "subcategory_id": "attempted-new-subcategory",
                }
            ]
            private_plan.write_text(
                json.dumps(mismatched_subcategory, ensure_ascii=False)
            )
            rejected = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("не может менять подкатегорию", rejected.stderr)

    def test_category_only_taxonomy_allows_zero_pairs_but_requires_categories(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "taxonomy.json"

            def write(categories):
                path.write_text(
                    json.dumps(
                        {
                            "format": "bitrix24-precision-taxonomy-v1",
                            "version": 1,
                            "year": 2025,
                            "categories": categories,
                            "subcategories": {},
                            "pairs": [],
                        },
                        ensure_ascii=False,
                    )
                )

            write({"1821": "Грузоподъёмное оборудование"})
            categories, subcategories, pairs, _ = load_taxonomy(
                path, 2025, require_pairs=False
            )
            self.assertEqual(categories, {"1821": "Грузоподъёмное оборудование"})
            self.assertEqual(subcategories, {})
            self.assertEqual(pairs, ())
            with self.assertRaisesRegex(PermanentWorkerError, "полных пар"):
                load_taxonomy(path, 2025, require_pairs=True)

            write({})
            with self.assertRaisesRegex(PermanentWorkerError, "целевых категорий"):
                load_taxonomy(path, 2025, require_pairs=False)

            write({"6901": "Новая или запрещённая категория"})
            with self.assertRaisesRegex(PermanentWorkerError, "запрещённые категории"):
                load_taxonomy(path, 2025, require_pairs=False)

    def test_activity_scan_defers_evidence_and_skips_terminal_rows(self):
        records = []
        for deal_id, category_only in (
            (1, False),
            (2, False),
            (3, True),
            (4, False),
        ):
            selected = activity_row(500 + deal_id, deal_id=deal_id)
            original = ("cat-old", "signed-sub")
            desired = (
                ("cat-new", "signed-sub")
                if category_only
                else ("cat-new", "sub-new")
            )
            mode = "category_activities" if category_only else "activities"
            evidence = canonical_activity_evidence(deal_id, [selected], [selected])
            records.append(
                (
                    deal_id,
                    original,
                    desired,
                    mode,
                    evidence,
                    activity_index_guard_fingerprint(
                        TEST_KEY,
                        deal_id,
                        canonical_activity_index(deal_id, [selected]),
                    ),
                    [
                        activity_locator_fingerprint(
                            TEST_KEY, deal_id, "email", selected["ID"]
                        )
                    ],
                )
            )
        plan = approved_plan(records)

        class ScanBitrix:
            def __init__(self):
                self.calls = []

            def call(self, method, params=None):
                self.calls.append((method, params))
                if method != "crm.deal.list":
                    raise AssertionError("activity APIs must not run during scan")
                return [
                    {
                        "ID": "1",
                        CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "signed-sub",
                        "STAGE_ID": "DUP",
                        "TITLE": "",
                    },
                    {
                        "ID": "2",
                        CATEGORY_FIELD: "cat-new",
                        SUBCATEGORY_FIELD: "sub-new",
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                    {
                        "ID": "3",
                        CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "manually-changed",
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                    {
                        "ID": "4",
                        CATEGORY_FIELD: "cat-old",
                        SUBCATEGORY_FIELD: "signed-sub",
                        "STAGE_ID": "NEW",
                        "TITLE": "",
                    },
                ]

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                bitrix = ScanBitrix()
                worker = make_worker(
                    state,
                    bitrix,
                    directory,
                    plan=plan,
                    pairs=(("cat-new", "sub-new"),),
                )
                worker.allowed_category_ids = ("cat-new",)
                worker.excluded_stage_ids = {"DUP"}
                worker.scan_deals()
                self.assertEqual(
                    state.counts(),
                    {
                        "excluded_stage": 1,
                        "verified": 1,
                        "conflict": 1,
                        "pending": 1,
                    },
                )
                self.assertFalse(
                    any(method.startswith("crm.activity") for method, _ in bitrix.calls)
                )
            finally:
                state.close()

    def test_activity_discovery_uses_binding_keyset_without_offsets(self):
        rows = [activity_row(index, deal_id=1) for index in range(1, 101)]

        class KeysetBitrix:
            def __init__(self):
                self.calls = []

            def call(self, method, params=None):
                self.calls.append((method, params))
                if method == "batch":
                    results = {}
                    for name, command in params["cmd"].items():
                        if command.startswith("crm.activity.binding.list?"):
                            values = parse_qs(urlsplit(command).query)
                            activity_id = values["activityId"][0]
                            start = int(values["start"][0])
                            row = next(
                                item for item in rows
                                if item["ID"] == activity_id
                            )
                            results[name] = [
                                {
                                    "entityTypeId": int(binding["OWNER_TYPE_ID"]),
                                    "entityId": int(binding["OWNER_ID"]),
                                }
                                for binding in row["BINDINGS"]
                            ][start:start + 50]
                            continue
                        values = parse_qs(urlsplit(command).query)
                        if values.get("start") != ["0"]:
                            raise AssertionError("offset pagination is forbidden")
                        last_id = int(values["filter[>ID]"][0])
                        results[name] = [
                            {
                                key: value
                                for key, value in row.items()
                                if key != "BINDINGS"
                            }
                            for row in rows if int(row["ID"]) > last_id
                        ][:50]
                    return {"result": results, "result_error": {}}
                if method == "crm.activity.binding.list":
                    row = next(
                        item for item in rows
                        if item["ID"] == str(params["activityId"])
                    )
                    start = int(params["start"])
                    return [
                        {
                            "entityTypeId": int(binding["OWNER_TYPE_ID"]),
                            "entityId": int(binding["OWNER_ID"]),
                        }
                        for binding in row["BINDINGS"]
                    ][start:start + 50]
                self.assert_params(params)
                last_id = int(params["filter"][">ID"])
                return [
                    {
                        key: value
                        for key, value in row.items()
                        if key != "BINDINGS"
                    }
                    for row in rows if int(row["ID"]) > last_id
                ][:50]

            @staticmethod
            def assert_params(params):
                if params.get("start") != 0:
                    raise AssertionError("offset pagination is forbidden")
                if params.get("order") != {"ID": "ASC"}:
                    raise AssertionError("activity order must be ID ASC")
                if params["filter"]["BINDINGS"] != [
                    {"OWNER_TYPE_ID": 2, "OWNER_ID": 1}
                ]:
                    raise AssertionError("deal binding filter is required")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                bitrix = KeysetBitrix()
                worker = make_worker(state, bitrix, directory)
                found = worker.list_relevant_activities(1)
                self.assertEqual([int(row["ID"]) for row in found], list(range(1, 101)))
                self.assertEqual(
                    [
                        call[1]["filter"][">ID"]
                        for call in bitrix.calls
                        if call[0] == "crm.activity.list"
                    ],
                    [0, 50, 100],
                )
                self.assertTrue(all(row["OWNER_TYPE_ID"] == "3" for row in found))
                batched, failures = worker.list_relevant_activities_many([1])
                self.assertEqual(failures, {})
                self.assertEqual(
                    [int(row["ID"]) for row in batched[1]],
                    list(range(1, 101)),
                )
            finally:
                state.close()

    def test_activity_binding_hydration_reads_all_pages_and_explicit_eof(self):
        index_row = activity_row(7001, deal_id=1)
        index_row.pop("BINDINGS")
        all_bindings = [
            {"entityTypeId": 2, "entityId": 1},
            *(
                {"entityTypeId": 3, "entityId": value}
                for value in range(1, 100)
            ),
        ]

        class PagedBindingBitrix:
            def __init__(self, *, direct_fallback=False, values=None):
                self.direct_fallback = direct_fallback
                self.values = list(values if values is not None else all_bindings)
                self.batch_starts = []
                self.direct_starts = []

            def call(self, method, params=None):
                params = params or {}
                if method == "batch":
                    results = {}
                    errors = {}
                    for name, command in params["cmd"].items():
                        parsed = urlsplit(command)
                        if parsed.path != "crm.activity.binding.list":
                            raise AssertionError(f"unexpected command: {command}")
                        values = parse_qs(parsed.query)
                        start = int(values["start"][0])
                        self.batch_starts.append(start)
                        if self.direct_fallback:
                            errors[name] = {
                                "error": "ERROR_BATCH_METHOD_NOT_ALLOWED"
                            }
                        else:
                            results[name] = self.values[start:start + 50]
                    return {
                        "result": results,
                        # The live portal returns an empty list on success.
                        "result_error": errors if errors else [],
                    }
                if method == "crm.activity.binding.list":
                    start = int(params["start"])
                    self.direct_starts.append(start)
                    return self.values[start:start + 50]
                raise AssertionError(f"unexpected method: {method}")

        for direct_fallback in (False, True):
            with self.subTest(
                direct_fallback=direct_fallback
            ), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    bitrix = PagedBindingBitrix(
                        direct_fallback=direct_fallback
                    )
                    worker = make_worker(state, bitrix, directory)
                    hydrated = worker._hydrate_activity_bindings(1, [index_row])
                    self.assertEqual(len(hydrated[0]["BINDINGS"]), 100)
                    self.assertEqual(bitrix.batch_starts, [0, 50, 100])
                    self.assertEqual(
                        bitrix.direct_starts,
                        [0, 50, 100] if direct_fallback else [],
                    )
                finally:
                    state.close()

        invalid_values = (
            (
                "overflow",
                [*all_bindings, {"entityTypeId": 4, "entityId": 999}],
                ValueError,
            ),
            (
                "cross-page-duplicate",
                [*all_bindings[:-1], all_bindings[0]],
                ValueError,
            ),
            (
                "non-positive",
                [{"entityTypeId": 2, "entityId": 0}],
                ValueError,
            ),
            (
                "conflicting-alias",
                [
                    {
                        "entityTypeId": 2,
                        "entityId": 1,
                        "OWNER_ID": 2,
                    }
                ],
                ValueError,
            ),
        )
        for name, values, expected_error in invalid_values:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    worker = make_worker(
                        state,
                        PagedBindingBitrix(values=values),
                        directory,
                    )
                    with self.assertRaises(expected_error):
                        worker._hydrate_activity_bindings(1, [index_row])
                finally:
                    state.close()

    def test_activity_discovery_page_cap_applies_to_direct_and_batch(self):
        rows = []
        for activity_id in range(1, MAX_ACTIVITY_DISCOVERY_PAGES * 50 + 1):
            row = activity_row(activity_id, deal_id=1)
            row["DIRECTION"] = "2"
            row.pop("BINDINGS")
            rows.append(row)

        class EndlessUnsupportedBitrix:
            def __init__(self):
                self.direct_calls = 0
                self.batch_calls = 0

            def call(self, method, params=None):
                params = params or {}
                if method == "crm.activity.list":
                    self.direct_calls += 1
                    last_id = int(params["filter"][">ID"])
                    return [row for row in rows if int(row["ID"]) > last_id][
                        :ACTIVITY_PAGE_SIZE
                    ]
                if method == "batch":
                    self.batch_calls += 1
                    results = {}
                    for name, command in params["cmd"].items():
                        parsed = urlsplit(command)
                        if parsed.path != "crm.activity.list":
                            raise AssertionError(f"unexpected command: {command}")
                        values = parse_qs(parsed.query)
                        last_id = int(values["filter[>ID]"][0])
                        results[name] = [
                            row for row in rows if int(row["ID"]) > last_id
                        ][:ACTIVITY_PAGE_SIZE]
                    return {"result": results, "result_error": []}
                raise AssertionError(f"unexpected method: {method}")

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                direct_bitrix = EndlessUnsupportedBitrix()
                direct_worker = make_worker(
                    state,
                    direct_bitrix,
                    directory,
                )
                with self.assertRaisesRegex(ValueError, "страниц"):
                    direct_worker.list_relevant_activities(1)
                self.assertEqual(
                    direct_bitrix.direct_calls,
                    MAX_ACTIVITY_DISCOVERY_PAGES,
                )

                batch_bitrix = EndlessUnsupportedBitrix()
                batch_worker = make_worker(
                    state,
                    batch_bitrix,
                    directory,
                )
                found, failures = batch_worker.list_relevant_activities_many([1])
                self.assertEqual(found, {})
                self.assertIsInstance(failures[1], ValueError)
                self.assertEqual(
                    batch_bitrix.batch_calls,
                    MAX_ACTIVITY_DISCOVERY_PAGES,
                )
            finally:
                state.close()

    def test_activity_get_timeout_splits_batch_before_row_level_failure(self):
        selected_rows = [
            activity_row(700 + offset, deal_id=1)
            for offset in range(4)
        ]

        class SplittingBitrix(ActivityRuntimeBitrix):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.detail_batch_sizes = []

            def call(self, method, params=None):
                commands = (params or {}).get("cmd", {})
                if method == "batch" and commands and all(
                    command.startswith("crm.activity.get?")
                    for command in commands.values()
                ):
                    self.detail_batch_sizes.append(len(commands))
                    if len(commands) > 2:
                        self.calls.append((method, params))
                        raise TimeoutError("activity detail batch timed out")
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                bitrix = SplittingBitrix(
                    live={},
                    activity_indexes=[[]],
                    details={row["ID"]: row for row in selected_rows},
                )
                worker = make_worker(state, bitrix, directory)
                with patch.object(worker, "sleep"):
                    result = worker.fetch_selected_activity_content(
                        [("email", row) for row in selected_rows]
                    )
                self.assertEqual(bitrix.detail_batch_sizes, [4, 2, 2])
                self.assertEqual(
                    [row["ID"] for row in result or []],
                    [row["ID"] for row in selected_rows],
                )
            finally:
                state.close()

    def test_activity_get_result_error_splits_to_singletons(self):
        selected_rows = [
            activity_row(800 + offset, deal_id=1)
            for offset in range(4)
        ]

        class PerCommandTimeoutBitrix(ActivityRuntimeBitrix):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.detail_batch_sizes = []

            def call(self, method, params=None):
                commands = (params or {}).get("cmd", {})
                if method == "batch" and commands and all(
                    command.startswith("crm.activity.get?")
                    for command in commands.values()
                ):
                    self.detail_batch_sizes.append(len(commands))
                    if len(commands) > 1:
                        self.calls.append((method, params))
                        return {
                            "result": {},
                            "result_error": {
                                name: {"error": "OPERATION_TIME_LIMIT"}
                                for name in commands
                            },
                        }
                return super().call(method, params)

        class SingletonTimeoutBitrix(PerCommandTimeoutBitrix):
            def call(self, method, params=None):
                commands = (params or {}).get("cmd", {})
                if method == "batch" and len(commands) == 1 and all(
                    command.startswith("crm.activity.get?")
                    for command in commands.values()
                ):
                    self.detail_batch_sizes.append(1)
                    self.calls.append((method, params))
                    return {
                        "result": {},
                        "result_error": {
                            next(iter(commands)): {
                                "error": "OPERATION_TIME_LIMIT"
                            }
                        },
                    }
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                bitrix = PerCommandTimeoutBitrix(
                    live={},
                    activity_indexes=[[]],
                    details={row["ID"]: row for row in selected_rows},
                )
                worker = make_worker(state, bitrix, directory)
                result = worker.fetch_selected_activity_content(
                    [("email", row) for row in selected_rows]
                )
                self.assertEqual(
                    bitrix.detail_batch_sizes,
                    [4, 2, 1, 1, 2, 1, 1],
                )
                self.assertEqual(len(result or []), 4)

                singleton_bitrix = SingletonTimeoutBitrix(
                    live={},
                    activity_indexes=[[]],
                    details={selected_rows[0]["ID"]: selected_rows[0]},
                )
                singleton_worker = make_worker(
                    state,
                    singleton_bitrix,
                    directory,
                )
                with self.assertRaises(ActivityEvidenceUnavailable) as raised:
                    singleton_worker.fetch_selected_activity_content(
                        [("email", selected_rows[0])]
                    )
                self.assertTrue(raised.exception.transient)
            finally:
                state.close()

    def test_activity_final_guard_writes_full_pair_and_verifies(self):
        selected = activity_row(501, deal_id=1)
        plan = activity_plan([selected], [selected])
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="activities")
                bitrix = ActivityRuntimeBitrix(
                    live={1: ("cat-old", "sub-old", "NEW", "", "")},
                    activity_indexes=[[selected]],
                    details={
                        selected["ID"]: {
                            key: value
                            for key, value in selected.items()
                            if key != "BINDINGS"
                        }
                    },
                )
                worker = make_worker(state, bitrix, directory, plan=plan)
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"), patch.object(
                    worker, "sleep"
                ):
                    worker.process_one_batch()
                self.assertEqual(state.counts(), {"verified": 1})
                self.assertEqual(bitrix.activity_list_count, 3)
                self.assertEqual(len(bitrix.update_commands), 1)
                command = bitrix.update_commands[0]
                self.assertIn(f"fields%5B{CATEGORY_FIELD}%5D", command)
                self.assertIn(f"fields%5B{SUBCATEGORY_FIELD}%5D", command)
            finally:
                state.close()

    def test_category_activity_write_preserves_subcategory(self):
        selected = activity_row(501, deal_id=1)
        plan = activity_plan(
            [selected],
            [selected],
            desired=("cat-new", "sub-old"),
            category_only=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(
                    state,
                    1,
                    desired=("cat-new", "sub-old"),
                    evidence_mode="category_activities",
                )
                bitrix = ActivityRuntimeBitrix(
                    live={1: ("cat-old", "sub-old", "NEW", "", "")},
                    activity_indexes=[[selected]],
                    details={selected["ID"]: selected},
                )
                worker = make_worker(state, bitrix, directory, plan=plan)
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"), patch.object(
                    worker, "sleep"
                ):
                    worker.process_one_batch()
                self.assertEqual(state.counts(), {"verified": 1})
                self.assertEqual(bitrix.live[1][1], "sub-old")
                self.assertNotIn(
                    f"fields%5B{SUBCATEGORY_FIELD}%5D",
                    bitrix.update_commands[0],
                )
            finally:
                state.close()

    def test_final_activity_index_error_isolated_to_one_deal(self):
        selected_by_deal = {
            deal_id: activity_row(500 + deal_id, deal_id=deal_id)
            for deal_id in (1, 2)
        }
        records = []
        for deal_id, selected in selected_by_deal.items():
            records.append(
                (
                    deal_id,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "activities",
                    canonical_activity_evidence(
                        deal_id, [selected], [selected]
                    ),
                    activity_index_guard_fingerprint(
                        TEST_KEY,
                        deal_id,
                        canonical_activity_index(deal_id, [selected]),
                    ),
                    [
                        activity_locator_fingerprint(
                            TEST_KEY, deal_id, "email", selected["ID"]
                        )
                    ],
                )
            )
        plan = approved_plan(records)

        class IsolatedFailureBitrix(ActivityRuntimeBitrix):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.direct_counts = {1: 0, 2: 0}

            def call(self, method, params=None):
                if method == "crm.activity.list":
                    deal_id = int(params["filter"]["BINDINGS"][0]["OWNER_ID"])
                    self.calls.append((method, params))
                    count = self.direct_counts[deal_id]
                    self.direct_counts[deal_id] += 1
                    if deal_id == 1 and count >= 2:
                        raise RuntimeError("ACCESS_DENIED")
                    return [selected_by_deal[deal_id]]
                if method == "batch" and any(
                    command.startswith("crm.activity.list?")
                    for command in (params or {}).get("cmd", {}).values()
                ):
                    self.calls.append((method, params))
                    results = {}
                    errors = {}
                    for name, command in params["cmd"].items():
                        values = parse_qs(urlsplit(command).query)
                        deal_id = int(
                            values["filter[BINDINGS][0][OWNER_ID]"][0]
                        )
                        if deal_id == 1:
                            errors[name] = {"error": "ACCESS_DENIED"}
                        else:
                            results[name] = [selected_by_deal[deal_id]]
                    return {"result": results, "result_error": errors}
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="activities")
                enqueue(state, 2, evidence_mode="activities")
                bitrix = IsolatedFailureBitrix(
                    live={
                        1: ("cat-old", "sub-old", "NEW", "", ""),
                        2: ("cat-old", "sub-old", "NEW", "", ""),
                    },
                    activity_indexes=[[selected_by_deal[1]]],
                    details={
                        selected["ID"]: selected
                        for selected in selected_by_deal.values()
                    },
                )
                worker = make_worker(state, bitrix, directory, plan=plan)
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"), patch.object(
                    worker, "sleep"
                ):
                    worker.process_one_batch()
                self.assertEqual(
                    state.counts(),
                    {"permanent_error": 1, "verified": 1},
                )
                self.assertEqual(len(bitrix.update_commands), 1)
                self.assertIn("id=2", bitrix.update_commands[0])
            finally:
                state.close()

    def test_final_activity_binding_error_isolated_to_one_deal(self):
        selected_by_deal = {
            deal_id: activity_row(600 + deal_id, deal_id=deal_id)
            for deal_id in (1, 2)
        }
        records = []
        for deal_id, selected in selected_by_deal.items():
            records.append(
                (
                    deal_id,
                    ("cat-old", "sub-old"),
                    ("cat-new", "sub-new"),
                    "activities",
                    canonical_activity_evidence(
                        deal_id, [selected], [selected]
                    ),
                    activity_index_guard_fingerprint(
                        TEST_KEY,
                        deal_id,
                        canonical_activity_index(deal_id, [selected]),
                    ),
                    [
                        activity_locator_fingerprint(
                            TEST_KEY, deal_id, "email", selected["ID"]
                        )
                    ],
                )
            )
        plan = approved_plan(records)

        class IsolatedBindingFailureBitrix(ActivityRuntimeBitrix):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.binding_batches = 0

            def call(self, method, params=None):
                params = params or {}
                commands = params.get("cmd", {})
                if method == "crm.activity.list":
                    self.calls.append((method, params))
                    deal_id = int(params["filter"]["BINDINGS"][0]["OWNER_ID"])
                    return self._activity_page(
                        [selected_by_deal[deal_id]],
                        int(params["filter"][">ID"]),
                    )
                if method == "batch" and commands and all(
                    command.startswith("crm.activity.list?")
                    for command in commands.values()
                ):
                    self.calls.append((method, params))
                    results = {}
                    for name, command in commands.items():
                        values = parse_qs(urlsplit(command).query)
                        deal_id = int(
                            values["filter[BINDINGS][0][OWNER_ID]"][0]
                        )
                        results[name] = self._activity_page(
                            [selected_by_deal[deal_id]],
                            int(values["filter[>ID]"][0]),
                        )
                    return {"result": results, "result_error": []}
                if method == "batch" and commands and all(
                    command.startswith("crm.activity.binding.list?")
                    for command in commands.values()
                ):
                    self.binding_batches += 1
                    if self.binding_batches == 5:
                        self.calls.append((method, params))
                        return {
                            "result": {},
                            "result_error": {
                                next(iter(commands)): {
                                    "error": "ACCESS_DENIED"
                                }
                            },
                        }
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="activities")
                enqueue(state, 2, evidence_mode="activities")
                bitrix = IsolatedBindingFailureBitrix(
                    live={
                        1: ("cat-old", "sub-old", "NEW", "", ""),
                        2: ("cat-old", "sub-old", "NEW", "", ""),
                    },
                    activity_indexes=[[]],
                    details={
                        selected["ID"]: selected
                        for selected in selected_by_deal.values()
                    },
                )
                worker = make_worker(state, bitrix, directory, plan=plan)
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"), patch.object(
                    worker, "sleep"
                ):
                    worker.process_one_batch()
                self.assertEqual(
                    state.counts(),
                    {"permanent_error": 1, "verified": 1},
                )
                self.assertEqual(len(bitrix.update_commands), 1)
                self.assertIn("id=2", bitrix.update_commands[0])
            finally:
                state.close()

    def test_activity_body_or_index_race_causes_conflict_without_write(self):
        selected = activity_row(501, deal_id=1)
        changed_body = dict(selected)
        changed_body["DESCRIPTION"] += "changed"
        changed_binding = dict(selected)
        changed_binding["BINDINGS"] = [
            {"OWNER_TYPE_ID": "3", "OWNER_ID": "77"},
            {"OWNER_TYPE_ID": "2", "OWNER_ID": "999"},
        ]
        rebound_index = dict(selected)
        rebound_index["BINDINGS"] = [
            {"OWNER_TYPE_ID": "3", "OWNER_ID": "77"},
            {"OWNER_TYPE_ID": "2", "OWNER_ID": "999"},
        ]
        changed_index = dict(selected)
        changed_index["SUBJECT"] += " changed"
        cases = (
            ("body", [[selected]], changed_body),
            ("get-binding", [[selected]], changed_binding),
            ("binding-between-a-b", [[selected], [rebound_index]], selected),
            (
                "binding-after-b",
                [[selected], [selected], [rebound_index]],
                selected,
            ),
            ("index-between-a-b", [[selected], [changed_index]], selected),
            (
                "index-after-b",
                [[selected], [selected], [changed_index]],
                selected,
            ),
        )
        for name, indexes, detail in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    enqueue(state, 1, evidence_mode="activities")
                    bitrix = ActivityRuntimeBitrix(
                        live={1: ("cat-old", "sub-old", "NEW", "", "")},
                        activity_indexes=indexes,
                        details={selected["ID"]: detail},
                    )
                    worker = make_worker(
                        state,
                        bitrix,
                        directory,
                        plan=activity_plan([selected], [selected]),
                    )
                    worker.metadata_resolved_at = time.time()
                    with patch.object(worker, "wait_for_write_slot"):
                        worker.process_one_batch()
                    self.assertEqual(state.counts(), {"conflict": 1})
                    self.assertEqual(bitrix.update_commands, [])
                finally:
                    state.close()

    def test_activity_index_add_delete_rebind_or_metadata_change_is_conflict(self):
        selected = activity_row(501, deal_id=1)
        added = activity_row(502, deal_id=1)
        rebound = dict(selected)
        rebound["BINDINGS"] = [{"OWNER_TYPE_ID": "3", "OWNER_ID": "77"}]
        metadata = dict(selected)
        metadata["PROVIDER_TYPE_ID"] = "CHANGED"
        cases = (
            ("add", [selected, added]),
            ("delete", []),
            ("rebind", [rebound]),
            ("metadata", [metadata]),
        )
        for name, live_index in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    enqueue(state, 1, evidence_mode="activities")
                    bitrix = ActivityRuntimeBitrix(
                        live={1: ("cat-old", "sub-old", "NEW", "", "")},
                        activity_indexes=[live_index],
                        details={selected["ID"]: selected},
                    )
                    worker = make_worker(
                        state,
                        bitrix,
                        directory,
                        plan=activity_plan([selected], [selected]),
                    )
                    worker.metadata_resolved_at = time.time()
                    with patch.object(worker, "wait_for_write_slot"):
                        worker.process_one_batch()
                    self.assertEqual(state.counts(), {"conflict": 1})
                    self.assertEqual(bitrix.update_commands, [])
                    self.assertFalse(
                        any(
                            method == "batch"
                            and any(
                                command.startswith("crm.activity.get?")
                                for command in params["cmd"].values()
                            )
                            for method, params in bitrix.calls
                        )
                    )
                finally:
                    state.close()

    def test_activity_locator_missing_and_deal_change_are_fail_closed(self):
        selected = activity_row(501, deal_id=1)
        plan = activity_plan([selected], [selected])
        target = desired_fingerprint(TEST_KEY, 1, "cat-new", "sub-new")
        missing_locator = activity_locator_fingerprint(TEST_KEY, 1, "email", 999)
        missing_plan = ApprovedPlan(
            plan.year,
            plan.count,
            plan.transitions,
            plan.desired_modes,
            plan.digest,
            plan.key,
            plan.subcategory_guards,
            plan.activity_index_guards,
            {target: (missing_locator,)},
            {
                target: PlanEntry(
                    transition=next(iter(plan.transitions)),
                    target=target,
                    evidence_mode="activities",
                    activity_index_guard=plan.activity_index_guards[target],
                    activity_locators=(missing_locator,),
                )
            },
        )

        class ChangingDealBitrix(ActivityRuntimeBitrix):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.deal_reads = 0

            def _deal_rows(self, deal_ids):
                self.deal_reads += 1
                if self.deal_reads == 2:
                    old = self.live[1]
                    self.live[1] = ("manual", old[1], old[2], old[3], old[4])
                return super()._deal_rows(deal_ids)

        cases = (
            ("missing-locator", missing_plan, ActivityRuntimeBitrix),
            ("deal-change", plan, ChangingDealBitrix),
        )
        for name, case_plan, bitrix_class in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    enqueue(state, 1, evidence_mode="activities")
                    bitrix = bitrix_class(
                        live={1: ("cat-old", "sub-old", "NEW", "", "")},
                        activity_indexes=[[selected]],
                        details={selected["ID"]: selected},
                    )
                    worker = make_worker(state, bitrix, directory, plan=case_plan)
                    worker.metadata_resolved_at = time.time()
                    with patch.object(worker, "wait_for_write_slot"):
                        worker.process_one_batch()
                    self.assertEqual(state.counts(), {"conflict": 1})
                    self.assertEqual(bitrix.update_commands, [])
                finally:
                    state.close()

    def test_activity_preflight_terminal_states_never_fetch_activity(self):
        selected = activity_row(501, deal_id=1)
        plan = activity_plan([selected], [selected])
        cases = (
            ("excluded", ("cat-old", "sub-old", "DUP", "", ""), "excluded_stage"),
            ("desired", ("cat-new", "sub-new", "NEW", "", ""), "verified"),
            ("changed", ("manual", "sub-old", "NEW", "", ""), "conflict"),
        )
        for name, live, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    enqueue(state, 1, evidence_mode="activities")
                    bitrix = ActivityRuntimeBitrix(
                        live={1: live},
                        activity_indexes=[[selected]],
                        details={selected["ID"]: selected},
                    )
                    worker = make_worker(state, bitrix, directory, plan=plan)
                    worker.excluded_stage_ids = {"DUP"}
                    worker.metadata_resolved_at = time.time()
                    with patch.object(worker, "wait_for_write_slot"):
                        worker.process_one_batch()
                    self.assertEqual(state.counts(), {expected: 1})
                    self.assertFalse(
                        any(method.startswith("crm.activity") for method, _ in bitrix.calls)
                    )
                finally:
                    state.close()

    def test_call_transcript_null_and_activity_partial_error_never_write(self):
        call = activity_row(
            987654321,
            deal_id=1,
            kind="call",
            transcription="SENTINEL-TRANSCRIPT-31b2",
        )
        plan = activity_plan([call], [call])
        cases = (
            ("null-transcript", {}, {}, "conflict"),
            (
                "partial-error",
                {call["ID"]: {"transcription": call["transcription"]}},
                {call["ID"]: {"error": "ACCESS_DENIED"}},
                "permanent_error",
            ),
        )
        for name, transcripts, detail_errors, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state = State(Path(directory) / "worker.sqlite3")
                try:
                    enqueue(state, 1, evidence_mode="activities")
                    bitrix = ActivityRuntimeBitrix(
                        live={1: ("cat-old", "sub-old", "NEW", "", "")},
                        activity_indexes=[[call]],
                        details={call["ID"]: call},
                        transcripts=transcripts,
                        detail_errors=detail_errors,
                    )
                    worker = make_worker(state, bitrix, directory, plan=plan)
                    worker.metadata_resolved_at = time.time()
                    with patch.object(worker, "wait_for_write_slot"):
                        worker.process_one_batch()
                    self.assertEqual(state.counts(), {expected: 1})
                    self.assertEqual(bitrix.update_commands, [])
                    worker.write_status()
                finally:
                    state.close()
                persisted = b"".join(
                    path.read_bytes()
                    for path in Path(directory).iterdir()
                    if path.is_file()
                )
                self.assertNotIn(call["transcription"].encode(), persisted)
                self.assertNotIn(call["ID"].encode(), persisted)

    def test_call_transcript_batch_method_falls_back_without_changing_raw_text(self):
        call = activity_row(
            987654322,
            deal_id=1,
            kind="call",
            transcription="Точный текст расшифровки \r\n",
        )

        class FallbackBitrix(ActivityRuntimeBitrix):
            def call(self, method, params=None):
                if method == "batch" and any(
                    command.startswith("crm.activity.call.getTranscript?")
                    for command in (params or {}).get("cmd", {}).values()
                ):
                    self.calls.append((method, params))
                    return {
                        "result": {},
                        "result_error": {
                            "transcript_0": {
                                "error": "ERROR_BATCH_METHOD_NOT_ALLOWED"
                            }
                        },
                    }
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="activities")
                bitrix = FallbackBitrix(
                    live={1: ("cat-old", "sub-old", "NEW", "", "")},
                    activity_indexes=[[call]],
                    details={call["ID"]: call},
                    transcripts={
                        call["ID"]: {"transcription": call["transcription"]}
                    },
                )
                worker = make_worker(
                    state,
                    bitrix,
                    directory,
                    plan=activity_plan([call], [call]),
                )
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"), patch.object(
                    worker, "sleep"
                ):
                    worker.process_one_batch()
                self.assertEqual(state.counts(), {"verified": 1})
                self.assertTrue(
                    any(
                        method == "crm.activity.call.getTranscript"
                        for method, _ in bitrix.calls
                    )
                )
            finally:
                state.close()

    def test_activity_transport_error_retries_without_persisting_api_payload(self):
        selected = activity_row(987654323, deal_id=1)

        class TimeoutBitrix(ActivityRuntimeBitrix):
            def call(self, method, params=None):
                if method == "crm.activity.list":
                    self.calls.append((method, params))
                    raise TimeoutError("timeout with PRIVATE-PAYLOAD-88d1")
                return super().call(method, params)

        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "worker.sqlite3")
            try:
                enqueue(state, 1, evidence_mode="activities")
                bitrix = TimeoutBitrix(
                    live={1: ("cat-old", "sub-old", "NEW", "", "")},
                    activity_indexes=[[selected]],
                    details={selected["ID"]: selected},
                )
                worker = make_worker(
                    state,
                    bitrix,
                    directory,
                    plan=activity_plan([selected], [selected]),
                )
                worker.metadata_resolved_at = time.time()
                with patch.object(worker, "wait_for_write_slot"):
                    worker.process_one_batch()
                row = state.db.execute(
                    "SELECT status,attempts,last_error FROM queue WHERE deal_id=1"
                ).fetchone()
                self.assertEqual((row["status"], row["attempts"]), ("retry_wait", 1))
                self.assertEqual(row["last_error"], "Activity evidence временно недоступно")
                worker.write_status()
            finally:
                state.close()
            persisted = b"".join(
                path.read_bytes()
                for path in Path(directory).iterdir()
                if path.is_file()
            )
            self.assertNotIn(b"PRIVATE-PAYLOAD-88d1", persisted)


if __name__ == "__main__":
    unittest.main()
