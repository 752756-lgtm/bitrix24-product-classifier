#!/usr/bin/env python3
"""Privacy-safe, strictly read-only live probe for the activity-v5 protocol.

The probe intentionally has no write-capable Bitrix method in its allowlist. It
discovers a small sample of still-unclassified 2025 deals, verifies the real
activity list/get/transcript response shapes, and writes aggregate diagnostics
only. Raw subjects, message bodies, transcripts, deal IDs and activity IDs are
never serialized.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode, urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classifier.bitrix import BitrixClient
from classifier.precision_plan import (
    MAX_RELEVANT_ACTIVITIES,
    activity_is_bound_to_deal,
    activity_kind,
    canonical_activity_evidence,
    canonical_activity_index,
)
from classifier.precision_worker import (
    ACTIVITY_INDEX_FIELDS,
    ACTIVITY_PAGE_SIZE,
    CATEGORY_FIELD,
    EXCLUDED_STAGE_NAMES,
    SUBCATEGORY_FIELD,
    normalize_stage_name,
    normalized,
)


READ_ONLY_METHODS = frozenset(
    {
        "batch",
        "crm.activity.call.getTranscript",
        "crm.activity.get",
        "crm.activity.list",
        "crm.category.list",
        "crm.deal.list",
        "crm.status.list",
    }
)
METHOD_NOT_ALLOWED = "ERROR_BATCH_METHOD_NOT_ALLOWED"
SAFE_CODE_RE = re.compile(r"^[A-Z0-9_]{1,80}$")
MIN_SAFE_INTERVAL_SECONDS = 1.2
MAX_SAFE_DISCOVERY_PAGES = 600
MAX_SAFE_ACTIVITY_PAGES = 20
MAX_SAFE_CONTENT_ROWS = 20
MAX_SAFE_INTERVAL_SECONDS = 10.0
MAX_SAFE_DIRECT_COMPARE = 5


class PilotError(RuntimeError):
    """A deliberately non-sensitive pilot failure."""


class PilotApiError(PilotError):
    def __init__(self, method: str, code: str):
        super().__init__(f"{method} failed ({code})")
        self.method = method
        self.code = code


def _validate_operational_limits(
    *,
    min_interval: float,
    sample_size: int,
    max_discovery_pages: int,
    activity_batch_size: int,
    max_activity_pages: int,
    direct_compare_count: int,
    max_emails: int,
    max_calls: int,
) -> None:
    if not 20 <= sample_size <= 50:
        raise PilotError("Pilot sample size is outside the safe range")
    if not 1 <= activity_batch_size <= 20:
        raise PilotError("Activity batch size is outside the safe range")
    if not 1 <= direct_compare_count <= min(sample_size, MAX_SAFE_DIRECT_COMPARE):
        raise PilotError("Direct compare count is outside the safe range")
    if not 1 <= max_emails <= 20 or not 1 <= max_calls <= 20:
        raise PilotError("Content sample size is outside the safe range")
    if max_emails + max_calls > MAX_SAFE_CONTENT_ROWS:
        raise PilotError("Activity content batch exceeds the safe response-size cap")
    if not 1 <= max_discovery_pages <= MAX_SAFE_DISCOVERY_PAGES:
        raise PilotError("Deal discovery page limit is outside the safe range")
    if not 1 <= max_activity_pages <= MAX_SAFE_ACTIVITY_PAGES:
        raise PilotError("Activity page limit is outside the safe range")
    if (
        not math.isfinite(min_interval)
        or not MIN_SAFE_INTERVAL_SECONDS
        <= min_interval
        <= MAX_SAFE_INTERVAL_SECONDS
    ):
        raise PilotError("API call interval is outside the safe bounds")


def _safe_error_code(value: object) -> str:
    if isinstance(value, dict):
        candidate = str(value.get("error") or "")
    else:
        candidate = ""
        match = re.search(r":\s*([A-Z][A-Z0-9_]{0,79})(?:\s|\u2014|$)", str(value))
        if match:
            candidate = match.group(1)
    candidate = candidate.upper()
    return candidate if SAFE_CODE_RE.fullmatch(candidate) else "API_ERROR"


def _batch_method(command: object) -> str:
    if not isinstance(command, str) or not command:
        raise PilotError("Malformed batch subcommand")
    method = command.split("?", 1)[0]
    if method not in READ_ONLY_METHODS or method == "batch":
        raise PilotError("Non-read-only batch subcommand rejected")
    return method


class ReadOnlyRateLimitedClient:
    """Fail-closed adapter around BitrixClient with API call spacing metrics."""

    def __init__(
        self,
        transport: Any,
        min_interval: float,
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.transport = transport
        self.min_interval = max(0.0, float(min_interval))
        self._sleep = sleep
        self._monotonic = monotonic
        self.started_at: list[float] = []
        self.errors: Counter[tuple[str, str]] = Counter()

    @staticmethod
    def _validate(method: str, params: dict[str, Any] | None) -> None:
        if method not in READ_ONLY_METHODS:
            raise PilotError("Non-read-only Bitrix method rejected")
        if method != "batch":
            return
        commands = (params or {}).get("cmd")
        if not isinstance(commands, dict) or not commands or len(commands) > 50:
            raise PilotError("Malformed batch command set")
        for command in commands.values():
            _batch_method(command)

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._validate(method, params)
        if self.started_at:
            remaining = self.started_at[-1] + self.min_interval - self._monotonic()
            if remaining > 0:
                self._sleep(remaining)
        started = self._monotonic()
        self.started_at.append(started)
        try:
            return self.transport.call(method, params or {})
        except Exception as exc:
            code = _safe_error_code(exc)
            self.errors[(method, code)] += 1
            raise PilotApiError(method, code) from None

    def spacing_report(self) -> dict[str, object]:
        gaps = [
            later - earlier
            for earlier, later in zip(self.started_at, self.started_at[1:])
        ]
        tolerance = min(0.02, self.min_interval * 0.05)
        minimum = min(gaps) if gaps else None
        return {
            "api_call_count": len(self.started_at),
            "configured_min_interval_seconds": self.min_interval,
            "minimum_observed_start_gap_seconds": (
                round(minimum, 4) if minimum is not None else None
            ),
            "spacing_verified": (
                minimum is None or minimum + tolerance >= self.min_interval
            ),
        }


def _rows(value: Any, nested_key: str = "") -> list[dict[str, Any]]:
    if nested_key and isinstance(value, dict):
        value = value.get(nested_key)
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise PilotError("Unexpected list response shape")
    if len(value) > 50:
        raise PilotError("Unexpected Bitrix page size")
    return value


def _batch_parts(value: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, dict):
        raise PilotError("Unexpected batch response shape")
    results = value.get("result") or {}
    errors = value.get("result_error") or {}
    if not isinstance(results, dict) or not isinstance(errors, dict):
        raise PilotError("Unexpected batch response shape")
    return results, errors


def resolve_excluded_stage_ids(client: ReadOnlyRateLimitedClient) -> set[str]:
    category_ids = {0}
    start = 0
    while True:
        value = client.call(
            "crm.category.list", {"entityTypeId": 2, "start": start}
        )
        categories = _rows(value, "categories")
        for row in categories:
            raw = row.get("id", row.get("ID"))
            if raw is not None:
                category_ids.add(int(raw))
        if len(categories) < 50:
            break
        start += len(categories)

    found: dict[str, set[str]] = {name: set() for name in EXCLUDED_STAGE_NAMES}
    for category_id in sorted(category_ids):
        entity_id = "DEAL_STAGE" if category_id == 0 else f"DEAL_STAGE_{category_id}"
        start = 0
        total = 0
        while True:
            statuses = _rows(
                client.call(
                    "crm.status.list",
                    {
                        "filter": {"ENTITY_ID": entity_id},
                        "order": {"SORT": "ASC"},
                        "start": start,
                    },
                )
                or []
            )
            total += len(statuses)
            for row in statuses:
                name = normalize_stage_name(str(row.get("NAME") or ""))
                status_id = str(row.get("STATUS_ID") or "")
                prefix = f"C{category_id}:"
                if category_id and status_id and not status_id.startswith(prefix):
                    status_id = prefix + status_id
                if name in found and status_id:
                    found[name].add(status_id)
            if len(statuses) < 50:
                break
            start += len(statuses)
        if total == 0:
            raise PilotError("A funnel returned no stages")
    if any(not values for values in found.values()):
        raise PilotError("Required excluded stages were not resolved")
    return set().union(*found.values())


def discover_remaining_deals(
    client: ReadOnlyRateLimitedClient,
    *,
    year: int,
    sample_size: int,
    excluded_stage_ids: set[str],
    max_pages: int,
) -> tuple[list[int], dict[str, int]]:
    selected: list[int] = []
    last_id = 0
    pages = 0
    rows_scanned = 0
    excluded_rows = 0
    already_categorized_rows = 0
    while pages < max_pages and len(selected) < sample_size:
        page = _rows(
            client.call(
                "crm.deal.list",
                {
                    "filter": {
                        ">=DATE_CREATE": f"{year}-01-01T00:00:00+03:00",
                        "<DATE_CREATE": f"{year + 1}-01-01T00:00:00+03:00",
                        ">ID": last_id,
                    },
                    "order": {"ID": "ASC"},
                    "select": [
                        "ID",
                        "STAGE_ID",
                        CATEGORY_FIELD,
                        SUBCATEGORY_FIELD,
                    ],
                    "start": 0,
                },
            )
            or []
        )
        pages += 1
        page_last = last_id
        for row in page:
            raw_id = normalized(row.get("ID"))
            if not raw_id.isdigit() or int(raw_id) <= page_last:
                raise PilotError("Deal keyset pagination was not strictly increasing")
            page_last = int(raw_id)
            rows_scanned += 1
            if normalized(row.get("STAGE_ID")) in excluded_stage_ids:
                excluded_rows += 1
                continue
            if normalized(row.get(CATEGORY_FIELD)):
                already_categorized_rows += 1
                continue
            selected.append(page_last)
            if len(selected) == sample_size:
                break
        if len(page) < 50 or len(selected) == sample_size:
            break
        if page_last <= last_id:
            raise PilotError("Deal keyset cursor did not advance")
        last_id = page_last
    if len(selected) != sample_size:
        raise PilotError("The requested remaining-deal sample was not found")
    return selected, {
        "pages_scanned": pages,
        "rows_scanned": rows_scanned,
        "excluded_rows_skipped": excluded_rows,
        "already_categorized_rows_skipped": already_categorized_rows,
    }


def _activity_list_command(deal_id: int, last_id: int) -> str:
    fields = [
        ("filter[BINDINGS][0][OWNER_TYPE_ID]", "2"),
        ("filter[BINDINGS][0][OWNER_ID]", str(deal_id)),
        ("filter[>ID]", str(last_id)),
        ("order[ID]", "ASC"),
        *((f"select[{index}]", field) for index, field in enumerate(ACTIVITY_INDEX_FIELDS)),
        ("start", "0"),
    ]
    return "crm.activity.list?" + urlencode(fields)


def _validate_activity_page(
    value: Any,
    *,
    deal_id: int,
    last_id: int,
) -> tuple[list[dict[str, Any]], int, int]:
    page = _rows(value, "activities")
    relevant: list[dict[str, Any]] = []
    page_last = last_id
    binding_only = 0
    for row in page:
        raw_id = normalized(row.get("ID", row.get("id")))
        if not raw_id.isdigit() or int(raw_id) <= page_last:
            raise PilotError("Activity keyset pagination was not strictly increasing")
        page_last = int(raw_id)
        try:
            bound = activity_is_bound_to_deal(row, deal_id)
        except ValueError:
            raise PilotError("Activity binding response shape was invalid") from None
        if not bound:
            raise PilotError("Activity binding filter returned a foreign row")
        if normalized(row.get("OWNER_TYPE_ID", row.get("ownerTypeId"))) != "2" or normalized(
            row.get("OWNER_ID", row.get("ownerId"))
        ) != str(deal_id):
            binding_only += 1
        if activity_kind(row) is not None:
            try:
                canonical_activity_index(deal_id, [row])
            except ValueError:
                raise PilotError("Relevant activity index shape was invalid") from None
            relevant.append(row)
    return relevant, page_last, binding_only


def _list_one_direct(
    client: ReadOnlyRateLimitedClient,
    deal_id: int,
    *,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    last_id = 0
    relevant: list[dict[str, Any]] = []
    pages = 0
    raw_rows = 0
    binding_only = 0
    while pages < max_pages:
        value = client.call(
            "crm.activity.list",
            {
                "filter": {
                    "BINDINGS": [{"OWNER_TYPE_ID": 2, "OWNER_ID": deal_id}],
                    ">ID": last_id,
                },
                "order": {"ID": "ASC"},
                "select": ACTIVITY_INDEX_FIELDS,
                "start": 0,
            },
        )
        page = _rows(value, "activities")
        page_relevant, page_last, page_binding_only = _validate_activity_page(
            value, deal_id=deal_id, last_id=last_id
        )
        pages += 1
        raw_rows += len(page)
        binding_only += page_binding_only
        relevant.extend(page_relevant)
        if len(relevant) > MAX_RELEVANT_ACTIVITIES:
            raise PilotError("Relevant activity protocol cap was exceeded")
        if len(page) < ACTIVITY_PAGE_SIZE:
            return relevant, {
                "pages": pages,
                "raw_rows": raw_rows,
                "binding_only_rows": binding_only,
            }
        if page_last <= last_id:
            raise PilotError("Activity keyset cursor did not advance")
        last_id = page_last
    raise PilotError("Activity page safety limit was reached")


def list_activities_batched(
    client: ReadOnlyRateLimitedClient,
    deal_ids: list[int],
    *,
    batch_size: int,
    max_pages: int,
) -> tuple[dict[int, list[dict[str, Any]]], dict[str, object]]:
    relevant = {deal_id: [] for deal_id in deal_ids}
    last_ids = {deal_id: 0 for deal_id in deal_ids}
    page_counts = Counter({deal_id: 0 for deal_id in deal_ids})
    pending = list(deal_ids)
    raw_rows = 0
    binding_only = 0
    batch_supported = True
    batch_calls = 0
    while pending:
        next_pending: list[int] = []
        for start in range(0, len(pending), batch_size):
            group = pending[start:start + batch_size]
            commands = {
                f"row_{offset}": _activity_list_command(deal_id, last_ids[deal_id])
                for offset, deal_id in enumerate(group)
            }
            batch_calls += 1
            try:
                results, errors = _batch_parts(
                    client.call("batch", {"halt": 0, "cmd": commands})
                )
            except PilotApiError as exc:
                if exc.code != METHOD_NOT_ALLOWED:
                    raise
                batch_supported = False
                results, errors = {}, {
                    name: {"error": METHOD_NOT_ALLOWED} for name in commands
                }
            for offset, deal_id in enumerate(group):
                name = f"row_{offset}"
                if name in errors:
                    code = _safe_error_code(errors[name])
                    if code != METHOD_NOT_ALLOWED:
                        raise PilotApiError("crm.activity.list", code)
                    batch_supported = False
                    direct, metrics = _list_one_direct(
                        client, deal_id, max_pages=max_pages
                    )
                    relevant[deal_id] = direct
                    page_counts[deal_id] = metrics["pages"]
                    raw_rows += metrics["raw_rows"]
                    binding_only += metrics["binding_only_rows"]
                    continue
                if name not in results:
                    raise PilotError("Batch omitted an activity-list result")
                if page_counts[deal_id] >= max_pages:
                    raise PilotError("Activity page safety limit was reached")
                page = _rows(results[name], "activities")
                rows, page_last, page_binding_only = _validate_activity_page(
                    results[name], deal_id=deal_id, last_id=last_ids[deal_id]
                )
                page_counts[deal_id] += 1
                raw_rows += len(page)
                binding_only += page_binding_only
                relevant[deal_id].extend(rows)
                if len(relevant[deal_id]) > MAX_RELEVANT_ACTIVITIES:
                    raise PilotError("Relevant activity protocol cap was exceeded")
                if len(page) == ACTIVITY_PAGE_SIZE:
                    if page_last <= last_ids[deal_id]:
                        raise PilotError("Activity keyset cursor did not advance")
                    last_ids[deal_id] = page_last
                    next_pending.append(deal_id)
        pending = next_pending

    email_count = sum(
        1 for rows in relevant.values() for row in rows if activity_kind(row) == "email"
    )
    call_count = sum(
        1 for rows in relevant.values() for row in rows if activity_kind(row) == "call"
    )
    return relevant, {
        "batch_activity_list_supported": batch_supported,
        "batch_calls": batch_calls,
        "pages": sum(page_counts.values()),
        "maximum_pages_for_one_deal": max(page_counts.values(), default=0),
        "raw_rows": raw_rows,
        "relevant_rows": email_count + call_count,
        "incoming_email_rows": email_count,
        "call_rows": call_count,
        "rows_found_only_through_binding": binding_only,
        "binding_filter_verified": True,
        "keyset_pagination_verified": True,
    }


def compare_direct_indexes(
    client: ReadOnlyRateLimitedClient,
    activity_indexes: dict[int, list[dict[str, Any]]],
    *,
    count: int,
    max_pages: int,
) -> dict[str, int]:
    checked = 0
    matched = 0
    for deal_id in list(activity_indexes)[:count]:
        direct, _metrics = _list_one_direct(client, deal_id, max_pages=max_pages)
        checked += 1
        if canonical_activity_index(deal_id, direct) == canonical_activity_index(
            deal_id, activity_indexes[deal_id]
        ):
            matched += 1
    return {"direct_compare_count": checked, "direct_compare_matches": matched}


def _select_activity_rows(
    activity_indexes: dict[int, list[dict[str, Any]]],
    *,
    max_emails: int,
    max_calls: int,
) -> list[tuple[int, str, dict[str, Any]]]:
    by_kind: dict[str, list[tuple[int, str, dict[str, Any]]]] = {
        "email": [],
        "call": [],
    }
    for deal_id, rows in activity_indexes.items():
        for row in rows:
            kind = activity_kind(row)
            if kind in by_kind:
                by_kind[kind].append((deal_id, kind, row))
    return by_kind["email"][:max_emails] + by_kind["call"][:max_calls]


def fetch_activity_details(
    client: ReadOnlyRateLimitedClient,
    selected: list[tuple[int, str, dict[str, Any]]],
) -> tuple[list[tuple[int, str, dict[str, Any], dict[str, Any]]], dict[str, object]]:
    if not selected:
        return [], {
            "selected_rows": 0,
            "batch_activity_get_supported": None,
            "valid_id_matches": 0,
            "email_body_field_present": 0,
            "email_body_nonempty": 0,
            "get_bindings_present": 0,
            "get_bindings_valid": 0,
            "direct_snapshot_compare": None,
        }
    commands = {
        f"detail_{offset}": "crm.activity.get?" + urlencode(
            [("id", normalized(row.get("ID", row.get("id"))))]
        )
        for offset, (_deal_id, _kind, row) in enumerate(selected)
    }
    batch_supported = True
    try:
        results, errors = _batch_parts(
            client.call("batch", {"halt": 0, "cmd": commands})
        )
    except PilotApiError as exc:
        if exc.code != METHOD_NOT_ALLOWED:
            raise
        batch_supported = False
        results, errors = {}, {
            name: {"error": METHOD_NOT_ALLOWED} for name in commands
        }

    fetched: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
    id_matches = 0
    body_present = 0
    body_nonempty = 0
    bindings_present = 0
    bindings_valid = 0
    for offset, (deal_id, kind, index_row) in enumerate(selected):
        name = f"detail_{offset}"
        if name in errors:
            code = _safe_error_code(errors[name])
            if code != METHOD_NOT_ALLOWED:
                raise PilotApiError("crm.activity.get", code)
            batch_supported = False
            detail = client.call(
                "crm.activity.get",
                {"id": normalized(index_row.get("ID", index_row.get("id")))},
            )
        else:
            detail = results.get(name)
        expected_id = normalized(index_row.get("ID", index_row.get("id")))
        if not isinstance(detail, dict):
            raise PilotError("Unexpected activity-get response shape")
        if normalized(detail.get("ID", detail.get("id"))) != expected_id:
            raise PilotError("Activity-get ID did not match the request")
        id_matches += 1
        if "BINDINGS" in detail or "bindings" in detail:
            bindings_present += 1
            try:
                if activity_is_bound_to_deal(detail, deal_id):
                    bindings_valid += 1
                else:
                    raise PilotError("Activity-get bindings changed deal")
            except ValueError:
                raise PilotError("Activity-get bindings were malformed") from None
        if kind == "email":
            if "DESCRIPTION" in detail or "description" in detail:
                body_present += 1
                raw_body = detail.get("DESCRIPTION", detail.get("description"))
                if raw_body is not None and raw_body is not False and str(raw_body).strip():
                    body_nonempty += 1
            if activity_kind(detail) != "email":
                raise PilotError("Activity-get changed incoming-email scope")
        elif activity_kind(detail) != "call":
            raise PilotError("Activity-get changed call scope")
        probe_detail = dict(detail)
        if "SUBJECT" in probe_detail:
            probe_detail["SUBJECT"] = "shape-probe"
        elif "subject" in probe_detail:
            probe_detail["subject"] = "shape-probe"
        if kind == "email":
            if "DESCRIPTION" in probe_detail:
                probe_detail["DESCRIPTION"] = "shape-probe"
            elif "description" in probe_detail:
                probe_detail["description"] = "shape-probe"
        selected_probe: dict[str, object] = {
            "kind": kind,
            "activity": probe_detail,
        }
        if kind == "call":
            selected_probe["transcription"] = "shape-probe"
        try:
            # Reuse the production v5 canonicalizer so the pilot cannot report
            # ready when crm.activity.get omits a required metadata field or
            # supplies bindings that disagree with the signed list snapshot.
            canonical_activity_evidence(
                deal_id,
                [index_row],
                [selected_probe],
            )
        except ValueError:
            raise PilotError(
                "Activity-get snapshot was incompatible with activity-v5"
            ) from None
        fetched.append((deal_id, kind, index_row, detail))

    direct_compare: bool | None = None
    if fetched:
        _deal_id, _kind, index_row, first = fetched[0]
        direct = client.call(
            "crm.activity.get",
            {"id": normalized(index_row.get("ID", index_row.get("id")))},
        )
        direct_compare = isinstance(direct, dict) and direct == first
    return fetched, {
        "selected_rows": len(selected),
        "batch_activity_get_supported": batch_supported,
        "valid_id_matches": id_matches,
        "email_body_field_present": body_present,
        "email_body_nonempty": body_nonempty,
        "get_bindings_present": bindings_present,
        "get_bindings_valid": bindings_valid,
        "direct_snapshot_compare": direct_compare,
    }


def _transcript_nonempty(value: Any) -> bool:
    if value is None or value is False:
        return False
    if not isinstance(value, dict):
        raise PilotError("Unexpected transcript response shape")
    if "transcription" not in value:
        raise PilotError("Unexpected transcript response shape")
    text = value.get("transcription")
    if text is None or text is False:
        return False
    if not isinstance(text, str):
        raise PilotError("Unexpected transcript response shape")
    return bool(text.strip())


def probe_transcripts(
    client: ReadOnlyRateLimitedClient,
    fetched: list[tuple[int, str, dict[str, Any], dict[str, Any]]],
) -> dict[str, object]:
    calls = [item for item in fetched if item[1] == "call"]
    if not calls:
        return {
            "selected_calls": 0,
            "batch_transcript_supported": None,
            "responses_received": 0,
            "nonempty_call_texts": 0,
            "null_call_texts": 0,
            "direct_result_compare": None,
        }
    commands = {
        f"call_{offset}": "crm.activity.call.getTranscript?" + urlencode(
            [("activityId", normalized(index_row.get("ID", index_row.get("id"))))]
        )
        for offset, (_deal_id, _kind, index_row, _detail) in enumerate(calls)
    }
    batch_supported = True
    try:
        results, errors = _batch_parts(
            client.call("batch", {"halt": 0, "cmd": commands})
        )
    except PilotApiError as exc:
        if exc.code != METHOD_NOT_ALLOWED:
            raise
        batch_supported = False
        results, errors = {}, {
            name: {"error": METHOD_NOT_ALLOWED} for name in commands
        }
    values: list[Any] = []
    for offset, (_deal_id, _kind, index_row, _detail) in enumerate(calls):
        name = f"call_{offset}"
        if name in errors:
            code = _safe_error_code(errors[name])
            if code != METHOD_NOT_ALLOWED:
                raise PilotApiError("crm.activity.call.getTranscript", code)
            batch_supported = False
            value = client.call(
                "crm.activity.call.getTranscript",
                {
                    "activityId": normalized(
                        index_row.get("ID", index_row.get("id"))
                    )
                },
            )
        else:
            if name not in results:
                raise PilotError("Batch omitted a transcript result")
            value = results[name]
        _transcript_nonempty(value)
        values.append(value)

    _deal_id, _kind, first_index, _detail = calls[0]
    direct = client.call(
        "crm.activity.call.getTranscript",
        {"activityId": normalized(first_index.get("ID", first_index.get("id")))},
    )
    _transcript_nonempty(direct)
    direct_compare = direct == values[0]
    nonempty = sum(1 for value in values if _transcript_nonempty(value))
    return {
        "selected_calls": len(calls),
        "batch_transcript_supported": batch_supported,
        "responses_received": len(values),
        "nonempty_call_texts": nonempty,
        "null_call_texts": len(values) - nonempty,
        "direct_result_compare": direct_compare,
    }


def _private_write(path: Path, value: dict[str, object]) -> None:
    if not path.is_absolute():
        raise PilotError("Pilot report path must be absolute")
    tmp_root = Path("/tmp").resolve(strict=True)
    try:
        resolved_parent = path.parent.resolve(strict=True)
        relative_parent = resolved_parent.relative_to(tmp_root)
    except (OSError, ValueError):
        raise PilotError("Pilot report parent must already exist under /tmp") from None
    if path.name in {"", ".", ".."}:
        raise PilotError("Pilot report filename is invalid")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_fd = os.open(tmp_root, directory_flags)
    try:
        for part in relative_parent.parts:
            previous_fd = directory_fd
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            directory_fd = next_fd
            os.close(previous_fd)
    except Exception:
        os.close(directory_fd)
        raise PilotError("Pilot report parent was not a safe directory") from None

    temporary_name = f".{path.name}.{os.getpid()}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)


def _assert_aggregate_report(value: object) -> None:
    forbidden_keys = {
        "comments",
        "description",
        "subject",
        "title",
        "transcription",
    }

    def visit(item: object) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                if str(key).casefold() in forbidden_keys:
                    raise PilotError("Private evidence key reached the aggregate report")
                visit(nested)
        elif isinstance(item, list):
            for nested in item:
                visit(nested)
        elif (
            isinstance(item, str)
            and "https://" in item.casefold()
            and "/rest/" in item.casefold()
        ):
            raise PilotError("Webhook-like value reached the aggregate report")

    visit(value)


def run_pilot(
    client: ReadOnlyRateLimitedClient,
    *,
    year: int,
    sample_size: int,
    max_discovery_pages: int,
    activity_batch_size: int,
    max_activity_pages: int,
    direct_compare_count: int,
    max_emails: int,
    max_calls: int,
) -> dict[str, object]:
    _validate_operational_limits(
        min_interval=client.min_interval,
        sample_size=sample_size,
        max_discovery_pages=max_discovery_pages,
        activity_batch_size=activity_batch_size,
        max_activity_pages=max_activity_pages,
        direct_compare_count=direct_compare_count,
        max_emails=max_emails,
        max_calls=max_calls,
    )
    excluded = resolve_excluded_stage_ids(client)
    deal_ids, discovery = discover_remaining_deals(
        client,
        year=year,
        sample_size=sample_size,
        excluded_stage_ids=excluded,
        max_pages=max_discovery_pages,
    )
    indexes, list_metrics = list_activities_batched(
        client,
        deal_ids,
        batch_size=activity_batch_size,
        max_pages=max_activity_pages,
    )
    list_metrics.update(
        compare_direct_indexes(
            client,
            indexes,
            count=direct_compare_count,
            max_pages=max_activity_pages,
        )
    )
    selected = _select_activity_rows(
        indexes, max_emails=max_emails, max_calls=max_calls
    )
    fetched, get_metrics = fetch_activity_details(client, selected)
    transcript_metrics = probe_transcripts(client, fetched)

    reasons: list[str] = []
    if list_metrics["direct_compare_matches"] != list_metrics["direct_compare_count"]:
        reasons.append("direct_and_batch_activity_indexes_differed")
    if int(list_metrics["incoming_email_rows"]) == 0:
        reasons.append("no_incoming_email_in_sample")
    if int(list_metrics["call_rows"]) == 0:
        reasons.append("no_call_in_sample")
    if int(get_metrics["email_body_field_present"]) == 0:
        reasons.append("email_body_shape_not_observed")
    if int(transcript_metrics["selected_calls"]) == 0:
        reasons.append("transcript_method_not_observed")
    if get_metrics["direct_snapshot_compare"] is False:
        reasons.append("activity_changed_during_direct_compare")
    if transcript_metrics["direct_result_compare"] is False:
        reasons.append("transcript_availability_changed_during_compare")
    spacing = client.spacing_report()
    if not spacing["spacing_verified"]:
        reasons.append("api_spacing_not_verified")

    safe_errors: dict[str, dict[str, int]] = {}
    for (method, code), count in sorted(client.errors.items()):
        safe_errors.setdefault(method, {})[code] = count
    return {
        "schema": "bitrix24-activity-live-pilot-v1",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "year": year,
            "sample_size": len(deal_ids),
            "remaining_definition": "category_blank_and_stage_allowed",
        },
        "safety": {
            "read_only": True,
            "raw_text_persisted": False,
            "deal_or_activity_ids_persisted": False,
            "allowed_methods": sorted(READ_ONLY_METHODS),
        },
        "discovery": discovery,
        "activity_list": list_metrics,
        "activity_get": get_metrics,
        "call_text_probe": transcript_metrics,
        "rate_limit": spacing,
        "api_errors": safe_errors,
        "verdict": {
            "pilot_complete": True,
            "ready_for_limited_private_activity_plan": not reasons,
            "inconclusive_or_blocking_reasons": reasons,
        },
    }


def _valid_webhook(value: str) -> bool:
    parsed = urlsplit(value)
    return parsed.scheme == "https" and bool(parsed.netloc) and "/rest/" in parsed.path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Strictly read-only Bitrix24 activity-v5 live pilot"
    )
    parser.add_argument("--confirm-writer-terminal", action="store_true")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--max-discovery-pages", type=int, default=100)
    parser.add_argument("--activity-batch-size", type=int, default=20)
    parser.add_argument("--max-activity-pages", type=int, default=20)
    parser.add_argument("--direct-compare-count", type=int, default=3)
    parser.add_argument("--max-emails", type=int, default=10)
    parser.add_argument("--max-calls", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--min-interval", type=float, default=1.2)
    parser.add_argument(
        "--output", default="/tmp/activity-v5-live-pilot-report.json"
    )
    args = parser.parse_args()

    if not args.confirm_writer_terminal:
        parser.error("--confirm-writer-terminal is required")
    if not 1 <= args.timeout <= 180:
        parser.error("--timeout must be between 1 and 180 seconds")
    try:
        _validate_operational_limits(
            min_interval=args.min_interval,
            sample_size=args.sample_size,
            max_discovery_pages=args.max_discovery_pages,
            activity_batch_size=args.activity_batch_size,
            max_activity_pages=args.max_activity_pages,
            direct_compare_count=args.direct_compare_count,
            max_emails=args.max_emails,
            max_calls=args.max_calls,
        )
    except PilotError as exc:
        parser.error(str(exc))

    webhook = os.getenv("BITRIX_WEBHOOK_URL", "").strip()
    if not webhook or not _valid_webhook(webhook):
        parser.error("BITRIX_WEBHOOK_URL is missing or invalid")

    client = ReadOnlyRateLimitedClient(
        BitrixClient(webhook, timeout=args.timeout), args.min_interval
    )
    try:
        report = run_pilot(
            client,
            year=args.year,
            sample_size=args.sample_size,
            max_discovery_pages=args.max_discovery_pages,
            activity_batch_size=args.activity_batch_size,
            max_activity_pages=args.max_activity_pages,
            direct_compare_count=args.direct_compare_count,
            max_emails=args.max_emails,
            max_calls=args.max_calls,
        )
        _assert_aggregate_report(report)
        _private_write(Path(args.output), report)
    except (PilotError, ValueError, OSError):
        print("activity-v5 pilot failed; no private evidence was printed", file=sys.stderr)
        return 2
    except Exception:
        print("activity-v5 pilot failed; no private evidence was printed", file=sys.stderr)
        return 2

    summary = {
        "sample_size": report["scope"]["sample_size"],
        "relevant_rows": report["activity_list"]["relevant_rows"],
        "incoming_email_rows": report["activity_list"]["incoming_email_rows"],
        "call_rows": report["activity_list"]["call_rows"],
        "email_bodies_observed": report["activity_get"]["email_body_nonempty"],
        "call_texts_observed": report["call_text_probe"]["nonempty_call_texts"],
        "ready": report["verdict"]["ready_for_limited_private_activity_plan"],
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    print("private aggregate report written")
    return 0 if summary["ready"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
