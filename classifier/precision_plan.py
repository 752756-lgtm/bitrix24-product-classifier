from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


PLAN_FORMAT_V2 = "bitrix24-precision-allowlist-v2"
PLAN_FORMAT_V3 = "bitrix24-precision-allowlist-v3"
PLAN_FORMAT_V4 = "bitrix24-precision-allowlist-v4"
PLAN_FORMAT = "bitrix24-precision-allowlist-v5"
PLAN_VERSION = 5
FINGERPRINT_RE = re.compile(r"^[0-9a-f]{32}$")
_DOMAIN = b"bitrix24-precision-plan-v1"
DEAL_TEXT_CANON_VERSION = "bitrix24-deal-text-v1"
ACTIVITY_INDEX_CANON_VERSION = "bitrix24-activity-index-v1"
ACTIVITY_EVIDENCE_CANON_VERSION = "bitrix24-activity-evidence-v1"
MAX_RELEVANT_ACTIVITIES = 100
MAX_SELECTED_ACTIVITIES = 8
MAX_SELECTED_ACTIVITY_BYTES = 512 * 1024
EVIDENCE_MODES_V2 = frozenset({"fields", "title", "products"})
EVIDENCE_MODES_V3 = EVIDENCE_MODES_V2 | {"deal_text"}
EVIDENCE_MODES_V4 = EVIDENCE_MODES_V3 | frozenset(
    {
        "category_fields",
        "category_title",
        "category_products",
        "category_deal_text",
    }
)
ACTIVITY_EVIDENCE_MODES = frozenset({"activities", "category_activities"})
CATEGORY_ONLY_EVIDENCE_MODES = frozenset(
    mode for mode in EVIDENCE_MODES_V4 if mode.startswith("category_")
) | {"category_activities"}
EVIDENCE_MODES = EVIDENCE_MODES_V4 | ACTIVITY_EVIDENCE_MODES
FORBIDDEN_CATEGORY_IDS = frozenset({"6901"})
PLAN_PROTOCOLS = {
    (PLAN_FORMAT_V2, 2): EVIDENCE_MODES_V2,
    (PLAN_FORMAT_V3, 3): EVIDENCE_MODES_V3,
    (PLAN_FORMAT_V4, 4): EVIDENCE_MODES_V4,
    (PLAN_FORMAT, PLAN_VERSION): EVIDENCE_MODES,
}


def is_category_only_mode(evidence_mode: str) -> bool:
    return evidence_mode in CATEGORY_ONLY_EVIDENCE_MODES


def is_activity_mode(evidence_mode: str) -> bool:
    return evidence_mode in ACTIVITY_EVIDENCE_MODES


def base_evidence_mode(evidence_mode: str) -> str:
    if evidence_mode == "category_activities":
        return "activities"
    if is_category_only_mode(evidence_mode):
        return evidence_mode.removeprefix("category_")
    return evidence_mode


def derive_plan_key(secret: str, portal_id: str = "") -> bytes:
    """Derive a portal-bound, domain-separated key without retaining secrets."""
    value = secret.strip().rstrip("/").encode("utf-8")
    if not value:
        raise ValueError("Пустой ключ плана")
    portal = portal_id.strip().encode("ascii")
    if not portal:
        raise ValueError("Пустой идентификатор портала")
    return hashlib.sha256(_DOMAIN + b"\0key\0" + portal + b"\0" + value).digest()


def portal_identity(webhook_url: str) -> str:
    """Return a non-secret identity used to prevent cross-portal state reuse."""
    hostname = (urlsplit(webhook_url).hostname or "").casefold()
    if not hostname:
        raise ValueError("Некорректный BITRIX_WEBHOOK_URL")
    return hashlib.sha256(hostname.encode("utf-8")).hexdigest()


def _decimal_text(value: object) -> str:
    try:
        number = Decimal(str(value if value not in (None, "") else 0))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Некорректное число в товарной строке: {value}") from exc
    if not number.is_finite():
        raise ValueError("Неконечное число в товарной строке")
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def canonical_product_evidence(rows: list[dict[str, object]]) -> str:
    """Canonicalize only fields that influenced the approved product classification."""
    values: list[tuple[str, str, str, str]] = []
    for row in rows:
        product_id = row.get("product_id", row.get("productId", row.get("PRODUCT_ID", "")))
        name = row.get("name", row.get("productName", row.get("PRODUCT_NAME", "")))
        price = row.get("price", row.get("PRICE", 0))
        quantity = row.get("quantity", row.get("QUANTITY", 0))
        values.append(
            (
                str(product_id if product_id is not None else ""),
                str(name if name is not None else ""),
                _decimal_text(price),
                _decimal_text(quantity),
            )
        )
    values.sort()
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def canonical_deal_text_evidence(title: object, comments: object) -> str:
    """Preserve the exact REST representation of standard deal text fields."""

    def value(raw: object) -> str:
        return "" if raw is None or raw is False else str(raw)

    return json.dumps(
        [DEAL_TEXT_CANON_VERSION, value(title), value(comments)],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _raw_text(value: object) -> str:
    return "" if value is None or value is False else str(value)


def _scalar_string(value: object) -> str:
    """Preserve the distinction between REST scalars outside raw text fields."""
    return str(value)


def _activity_field(row: dict[str, Any], *names: str) -> object:
    for name in names:
        if name in row:
            return row[name]
    return ""


def _activity_has_field(row: dict[str, Any], *names: str) -> bool:
    return any(name in row for name in names)


def canonical_activity_bindings(value: object) -> list[list[str]]:
    if not isinstance(value, list):
        raise ValueError("Некорректные bindings activity")
    bindings: set[tuple[str, str]] = set()
    for binding in value:
        if not isinstance(binding, dict):
            raise ValueError("Некорректные bindings activity")
        entity_type = _scalar_string(
            _activity_field(binding, "OWNER_TYPE_ID", "ownerTypeId", "entityTypeId")
        )
        entity_id = _scalar_string(
            _activity_field(binding, "OWNER_ID", "ownerId", "entityId")
        )
        if not entity_type.isdigit() or not entity_id.isdigit():
            raise ValueError("Некорректные bindings activity")
        bindings.add((entity_type, entity_id))
    return [
        [entity_type, entity_id]
        for entity_type, entity_id in sorted(
            bindings,
            key=lambda item: (int(item[0]), int(item[1])),
        )
    ]


def activity_kind(row: dict[str, Any]) -> str | None:
    activity_type = _scalar_string(_activity_field(row, "TYPE_ID", "typeId"))
    direction = _scalar_string(_activity_field(row, "DIRECTION", "direction"))
    if activity_type == "4" and direction == "1":
        return "email"
    if activity_type == "2" and direction in {"1", "2"}:
        return "call"
    return None


def activity_is_bound_to_deal(row: dict[str, Any], deal_id: int) -> bool:
    expected = ["2", str(int(deal_id))]
    return expected in canonical_activity_bindings(
        _activity_field(row, "BINDINGS", "bindings")
    )


def _canonical_activity_metadata(row: dict[str, Any]) -> list[object]:
    required_aliases = (
        ("ID", "id"),
        ("OWNER_TYPE_ID", "ownerTypeId"),
        ("OWNER_ID", "ownerId"),
        ("TYPE_ID", "typeId"),
        ("DIRECTION", "direction"),
        ("PROVIDER_ID", "providerId"),
        ("PROVIDER_TYPE_ID", "providerTypeId"),
        ("SUBJECT", "subject"),
        ("DESCRIPTION_TYPE", "descriptionType"),
        ("CREATED", "created"),
        ("LAST_UPDATED", "lastUpdated"),
        ("START_TIME", "startTime"),
        ("END_TIME", "endTime"),
        ("COMPLETED", "completed"),
        ("BINDINGS", "bindings"),
    )
    if any(not _activity_has_field(row, *aliases) for aliases in required_aliases):
        raise ValueError("Activity snapshot не содержит обязательные поля")
    activity_id = _scalar_string(_activity_field(row, "ID", "id"))
    if not activity_id.isdigit() or int(activity_id) <= 0:
        raise ValueError("Некорректный ID activity")
    return [
        activity_id,
        _scalar_string(_activity_field(row, "OWNER_TYPE_ID", "ownerTypeId")),
        _scalar_string(_activity_field(row, "OWNER_ID", "ownerId")),
        _scalar_string(_activity_field(row, "TYPE_ID", "typeId")),
        _scalar_string(_activity_field(row, "DIRECTION", "direction")),
        _scalar_string(_activity_field(row, "PROVIDER_ID", "providerId")),
        _scalar_string(_activity_field(row, "PROVIDER_TYPE_ID", "providerTypeId")),
        _raw_text(_activity_field(row, "SUBJECT", "subject")),
        _scalar_string(_activity_field(row, "DESCRIPTION_TYPE", "descriptionType")),
        _scalar_string(_activity_field(row, "CREATED", "created")),
        _scalar_string(_activity_field(row, "LAST_UPDATED", "lastUpdated")),
        _scalar_string(_activity_field(row, "START_TIME", "startTime")),
        _scalar_string(_activity_field(row, "END_TIME", "endTime")),
        _scalar_string(_activity_field(row, "COMPLETED", "completed")),
        canonical_activity_bindings(_activity_field(row, "BINDINGS", "bindings")),
    ]


def _activity_rows_from_index(value: object, deal_id: int) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("Некорректный activity index")
    if (
        len(value) == 3
        and value[0] == ACTIVITY_INDEX_CANON_VERSION
        and _scalar_string(value[1]) == str(int(deal_id))
    ):
        canonical_rows = value[2]
        if not isinstance(canonical_rows, list):
            raise ValueError("Некорректный activity index")
        result: list[dict[str, Any]] = []
        names = (
            "ID", "OWNER_TYPE_ID", "OWNER_ID", "TYPE_ID", "DIRECTION",
            "PROVIDER_ID", "PROVIDER_TYPE_ID", "SUBJECT", "DESCRIPTION_TYPE",
            "CREATED", "LAST_UPDATED", "START_TIME", "END_TIME", "COMPLETED",
            "BINDINGS",
        )
        for raw_row in canonical_rows:
            if not isinstance(raw_row, list) or len(raw_row) != len(names):
                raise ValueError("Некорректный activity index")
            bindings = raw_row[-1]
            if not isinstance(bindings, list):
                raise ValueError("Некорректный activity index")
            converted_bindings = []
            for binding in bindings:
                if not isinstance(binding, list) or len(binding) != 2:
                    raise ValueError("Некорректный activity index")
                converted_bindings.append(
                    {"OWNER_TYPE_ID": binding[0], "OWNER_ID": binding[1]}
                )
            row = dict(zip(names, raw_row, strict=True))
            row["BINDINGS"] = converted_bindings
            result.append(row)
        return result
    if any(not isinstance(row, dict) for row in value):
        raise ValueError("Некорректный activity index")
    return list(value)


def canonical_activity_index_value(deal_id: int, activities: object) -> list[object]:
    deal = int(deal_id)
    rows = _activity_rows_from_index(activities, deal)
    if len(rows) > MAX_RELEVANT_ACTIVITIES:
        raise ValueError("Превышен лимит relevant activities")
    by_id: dict[str, list[object]] = {}
    for row in rows:
        if activity_kind(row) is None or not activity_is_bound_to_deal(row, deal):
            raise ValueError("Activity не входит в разрешённый scope")
        canonical = _canonical_activity_metadata(row)
        activity_id = str(canonical[0])
        previous = by_id.get(activity_id)
        if previous is not None and previous != canonical:
            raise ValueError("Противоречивые метаданные activity")
        by_id[activity_id] = canonical
    ordered = [by_id[key] for key in sorted(by_id, key=int)]
    return [ACTIVITY_INDEX_CANON_VERSION, str(deal), ordered]


def canonical_activity_index(deal_id: int, activities: object) -> str:
    return json.dumps(
        canonical_activity_index_value(deal_id, activities),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _selected_activity_payload(value: object) -> tuple[str | None, dict[str, Any], object]:
    if not isinstance(value, dict):
        raise ValueError("Некорректный selected activity")
    nested = value.get("activity")
    if nested is not None:
        if not isinstance(nested, dict):
            raise ValueError("Некорректный selected activity")
        row = nested
    else:
        row = value
    kind_value = value.get("kind")
    kind = str(kind_value) if kind_value is not None else None
    transcript = value.get("transcription", row.get("transcription"))
    return kind, row, transcript


def selected_activity_identity(value: object) -> tuple[str, str]:
    declared_kind, row, _transcript = _selected_activity_payload(value)
    kind = activity_kind(row)
    activity_id = _scalar_string(_activity_field(row, "ID", "id"))
    if (
        kind is None
        or (declared_kind is not None and declared_kind != kind)
        or not activity_id.isdigit()
        or int(activity_id) <= 0
    ):
        raise ValueError("Selected activity имеет некорректный тип или ID")
    return kind, activity_id


def canonical_selected_activity(deal_id: int, value: object) -> tuple[str, str, list[object]]:
    declared_kind, row, transcript = _selected_activity_payload(value)
    kind, activity_id = selected_activity_identity(value)
    if not activity_is_bound_to_deal(row, deal_id):
        raise ValueError("Selected activity не привязана к сделке")
    metadata = _canonical_activity_metadata(row)
    if str(metadata[0]) != activity_id:
        raise ValueError("Selected activity имеет некорректный ID")
    common = metadata[:8]
    timestamps = metadata[9:14]
    bindings = metadata[14]
    if kind == "email":
        if not _activity_has_field(row, "DESCRIPTION", "description"):
            raise ValueError("Selected email не содержит DESCRIPTION")
        description = _raw_text(_activity_field(row, "DESCRIPTION", "description"))
        selected = [
            "email-v1", *common,
            description,
            metadata[8],
            *timestamps,
            bindings,
        ]
    else:
        transcription = _raw_text(transcript)
        if not transcription.strip():
            raise ValueError("У selected call нет расшифровки")
        selected = [
            "call-v1", *common,
            transcription,
            *timestamps,
            bindings,
        ]
    return kind, activity_id, selected


def canonical_activity_evidence(
    deal_id: int,
    activity_index: object,
    selected_activities: object,
) -> str:
    index_value = canonical_activity_index_value(deal_id, activity_index)
    if not isinstance(selected_activities, list):
        raise ValueError("Некорректный список selected activities")
    if not 1 <= len(selected_activities) <= MAX_SELECTED_ACTIVITIES:
        raise ValueError("Некорректное число selected activities")
    indexed_kinds = {
        str(row[0]): (
            "email"
            if str(row[3]) == "4" and str(row[4]) == "1"
            else "call"
        )
        for row in index_value[2]
    }
    indexed_bindings = {str(row[0]): row[14] for row in index_value[2]}
    selected: list[tuple[str, str, list[object]]] = []
    seen_ids: set[str] = set()
    for raw in selected_activities:
        kind, activity_id = selected_activity_identity(raw)
        bindings = indexed_bindings.get(activity_id)
        if bindings is None:
            raise ValueError("Selected activity отсутствует в index или дублируется")
        declared_kind, selected_row, transcript = _selected_activity_payload(raw)
        selected_row = dict(selected_row)
        # crm.activity.get does not consistently include BINDINGS. The full
        # signed index is the authoritative binding snapshot, while every
        # other selected field remains from the single get response.
        index_binding_dicts = [
            {"OWNER_TYPE_ID": binding[0], "OWNER_ID": binding[1]}
            for binding in bindings
        ]
        bindings_present = "BINDINGS" in selected_row or "bindings" in selected_row
        supplied_bindings = (
            selected_row.get("BINDINGS")
            if "BINDINGS" in selected_row
            else selected_row.get("bindings")
        )
        if bindings_present and canonical_activity_bindings(
            supplied_bindings
        ) != bindings:
            raise ValueError("Selected activity bindings не совпадают с index")
        selected_row["BINDINGS"] = index_binding_dicts
        selected_value: dict[str, object] = {
            "kind": declared_kind or kind,
            "activity": selected_row,
        }
        if transcript is not None:
            selected_value["transcription"] = transcript
        kind, activity_id, canonical = canonical_selected_activity(
            deal_id,
            selected_value,
        )
        if (
            indexed_kinds.get(activity_id) != kind
            or activity_id in seen_ids
        ):
            raise ValueError("Selected activity отсутствует в index или дублируется")
        seen_ids.add(activity_id)
        selected.append((kind, activity_id, canonical))
    selected.sort(key=lambda item: (item[0], int(item[1])))
    selected_values = [item[2] for item in selected]
    selected_bytes = len(
        json.dumps(
            selected_values,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    if selected_bytes > MAX_SELECTED_ACTIVITY_BYTES:
        raise ValueError("Превышен лимит selected activity evidence")
    return json.dumps(
        [
            ACTIVITY_EVIDENCE_CANON_VERSION,
            str(int(deal_id)),
            index_value,
            selected_values,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _fingerprint(key: bytes, purpose: bytes, parts: tuple[object, ...]) -> str:
    payload = purpose + b"\0" + json.dumps(
        [str(part) for part in parts],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hmac.new(key, _DOMAIN + b"\0" + payload, hashlib.sha256).hexdigest()[:32]


def transition_fingerprint(
    key: bytes,
    deal_id: int,
    original_category: str,
    original_subcategory: str,
    desired_category: str,
    desired_subcategory: str,
    evidence_mode: str = "fields",
    evidence: str = "",
) -> str:
    if evidence_mode not in EVIDENCE_MODES:
        raise ValueError(f"Неподдерживаемый источник плана: {evidence_mode}")
    return _fingerprint(
        key,
        b"transition",
        (
            int(deal_id),
            str(original_category),
            str(original_subcategory),
            str(desired_category),
            str(desired_subcategory),
            evidence_mode,
            str(evidence),
        ),
    )


def desired_fingerprint(
    key: bytes,
    deal_id: int,
    desired_category: str,
    desired_subcategory: str,
) -> str:
    return _fingerprint(
        key,
        b"desired",
        (int(deal_id), str(desired_category), str(desired_subcategory)),
    )


def category_desired_fingerprint(
    key: bytes,
    deal_id: int,
    desired_category: str,
) -> str:
    return _fingerprint(
        key,
        b"category-desired",
        (int(deal_id), str(desired_category)),
    )


def subcategory_guard_fingerprint(
    key: bytes,
    deal_id: int,
    original_subcategory: str,
) -> str:
    return _fingerprint(
        key,
        b"subcategory-guard",
        (int(deal_id), str(original_subcategory)),
    )


def activity_index_guard_fingerprint(
    key: bytes,
    deal_id: int,
    activity_index: object,
) -> str:
    canonical = (
        activity_index
        if isinstance(activity_index, str)
        else json.dumps(
            canonical_activity_index_value(deal_id, activity_index),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return _fingerprint(
        key,
        b"activity-index-guard",
        (int(deal_id), str(canonical)),
    )


def activity_locator_fingerprint(
    key: bytes,
    deal_id: int,
    kind: str,
    activity_id: int | str,
) -> str:
    if kind not in {"email", "call"}:
        raise ValueError("Некорректный тип activity locator")
    raw_id = str(activity_id)
    if not raw_id.isdigit() or int(raw_id) <= 0:
        raise ValueError("Некорректный ID activity locator")
    return _fingerprint(
        key,
        b"activity-locator",
        (int(deal_id), kind, raw_id),
    )


def key_check(key: bytes) -> str:
    return hmac.new(key, _DOMAIN + b"\0key-check", hashlib.sha256).hexdigest()


def entries_digest(lines: list[str]) -> str:
    return hashlib.sha256(("\n".join(lines) + "\n").encode("utf-8")).hexdigest()


def manifest_mac(key: bytes, header: dict[str, object]) -> str:
    canonical = json.dumps(
        header,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hmac.new(key, _DOMAIN + b"\0manifest\0" + canonical, hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class PlanEntry:
    transition: str
    target: str
    evidence_mode: str
    subcategory_guard: str = "-"
    activity_index_guard: str = "-"
    activity_locators: tuple[str, ...] = ()


@dataclass(frozen=True)
class ApprovedPlan:
    year: int
    count: int
    transitions: frozenset[str]
    desired_modes: dict[str, str]
    digest: str
    key: bytes
    subcategory_guards: dict[str, str] = field(default_factory=dict)
    activity_index_guards: dict[str, str] = field(default_factory=dict)
    activity_locators: dict[str, tuple[str, ...]] = field(default_factory=dict)
    plan_entries: dict[str, PlanEntry] = field(default_factory=dict)

    @property
    def requires_full_pairs(self) -> bool:
        return any(not is_category_only_mode(mode) for mode in self.desired_modes.values())

    @classmethod
    def load(
        cls,
        path: Path,
        key: bytes,
        expected_year: int,
        expected_portal: str,
    ) -> "ApprovedPlan":
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
        if not lines:
            raise ValueError("Файл согласованного плана пуст")
        header = json.loads(lines[0])
        try:
            protocol = (str(header.get("format", "")), int(header.get("version", 0)))
        except (TypeError, ValueError) as exc:
            raise ValueError("Неподдерживаемый формат согласованного плана") from exc
        allowed_evidence_modes = PLAN_PROTOCOLS.get(protocol)
        if allowed_evidence_modes is None:
            raise ValueError("Неподдерживаемый формат согласованного плана")
        is_v4 = protocol == (PLAN_FORMAT_V4, 4)
        is_v5 = protocol == (PLAN_FORMAT, PLAN_VERSION)
        year = int(header.get("year", 0))
        if year != expected_year:
            raise ValueError(f"План рассчитан на {year}, а worker запущен для {expected_year}")
        if not hmac.compare_digest(str(header.get("portal", "")), expected_portal):
            raise ValueError("План рассчитан для другого портала")
        if not hmac.compare_digest(str(header.get("key_check", "")), key_check(key)):
            raise ValueError("Ключ согласованного плана не подходит")

        content_lines = [line for line in lines[1:] if line.strip()]
        if not hmac.compare_digest(
            str(header.get("entries_digest", "")), entries_digest(content_lines)
        ):
            raise ValueError("Контрольная сумма строк плана не совпадает")
        signed_header = dict(header)
        supplied_mac = str(signed_header.pop("manifest_mac", ""))
        if not hmac.compare_digest(supplied_mac, manifest_mac(key, signed_header)):
            raise ValueError("Подпись заголовка плана не совпадает")

        transitions: set[str] = set()
        desired_modes: dict[str, str] = {}
        subcategory_guards: dict[str, str] = {}
        activity_index_guards: dict[str, str] = {}
        activity_locators: dict[str, tuple[str, ...]] = {}
        plan_entries: dict[str, PlanEntry] = {}
        for line_number, line in enumerate(lines[1:], start=2):
            if not line.strip():
                continue
            columns = line.split("\t")
            expected_columns = 6 if is_v5 else 4 if is_v4 else 3
            if len(columns) != expected_columns:
                raise ValueError(f"Некорректная строка плана {line_number}")
            transition, target, evidence_mode = columns[:3]
            subcategory_guard = columns[3] if is_v4 or is_v5 else "-"
            activity_index_guard = columns[4] if is_v5 else "-"
            locator_column = columns[5] if is_v5 else "-"
            if not FINGERPRINT_RE.fullmatch(transition) or not FINGERPRINT_RE.fullmatch(target):
                raise ValueError(f"Некорректный отпечаток в строке {line_number}")
            if evidence_mode not in allowed_evidence_modes:
                raise ValueError(f"Некорректный источник в строке {line_number}")
            if is_category_only_mode(evidence_mode):
                if not FINGERPRINT_RE.fullmatch(subcategory_guard):
                    raise ValueError(
                        f"Некорректная защита подкатегории в строке {line_number}"
                    )
            elif subcategory_guard != "-":
                raise ValueError(
                    f"Некорректная защита подкатегории в строке {line_number}"
                )
            if is_activity_mode(evidence_mode):
                if not is_v5 or not FINGERPRINT_RE.fullmatch(activity_index_guard):
                    raise ValueError(
                        f"Некорректная защита activity index в строке {line_number}"
                    )
                locators = tuple(locator_column.split(","))
                if (
                    not 1 <= len(locators) <= MAX_SELECTED_ACTIVITIES
                    or any(not FINGERPRINT_RE.fullmatch(value) for value in locators)
                    or tuple(sorted(locators)) != locators
                    or len(set(locators)) != len(locators)
                ):
                    raise ValueError(
                        f"Некорректные activity locators в строке {line_number}"
                    )
            else:
                if activity_index_guard != "-" or locator_column != "-":
                    raise ValueError(
                        f"Некорректные activity поля в строке {line_number}"
                    )
                locators = ()
            transitions.add(transition)
            if target in desired_modes:
                raise ValueError(f"Дублирующийся результат в строке {line_number}")
            desired_modes[target] = evidence_mode
            subcategory_guards[target] = subcategory_guard
            activity_index_guards[target] = activity_index_guard
            activity_locators[target] = locators
            plan_entries[target] = PlanEntry(
                transition=transition,
                target=target,
                evidence_mode=evidence_mode,
                subcategory_guard=subcategory_guard,
                activity_index_guard=activity_index_guard,
                activity_locators=locators,
            )

        count = int(header.get("count", 0))
        if count <= 0 or len(transitions) != count or len(desired_modes) != count:
            raise ValueError(
                "Число уникальных переходов не совпадает с заголовком плана: "
                f"{len(transitions)}/{len(desired_modes)} вместо {count}"
            )
        return cls(
            year=year,
            count=count,
            transitions=frozenset(transitions),
            desired_modes=desired_modes,
            digest=hashlib.sha256(raw).hexdigest(),
            key=key,
            subcategory_guards=subcategory_guards,
            activity_index_guards=activity_index_guards,
            activity_locators=activity_locators,
            plan_entries=plan_entries,
        )

    def entry_for_target(
        self,
        deal_id: int,
        desired: tuple[str, str],
        evidence_mode: str,
    ) -> PlanEntry | None:
        target = (
            category_desired_fingerprint(self.key, deal_id, desired[0])
            if is_category_only_mode(evidence_mode)
            else desired_fingerprint(self.key, deal_id, desired[0], desired[1])
        )
        if self.desired_modes.get(target) != evidence_mode:
            return None
        entry = self.plan_entries.get(target)
        if entry is not None:
            return entry
        return PlanEntry(
            transition="",
            target=target,
            evidence_mode=evidence_mode,
            subcategory_guard=self.subcategory_guards.get(target, "-"),
            activity_index_guard=self.activity_index_guards.get(target, "-"),
            activity_locators=self.activity_locators.get(target, ()),
        )

    def category_guard_matches(
        self,
        deal_id: int,
        desired_category: str,
        live_subcategory: str,
    ) -> bool:
        target = category_desired_fingerprint(self.key, deal_id, desired_category)
        evidence_mode = self.desired_modes.get(target)
        if not evidence_mode or not is_category_only_mode(evidence_mode):
            return False
        expected_guard = self.subcategory_guards.get(target, "")
        live_guard = subcategory_guard_fingerprint(
            self.key,
            deal_id,
            live_subcategory,
        )
        return hmac.compare_digest(expected_guard, live_guard)

    def activity_index_guard_matches(
        self,
        deal_id: int,
        desired: tuple[str, str],
        evidence_mode: str,
        canonical_index: str,
    ) -> bool:
        entry = self.entry_for_target(deal_id, desired, evidence_mode)
        if entry is None or not is_activity_mode(entry.evidence_mode):
            return False
        live_guard = activity_index_guard_fingerprint(
            self.key,
            deal_id,
            canonical_index,
        )
        return hmac.compare_digest(entry.activity_index_guard, live_guard)

    def target_for(
        self,
        deal_id: int,
        allowed_pairs: tuple[tuple[str, str], ...],
        current: tuple[str, str] | None = None,
        allowed_categories: tuple[str, ...] = (),
    ) -> tuple[str, str, str, bool] | None:
        allowed_pair_set = set(allowed_pairs)
        allowed_category_set = {
            str(category)
            for category in allowed_categories
            if str(category) not in FORBIDDEN_CATEGORY_IDS
        }
        if current and current[0] not in FORBIDDEN_CATEGORY_IDS:
            fingerprint = desired_fingerprint(self.key, deal_id, current[0], current[1])
            evidence_mode = self.desired_modes.get(fingerprint)
            if (
                evidence_mode
                and not is_category_only_mode(evidence_mode)
                and current in allowed_pair_set
            ):
                return current[0], current[1], evidence_mode, True
        for category, subcategory in allowed_pairs:
            if category in FORBIDDEN_CATEGORY_IDS:
                continue
            if current == (category, subcategory):
                continue
            fingerprint = desired_fingerprint(self.key, deal_id, category, subcategory)
            evidence_mode = self.desired_modes.get(fingerprint)
            if evidence_mode and not is_category_only_mode(evidence_mode):
                return category, subcategory, evidence_mode, True
        # Category-only target discovery is intentionally independent of the
        # live subcategory. The separate signed guard makes a changed value a
        # discovered conflict instead of silently leaving the row undiscovered.
        if current:
            for category in sorted(allowed_category_set):
                fingerprint = category_desired_fingerprint(self.key, deal_id, category)
                evidence_mode = self.desired_modes.get(fingerprint)
                if evidence_mode and is_category_only_mode(evidence_mode):
                    guard_match = self.category_guard_matches(
                        deal_id,
                        category,
                        current[1],
                    )
                    return category, current[1], evidence_mode, guard_match
        return None

    def approves_transition(
        self,
        deal_id: int,
        original: tuple[str, str],
        desired: tuple[str, str],
        evidence_mode: str = "fields",
        evidence: str = "",
    ) -> bool:
        value = transition_fingerprint(
            self.key,
            deal_id,
            original[0],
            original[1],
            desired[0],
            desired[1],
            evidence_mode,
            evidence,
        )
        return value in self.transitions
