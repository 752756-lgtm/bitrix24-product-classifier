from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit


PLAN_FORMAT_V2 = "bitrix24-precision-allowlist-v2"
PLAN_FORMAT_V3 = "bitrix24-precision-allowlist-v3"
PLAN_FORMAT = "bitrix24-precision-allowlist-v4"
PLAN_VERSION = 4
FINGERPRINT_RE = re.compile(r"^[0-9a-f]{32}$")
_DOMAIN = b"bitrix24-precision-plan-v1"
DEAL_TEXT_CANON_VERSION = "bitrix24-deal-text-v1"
EVIDENCE_MODES_V2 = frozenset({"fields", "title", "products"})
EVIDENCE_MODES_V3 = EVIDENCE_MODES_V2 | {"deal_text"}
CATEGORY_ONLY_EVIDENCE_MODES = frozenset(
    {
        "category_fields",
        "category_title",
        "category_products",
        "category_deal_text",
    }
)
EVIDENCE_MODES = EVIDENCE_MODES_V3 | CATEGORY_ONLY_EVIDENCE_MODES
FORBIDDEN_CATEGORY_IDS = frozenset({"6901"})
PLAN_PROTOCOLS = {
    (PLAN_FORMAT_V2, 2): EVIDENCE_MODES_V2,
    (PLAN_FORMAT_V3, 3): EVIDENCE_MODES_V3,
    (PLAN_FORMAT, PLAN_VERSION): EVIDENCE_MODES,
}


def is_category_only_mode(evidence_mode: str) -> bool:
    return evidence_mode in CATEGORY_ONLY_EVIDENCE_MODES


def base_evidence_mode(evidence_mode: str) -> str:
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
class ApprovedPlan:
    year: int
    count: int
    transitions: frozenset[str]
    desired_modes: dict[str, str]
    digest: str
    key: bytes
    subcategory_guards: dict[str, str] = field(default_factory=dict)

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
        is_v4 = protocol == (PLAN_FORMAT, PLAN_VERSION)
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
        for line_number, line in enumerate(lines[1:], start=2):
            if not line.strip():
                continue
            columns = line.split("\t")
            expected_columns = 4 if is_v4 else 3
            if len(columns) != expected_columns:
                raise ValueError(f"Некорректная строка плана {line_number}")
            transition, target, evidence_mode = columns[:3]
            subcategory_guard = columns[3] if is_v4 else "-"
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
            transitions.add(transition)
            if target in desired_modes:
                raise ValueError(f"Дублирующийся результат в строке {line_number}")
            desired_modes[target] = evidence_mode
            subcategory_guards[target] = subcategory_guard

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
