from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from classifier.precision_plan import (
    EVIDENCE_MODES,
    FORBIDDEN_CATEGORY_IDS,
    PLAN_FORMAT,
    PLAN_VERSION,
    activity_index_guard_fingerprint,
    activity_locator_fingerprint,
    canonical_activity_evidence,
    canonical_activity_index,
    canonical_deal_text_evidence,
    canonical_product_evidence,
    category_desired_fingerprint,
    derive_plan_key,
    desired_fingerprint,
    entries_digest,
    key_check,
    manifest_mac,
    portal_identity,
    selected_activity_identity,
    subcategory_guard_fingerprint,
    transition_fingerprint,
)


def _private_activity_texts(value: object) -> set[str]:
    result: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).casefold() in {
                "subject",
                "description",
                "transcription",
            }:
                text = "" if item is None or item is False else str(item)
                if text:
                    result.add(text)
            else:
                result.update(_private_activity_texts(item))
    elif isinstance(value, list):
        if (
            len(value) == 3
            and value[0] == "bitrix24-activity-index-v1"
            and isinstance(value[2], list)
        ):
            for row in value[2]:
                if isinstance(row, list) and len(row) == 15:
                    subject = (
                        "" if row[7] is None or row[7] is False else str(row[7])
                    )
                    if subject:
                        result.add(subject)
        for item in value:
            result.update(_private_activity_texts(item))
    return result


def _has_private_activity_text(
    private_texts: set[str],
    allowlist_content: str,
    taxonomy_content: str,
    summary_content: str,
) -> bool:
    try:
        allowlist_lines = allowlist_content.splitlines()
        header = json.loads(allowlist_lines[0])
        taxonomy = json.loads(taxonomy_content)
        summary = json.loads(summary_content)
    except (IndexError, json.JSONDecodeError, TypeError):
        return True

    hex32 = re.compile(r"^[0-9a-f]{32}$").fullmatch
    hex64 = re.compile(r"^[0-9a-f]{64}$").fullmatch
    expected_header_keys = {
        "format", "version", "year", "count", "portal", "key_check",
        "entries_digest", "manifest_mac",
    }
    if (
        not isinstance(header, dict)
        or set(header) != expected_header_keys
        or header.get("format") != PLAN_FORMAT
        or header.get("version") != PLAN_VERSION
        or not isinstance(header.get("year"), int)
        or not isinstance(header.get("count"), int)
        or any(
            not isinstance(header.get(field), str) or not hex64(header[field])
            for field in ("portal", "key_check", "entries_digest", "manifest_mac")
        )
        or header["count"] != len(allowlist_lines) - 1
    ):
        return True
    for line in allowlist_lines[1:]:
        columns = line.split("\t")
        if len(columns) != 6:
            return True
        transition, target, mode, subguard, index_guard, locators = columns
        if (
            not hex32(transition)
            or not hex32(target)
            or mode not in EVIDENCE_MODES
            or (subguard != "-" and not hex32(subguard))
            or (index_guard != "-" and not hex32(index_guard))
        ):
            return True
        if locators != "-":
            values = locators.split(",")
            if (
                not 1 <= len(values) <= 8
                or values != sorted(set(values))
                or any(not hex32(value) for value in values)
            ):
                return True

    expected_taxonomy_keys = {
        "format", "version", "year", "categories", "subcategories", "pairs",
    }
    if (
        not isinstance(taxonomy, dict)
        or set(taxonomy) != expected_taxonomy_keys
        or taxonomy.get("format") != "bitrix24-precision-taxonomy-v1"
        or taxonomy.get("version") != 1
        or taxonomy.get("year") != header["year"]
        or not isinstance(taxonomy.get("categories"), dict)
        or not isinstance(taxonomy.get("subcategories"), dict)
        or not isinstance(taxonomy.get("pairs"), list)
    ):
        return True
    labels: list[str] = []
    for mapping in (taxonomy["categories"], taxonomy["subcategories"]):
        if any(
            not isinstance(key, str)
            or not key.isdigit()
            or not isinstance(value, str)
            for key, value in mapping.items()
        ):
            return True
        labels.extend(mapping.values())
    if any(
        not isinstance(pair, list)
        or len(pair) != 2
        or any(not isinstance(value, str) or not value.isdigit() for value in pair)
        for pair in taxonomy["pairs"]
    ):
        return True
    if (
        not isinstance(summary, dict)
        or set(summary) != {"year", "transitions", "pairs"}
        or summary.get("year") != header["year"]
        or summary.get("transitions") != header["count"]
        or summary.get("pairs") != len(taxonomy["pairs"])
    ):
        return True

    for value in private_texts:
        if len(value) >= 8 and any(value in label for label in labels):
            return True
        if len(value) < 8 and value in labels:
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Создать публично безопасные HMAC-отпечатки из приватного плана"
    )
    parser.add_argument("--plan", required=True)
    parser.add_argument("--allowlist", required=True)
    parser.add_argument("--taxonomy", required=True)
    parser.add_argument("--products", required=True)
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()

    webhook = os.getenv("BITRIX_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("Нужен BITRIX_WEBHOOK_URL для привязки плана к порталу")
    secret = os.getenv("PRECISION_PLAN_KEY") or webhook
    portal = portal_identity(webhook)
    key = derive_plan_key(secret, portal)
    rows = json.loads(Path(args.plan).read_text())
    products = json.loads(Path(args.products).read_text())

    seen_ids: set[int] = set()
    fingerprints: list[tuple[str, str, str, str, str, str]] = []
    private_activity_texts: set[str] = set()
    categories: dict[str, str] = {}
    subcategories: dict[str, str] = {}
    pairs: set[tuple[str, str]] = set()
    for row in rows:
        deal_id = int(row["deal_id"])
        if deal_id in seen_ids:
            raise ValueError(f"Дублирующийся ID сделки: {deal_id}")
        seen_ids.add(deal_id)
        category = str(row["category_id"])
        if not category.isdigit():
            raise ValueError(f"У сделки {deal_id} некорректная категория")
        if "current_category_id" not in row or "current_subcategory_id" not in row:
            raise ValueError(f"У сделки {deal_id} нет live снимка текущих полей")
        original_category = str(row.get("current_category_id") or "")
        original_subcategory = str(row.get("current_subcategory_id") or "")
        category_only = row.get("category_only", False)
        if not isinstance(category_only, bool):
            raise ValueError(f"У сделки {deal_id} category_only должен быть boolean")
        if category in FORBIDDEN_CATEGORY_IDS:
            raise ValueError(f"У сделки {deal_id} запрещённая категория: {category}")
        if category_only:
            supplied_subcategory = str(row.get("subcategory_id") or "")
            if supplied_subcategory and supplied_subcategory != original_subcategory:
                raise ValueError(
                    f"У сделки {deal_id} category_only не может менять подкатегорию"
                )
            subcategory = original_subcategory
        else:
            subcategory = str(row["subcategory_id"])
            if not subcategory.isdigit():
                raise ValueError(f"У сделки {deal_id} некорректная подкатегория")
        reason = str(row.get("reason") or "")
        if reason == "existing_precise_subcategory":
            evidence_mode = "fields"
        elif reason in {"title_rule", "title_site_id"}:
            evidence_mode = "title"
        elif reason == "product_value":
            evidence_mode = "products"
        elif reason == "deal_text":
            evidence_mode = "deal_text"
        elif reason == "activities":
            evidence_mode = "activities"
        else:
            raise ValueError(f"У сделки {deal_id} неподдерживаемый источник: {reason}")
        if category_only:
            evidence_mode = f"category_{evidence_mode}"
        base_evidence_mode = evidence_mode.removeprefix("category_")
        activity_index_guard = "-"
        activity_locators = "-"
        if base_evidence_mode == "fields":
            evidence = ""
        elif base_evidence_mode == "title":
            evidence = str(row.get("title") or "")
        elif base_evidence_mode == "products":
            if str(deal_id) not in products:
                raise ValueError(f"У сделки {deal_id} нет снимка товарных строк")
            product_rows = products[str(deal_id)]
            if not product_rows:
                raise ValueError(f"У сделки {deal_id} пустой снимок товарных строк")
            evidence = canonical_product_evidence(product_rows)
        elif base_evidence_mode == "deal_text":
            comments = row.get("comments")
            if comments is None or comments is False or str(comments) == "":
                raise ValueError(f"У сделки {deal_id} нет комментария для проверки источника")
            evidence = canonical_deal_text_evidence(row.get("title"), comments)
        else:
            if "activity_index" not in row or "selected_activities" not in row:
                raise ValueError(f"У сделки {deal_id} нет activity evidence")
            raw_index = row["activity_index"]
            raw_selected = row["selected_activities"]
            canonical_index = canonical_activity_index(deal_id, raw_index)
            evidence = canonical_activity_evidence(
                deal_id,
                raw_index,
                raw_selected,
            )
            selected_locators = []
            for selected in raw_selected:
                kind, activity_id = selected_activity_identity(selected)
                selected_locators.append(
                    activity_locator_fingerprint(
                        key,
                        deal_id,
                        kind,
                        activity_id,
                    )
                )
            selected_locators.sort()
            if len(selected_locators) != len(set(selected_locators)):
                raise ValueError(f"У сделки {deal_id} дублирующиеся activity locators")
            activity_index_guard = activity_index_guard_fingerprint(
                key,
                deal_id,
                canonical_index,
            )
            activity_locators = ",".join(selected_locators)
            private_activity_texts.update(_private_activity_texts(raw_index))
            private_activity_texts.update(_private_activity_texts(raw_selected))
        if base_evidence_mode == "title" and not evidence:
            raise ValueError(f"У сделки {deal_id} нет названия для проверки источника")
        category_label = str(row["category"])
        if category in categories and categories[category] != category_label:
            raise ValueError(f"У категории {category} два разных названия")
        categories[category] = category_label
        if not category_only:
            subcategory_label = str(row["subcategory"])
            if subcategory in subcategories and subcategories[subcategory] != subcategory_label:
                raise ValueError(f"У подкатегории {subcategory} два разных названия")
            subcategories[subcategory] = subcategory_label
            pairs.add((category, subcategory))
        fingerprints.append(
            (
                transition_fingerprint(
                    key,
                    deal_id,
                    original_category,
                    original_subcategory,
                    category,
                    subcategory,
                    evidence_mode,
                    evidence,
                ),
                (
                    category_desired_fingerprint(key, deal_id, category)
                    if category_only
                    else desired_fingerprint(key, deal_id, category, subcategory)
                ),
                evidence_mode,
                (
                    subcategory_guard_fingerprint(
                        key,
                        deal_id,
                        original_subcategory,
                    )
                    if category_only
                    else "-"
                ),
                activity_index_guard,
                activity_locators,
            )
        )

    if len({item[0] for item in fingerprints}) != len(rows):
        raise ValueError("Коллизия или дубль отпечатка перехода")
    if len({item[1] for item in fingerprints}) != len(rows):
        raise ValueError("Коллизия или дубль отпечатка результата")

    content_lines = [
        (
            f"{transition}\t{target}\t{evidence_mode}\t{subcategory_guard}"
            f"\t{activity_index_guard}\t{activity_locators}"
        )
        for (
            transition,
            target,
            evidence_mode,
            subcategory_guard,
            activity_index_guard,
            activity_locators,
        ) in sorted(fingerprints)
    ]
    header = {
        "format": PLAN_FORMAT,
        "version": PLAN_VERSION,
        "year": args.year,
        "count": len(rows),
        "portal": portal,
        "key_check": key_check(key),
        "entries_digest": entries_digest(content_lines),
    }
    header["manifest_mac"] = manifest_mac(key, header)
    allowlist_path = Path(args.allowlist)
    allowlist_path.parent.mkdir(parents=True, exist_ok=True)
    allowlist_content = (
        "\n".join(
            [json.dumps(header, ensure_ascii=False, sort_keys=True)] + content_lines
        )
        + "\n"
    )

    taxonomy = {
        "format": "bitrix24-precision-taxonomy-v1",
        "version": 1,
        "year": args.year,
        "categories": dict(sorted(categories.items(), key=lambda item: int(item[0]))),
        "subcategories": dict(sorted(subcategories.items(), key=lambda item: int(item[0]))),
        "pairs": sorted((list(pair) for pair in pairs), key=lambda pair: (int(pair[0]), int(pair[1]))),
    }
    taxonomy_path = Path(args.taxonomy)
    taxonomy_content = json.dumps(taxonomy, ensure_ascii=False, indent=2) + "\n"
    summary = {"year": args.year, "transitions": len(rows), "pairs": len(pairs)}
    summary_content = json.dumps(summary)
    if _has_private_activity_text(
        private_activity_texts,
        allowlist_content,
        taxonomy_content,
        summary_content,
    ):
        raise ValueError("Приватные activity данные попали в публичные assets")
    allowlist_path.write_text(allowlist_content)
    taxonomy_path.parent.mkdir(parents=True, exist_ok=True)
    taxonomy_path.write_text(taxonomy_content)
    print(summary_content)


if __name__ == "__main__":
    main()
