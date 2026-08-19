from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from classifier.precision_plan import (
    PLAN_FORMAT,
    PLAN_VERSION,
    canonical_product_evidence,
    derive_plan_key,
    desired_fingerprint,
    entries_digest,
    key_check,
    manifest_mac,
    portal_identity,
    transition_fingerprint,
)


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
    fingerprints: list[tuple[str, str, str]] = []
    categories: dict[str, str] = {}
    subcategories: dict[str, str] = {}
    pairs: set[tuple[str, str]] = set()
    for row in rows:
        deal_id = int(row["deal_id"])
        if deal_id in seen_ids:
            raise ValueError(f"Дублирующийся ID сделки: {deal_id}")
        seen_ids.add(deal_id)
        category = str(row["category_id"])
        subcategory = str(row["subcategory_id"])
        original_category = str(row.get("current_category_id") or "")
        original_subcategory = str(row.get("current_subcategory_id") or "")
        reason = str(row.get("reason") or "")
        if reason == "existing_precise_subcategory":
            evidence_mode = "fields"
        elif reason in {"title_rule", "title_site_id"}:
            evidence_mode = "title"
        elif reason == "product_value":
            evidence_mode = "products"
        else:
            raise ValueError(f"У сделки {deal_id} неподдерживаемый источник: {reason}")
        if evidence_mode == "fields":
            evidence = ""
        elif evidence_mode == "title":
            evidence = str(row.get("title") or "")
        else:
            if str(deal_id) not in products:
                raise ValueError(f"У сделки {deal_id} нет снимка товарных строк")
            product_rows = products[str(deal_id)]
            if not product_rows:
                raise ValueError(f"У сделки {deal_id} пустой снимок товарных строк")
            evidence = canonical_product_evidence(product_rows)
        if evidence_mode == "title" and not evidence:
            raise ValueError(f"У сделки {deal_id} нет названия для проверки источника")
        category_label = str(row["category"])
        subcategory_label = str(row["subcategory"])
        if category in categories and categories[category] != category_label:
            raise ValueError(f"У категории {category} два разных названия")
        if subcategory in subcategories and subcategories[subcategory] != subcategory_label:
            raise ValueError(f"У подкатегории {subcategory} два разных названия")
        categories[category] = category_label
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
                desired_fingerprint(key, deal_id, category, subcategory),
                evidence_mode,
            )
        )

    if len({item[0] for item in fingerprints}) != len(rows):
        raise ValueError("Коллизия или дубль отпечатка перехода")
    if len({item[1] for item in fingerprints}) != len(rows):
        raise ValueError("Коллизия или дубль отпечатка результата")

    content_lines = [
        f"{transition}\t{target}\t{evidence_mode}"
        for transition, target, evidence_mode in sorted(fingerprints)
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
    allowlist_path.write_text(
        "\n".join(
            [json.dumps(header, ensure_ascii=False, sort_keys=True)]
            + content_lines
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
    taxonomy_path.parent.mkdir(parents=True, exist_ok=True)
    taxonomy_path.write_text(json.dumps(taxonomy, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"year": args.year, "transitions": len(rows), "pairs": len(pairs)}))


if __name__ == "__main__":
    main()
