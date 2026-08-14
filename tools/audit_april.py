from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path
from urllib.parse import urlencode

from classifier.bitrix import BitrixClient
from classifier.classify import classify_products
from classifier.domain import DealProduct
from classifier.yml_feed import parse_yml


def chunks(values, size):
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def batch(client: BitrixClient, commands: dict[str, str]):
    last_error = None
    for attempt in range(3):
        try:
            response = client._call("batch", {"halt": 0, "cmd": commands})
            return response.get("result", {}), response.get("result_error", {})
        except Exception as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise last_error


def list_april_deals(client: BitrixClient) -> list[int]:
    ids: list[int] = []
    start = 0
    while True:
        rows = None
        for attempt in range(3):
            try:
                rows = client._call("crm.deal.list", {
                    "filter": {
                        ">=DATE_CREATE": "2026-04-01T00:00:00+03:00",
                        "<=DATE_CREATE": "2026-04-30T23:59:59+03:00",
                    },
                    "order": {"ID": "ASC"},
                    "select": ["ID"],
                    "start": start,
                }) or []
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2 * (attempt + 1))
        ids.extend(int(row["ID"]) for row in rows)
        print(f"deal-list: {len(ids)}", flush=True)
        if len(rows) < 50:
            break
        start += len(rows)
    return ids


def product_rows(client: BitrixClient, deal_ids: list[int]):
    rows_by_deal = {}
    errors = {}
    processed = 0
    for deal_group in chunks(deal_ids, 50):
        for deal_id in deal_group:
            rows_by_deal[deal_id] = []
        start = 0
        while True:
            result = None
            for attempt in range(3):
                try:
                    result = client._call("crm.item.productrow.list", {
                        "filter": {"=ownerType": "D", "=ownerId": deal_group},
                        "start": start,
                    }) or {}
                    break
                except Exception as exc:
                    if attempt == 2:
                        for deal_id in deal_group:
                            errors[deal_id] = str(exc)
                        result = {"productRows": []}
                    else:
                        time.sleep(2 * (attempt + 1))
            source_rows = result.get("productRows", [])
            for row in source_rows:
                owner_id = int(row["ownerId"])
                normalized = {
                    "PRODUCT_ID": row.get("productId", ""),
                    "PRODUCT_XML_ID": row.get("xmlId", ""),
                    "PRODUCT_NAME": row.get("productName", ""),
                    "PRICE": row.get("price", 0),
                    "QUANTITY": row.get("quantity", 0),
                }
                rows_by_deal[owner_id].extend(client._products_from_rows([normalized]))
            if len(source_rows) < 50:
                break
            start += len(source_rows)
        processed += len(deal_group)
        print(f"product-rows: {processed}/{len(deal_ids)}", flush=True)
    return rows_by_deal, errors


def main():
    client = BitrixClient(os.environ["BITRIX_WEBHOOK_URL"], 90)
    catalog = parse_yml(Path(os.environ["YML_FILE"]).read_bytes())
    category_data = json.loads(Path(os.environ["CATEGORY_JSON"]).read_text())["result"]
    subcategory_data = json.loads(Path(os.environ["SUBCATEGORY_JSON"]).read_text())["result"]
    category_map = {item["VALUE"]: item["ID"] for item in category_data.get("LIST", [])}
    subcategory_map = {item["VALUE"]: item["ID"] for item in subcategory_data.get("LIST", [])}
    categories = set(category_map)
    subcategories = set(subcategory_map)
    cache_path = Path(os.getenv("CACHE_JSON", "/tmp/april-products.json"))
    if cache_path.exists():
        cached = json.loads(cache_path.read_text())
        deal_ids = cached["deal_ids"]
        rows = {
            int(deal_id): [
                DealProduct(
                    product_id=item["product_id"], xml_id=item["xml_id"], name=item["name"],
                    price=Decimal(item["price"]), quantity=Decimal(item["quantity"]),
                )
                for item in items
            ]
            for deal_id, items in cached["rows"].items()
        }
        request_errors = cached["request_errors"]
    else:
        deal_ids = list_april_deals(client)
        rows, request_errors = product_rows(client, deal_ids)
        cache_path.write_text(json.dumps({
            "deal_ids": deal_ids,
            "rows": {
                str(deal_id): [
                    {"product_id": item.product_id, "xml_id": item.xml_id, "name": item.name,
                     "price": str(item.price), "quantity": str(item.quantity)}
                    for item in items
                ]
                for deal_id, items in rows.items()
            },
            "request_errors": request_errors,
        }, ensure_ascii=False))
    classified = {}
    no_match = []
    category_counts = Counter()
    subcategory_counts = Counter()
    for deal_id in deal_ids:
        if deal_id in request_errors:
            continue
        result = classify_products(rows.get(deal_id, []), catalog)
        if result is None:
            no_match.append(deal_id)
            continue
        classified[deal_id] = result
        category_counts[result.category] += 1
        subcategory_counts[result.subcategory] += 1
    unmatched_categories = {k: v for k, v in category_counts.items() if k not in categories}
    unmatched_subcategories = {k: v for k, v in subcategory_counts.items() if k not in subcategories}
    plan = [
        {
            "deal_id": deal_id,
            "category": result.category,
            "subcategory": result.subcategory,
            "category_id": category_map[result.category],
            "subcategory_id": subcategory_map[result.subcategory],
        }
        for deal_id, result in classified.items()
        if result.category in categories and result.subcategory in subcategories
    ]
    Path(os.getenv("PLAN_JSON", "/tmp/april-plan.json")).write_text(
        json.dumps(plan, ensure_ascii=False)
    )
    print(json.dumps({
        "deals": len(deal_ids),
        "with_product_rows": sum(bool(value) for value in rows.values()),
        "classified": len(classified),
        "no_match_or_empty": len(no_match),
        "request_errors": len(request_errors),
        "valid_for_write": sum(
            result.category in categories and result.subcategory in subcategories
            for result in classified.values()
        ),
        "unmatched_categories": dict(sorted(unmatched_categories.items(), key=lambda x: -x[1])),
        "unmatched_subcategories": dict(
            sorted(unmatched_subcategories.items(), key=lambda x: -x[1])
        ),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
