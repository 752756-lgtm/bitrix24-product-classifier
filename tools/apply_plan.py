from __future__ import annotations

import argparse
import json
import os
import time
from itertools import islice
from pathlib import Path
from urllib.parse import urlencode

from classifier.bitrix import BitrixClient

CATEGORY_FIELD = "UF_CRM_1776320088319"
SUBCATEGORY_FIELD = "UF_CRM_1568228872884"


def chunks(values, size):
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def batch(client, commands):
    last_error = None
    for attempt in range(3):
        try:
            response = client._call("batch", {"halt": 0, "cmd": commands})
            return response.get("result", {}), response.get("result_error", {})
        except Exception as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise last_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text())
    client = BitrixClient(os.environ["BITRIX_WEBHOOK_URL"], 90)
    expected = {item["deal_id"]: item for item in plan}
    confirmed = []
    for group in chunks([item["deal_id"] for item in plan], 50):
        rows = client._call("crm.deal.list", {
            "filter": {"@ID": group},
            "select": ["ID", CATEGORY_FIELD, SUBCATEGORY_FIELD],
            "order": {"ID": "ASC"},
        }) or []
        for row in rows:
            deal_id = int(row["ID"])
            item = expected[deal_id]
            if (str(row.get(CATEGORY_FIELD, "")), str(row.get(SUBCATEGORY_FIELD, ""))) == (
                str(item["category_id"]), str(item["subcategory_id"])
            ):
                confirmed.append(deal_id)
    pending = [item for item in plan if item["deal_id"] not in set(confirmed)]
    print(f"already confirmed: {len(confirmed)}, pending: {len(pending)}", flush=True)
    updated = list(confirmed)
    failed = {}
    for group in chunks(pending, 10):
        commands = {}
        for item in group:
            params = [
                ("id", str(item["deal_id"])),
                (f"fields[{CATEGORY_FIELD}]", item["category_id"]),
                (f"fields[{SUBCATEGORY_FIELD}]", item["subcategory_id"]),
            ]
            commands[f"deal_{item['deal_id']}"] = "crm.deal.update?" + urlencode(params)
        result, errors = batch(client, commands)
        for item in group:
            deal_id = item["deal_id"]
            key = f"deal_{deal_id}"
            if result.get(key) is True:
                updated.append(deal_id)
            else:
                failed[str(deal_id)] = errors.get(key, "update returned false")
        print(f"updated: {len(updated)}/{len(plan)}, failed: {len(failed)}", flush=True)

    mismatches = {}
    for group in chunks(updated, 50):
        rows = client._call("crm.deal.list", {
            "filter": {"@ID": group},
            "select": ["ID", CATEGORY_FIELD, SUBCATEGORY_FIELD],
            "order": {"ID": "ASC"},
        }) or []
        returned = {int(row["ID"]): row for row in rows}
        for deal_id in group:
            row = returned.get(deal_id)
            item = expected[deal_id]
            actual = None if row is None else (
                str(row.get(CATEGORY_FIELD, "")), str(row.get(SUBCATEGORY_FIELD, ""))
            )
            wanted = (str(item["category_id"]), str(item["subcategory_id"]))
            if actual != wanted:
                mismatches[str(deal_id)] = {"expected": wanted, "actual": actual}
        print(f"verified through deal {group[-1]}", flush=True)

    report = {"planned": len(plan), "updated": len(updated), "failed": failed,
              "verification_mismatches": mismatches}
    Path("/tmp/april-apply-report.json").write_text(json.dumps(report, ensure_ascii=False))
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
