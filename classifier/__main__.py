from __future__ import annotations

import argparse
import json
import sys

from .config import Settings
from .service import classify_deal


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify Bitrix24 deal products from YML")
    parser.add_argument("--deal-id", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        result = classify_deal(Settings.from_env(), args.deal_id, args.dry_run)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    if result is None:
        print(json.dumps({"ok": True, "classified": False}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "classified": True,
                "category": result.category,
                "subcategory": result.subcategory,
                "amount": str(result.amount),
                "matched_products": result.matched_products,
                "dry_run": args.dry_run,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

