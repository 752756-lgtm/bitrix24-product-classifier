from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from .config import Settings
from .service import classify_deal, classify_period


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify Bitrix24 deal products from YML")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--deal-id", type=int)
    mode.add_argument("--created-from")
    parser.add_argument("--created-to")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        settings = Settings.from_env()
        if args.created_from:
            if not args.created_to:
                parser.error("--created-to is required with --created-from")
            results = classify_period(
                settings,
                datetime.fromisoformat(args.created_from),
                datetime.fromisoformat(args.created_to),
                apply=args.apply,
                limit=args.limit,
            )
            print(
                json.dumps(
                    {
                        "ok": True,
                        "apply": args.apply,
                        "total": len(results),
                        "classified": sum(result is not None for _, result, _ in results),
                        "errors": sum(error is not None for _, _, error in results),
                        "deals": [
                            {
                                "id": deal_id,
                                "category": result.category if result else None,
                                "subcategory": result.subcategory if result else None,
                                "error": error,
                            }
                            for deal_id, result, error in results
                        ],
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        result = classify_deal(settings, args.deal_id, args.dry_run or not args.apply)
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
