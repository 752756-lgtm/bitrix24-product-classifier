from __future__ import annotations

import httpx

from .bitrix import BitrixClient
from .config import Settings
from .domain import Classification
from .classify import classify_products
from .yml_feed import parse_yml


def classify_deal(settings: Settings, deal_id: int, dry_run: bool = False) -> Classification | None:
    response = httpx.get(settings.yml_url, timeout=settings.timeout, follow_redirects=True)
    response.raise_for_status()
    catalog = parse_yml(response.content)
    bitrix = BitrixClient(settings.bitrix_webhook_url, settings.timeout)
    try:
        result = classify_products(bitrix.get_deal_products(deal_id), catalog)
        if result is None or dry_run:
            return result
        category = settings.category_value_map.get(result.category, result.category)
        subcategory = settings.subcategory_value_map.get(result.subcategory, result.subcategory)
        bitrix.update_deal(
            deal_id,
            {
                settings.category_field_id: category,
                settings.subcategory_field_id: subcategory,
            },
        )
        return result
    finally:
        bitrix.close()

