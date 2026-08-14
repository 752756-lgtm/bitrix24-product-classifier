from __future__ import annotations

from urllib.request import Request, urlopen
from datetime import datetime

from .bitrix import BitrixClient
from .config import Settings
from .domain import Classification
from .classify import classify_products
from .yml_feed import parse_yml


def load_catalog(settings: Settings):
    request = Request(
        settings.yml_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept": "application/xml,text/xml,*/*",
        },
    )
    with urlopen(request, timeout=settings.timeout) as response:
        return parse_yml(response.read())


def classify_deal(settings: Settings, deal_id: int, dry_run: bool = False) -> Classification | None:
    catalog = load_catalog(settings)
    bitrix = BitrixClient(settings.bitrix_webhook_url, settings.timeout)
    try:
        result = classify_products(bitrix.get_deal_products(deal_id), catalog)
        if result is None or dry_run:
            return result
        category_field, category = bitrix.resolve_enumeration_value(
            settings.category_field_title, result.category
        )
        subcategory_field, subcategory = bitrix.resolve_enumeration_value(
            settings.subcategory_field_title, result.subcategory
        )
        bitrix.update_deal(
            deal_id,
            {
                category_field: category,
                subcategory_field: subcategory,
            },
        )
        return result
    finally:
        bitrix.close()


def classify_period(
    settings: Settings,
    date_from: datetime,
    date_to: datetime,
    *,
    apply: bool = False,
    limit: int | None = None,
) -> list[tuple[int, Classification | None, str | None]]:
    catalog = load_catalog(settings)
    bitrix = BitrixClient(settings.bitrix_webhook_url, settings.timeout)
    results: list[tuple[int, Classification | None, str | None]] = []
    try:
        deal_ids = bitrix.list_deals_created(date_from, date_to, limit)
        for deal_id in deal_ids:
            try:
                result = classify_products(bitrix.get_deal_products(deal_id), catalog)
                if result is not None and apply:
                    category_field, category = bitrix.resolve_enumeration_value(
                        settings.category_field_title, result.category
                    )
                    subcategory_field, subcategory = bitrix.resolve_enumeration_value(
                        settings.subcategory_field_title, result.subcategory
                    )
                    bitrix.update_deal(
                        deal_id,
                        {category_field: category, subcategory_field: subcategory},
                    )
                results.append((deal_id, result, None))
            except Exception as exc:
                results.append((deal_id, None, str(exc)))
        return results
    finally:
        bitrix.close()
