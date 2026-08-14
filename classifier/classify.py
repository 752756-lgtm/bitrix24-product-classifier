from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from .domain import Classification, DealProduct
from .yml_feed import YmlCatalog


def classify_products(products: list[DealProduct], catalog: YmlCatalog) -> Classification | None:
    totals: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for product in products:
        offer = catalog.find(product.xml_id, product.name)
        if offer is None:
            continue
        key = (offer.category, offer.subcategory)
        totals[key] += product.amount
        counts[key] += 1
    if not totals:
        return None
    winner = max(totals, key=lambda key: (totals[key], counts[key], key))
    return Classification(*winner, amount=totals[winner], matched_products=counts[winner])

