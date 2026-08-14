from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Offer:
    offer_id: str
    name: str
    category: str
    subcategory: str


@dataclass(frozen=True)
class DealProduct:
    product_id: str
    xml_id: str
    name: str
    price: Decimal
    quantity: Decimal

    @property
    def amount(self) -> Decimal:
        return self.price * self.quantity


@dataclass(frozen=True)
class Classification:
    category: str
    subcategory: str
    amount: Decimal
    matched_products: int

