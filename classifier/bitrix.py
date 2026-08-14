from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from .domain import DealProduct


class BitrixError(RuntimeError):
    pass


def _normalized(value: str) -> str:
    return " ".join(value.casefold().split())


class BitrixClient:
    def __init__(self, webhook_url: str, timeout: float = 30.0) -> None:
        self.webhook_url = webhook_url.rstrip("/") + "/"
        self.client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self.client.close()

    def _call(self, method: str, payload: dict[str, Any]) -> Any:
        response = self.client.post(self.webhook_url + method + ".json", json=payload)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise BitrixError(f"{data['error']}: {data.get('error_description', '')}")
        return data.get("result")

    def get_deal_products(self, deal_id: int) -> list[DealProduct]:
        rows = self._call("crm.deal.productrows.get", {"id": deal_id}) or []
        return [
            DealProduct(
                product_id=str(row.get("PRODUCT_ID", "")),
                xml_id=str(row.get("PRODUCT_XML_ID", "")),
                name=str(row.get("PRODUCT_NAME", "")),
                price=Decimal(str(row.get("PRICE", 0))),
                quantity=Decimal(str(row.get("QUANTITY", 0))),
            )
            for row in rows
        ]

    def update_deal(self, deal_id: int, fields: dict[str, str]) -> None:
        self._call("crm.deal.update", {"id": deal_id, "fields": fields})

    def resolve_enumeration_value(self, field_title: str, value: str) -> tuple[str, str]:
        fields = self._call("crm.deal.fields", {}) or {}
        wanted_title = _normalized(field_title)
        for field_id, field in fields.items():
            labels = [
                field.get("title", ""),
                field.get("formLabel", ""),
                field.get("filterLabel", ""),
                field.get("listLabel", ""),
            ]
            if wanted_title not in {_normalized(str(label)) for label in labels if label}:
                continue
            if field.get("type") != "enumeration":
                raise BitrixError(f"Field {field_title!r} is not an enumeration")
            wanted_value = _normalized(value)
            for item in field.get("items", []):
                if _normalized(str(item.get("VALUE", ""))) == wanted_value:
                    return field_id, str(item["ID"])
            raise BitrixError(f"Value {value!r} is missing from field {field_title!r}")
        raise BitrixError(f"Deal field {field_title!r} was not found")
