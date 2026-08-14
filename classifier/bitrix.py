from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from .domain import DealProduct


class BitrixError(RuntimeError):
    pass


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

