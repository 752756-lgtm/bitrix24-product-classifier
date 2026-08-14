from __future__ import annotations

from decimal import Decimal
from datetime import datetime
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .domain import DealProduct


class BitrixError(RuntimeError):
    pass


def _normalized(value: str) -> str:
    return " ".join(value.casefold().split())


class BitrixClient:
    def __init__(self, webhook_url: str, timeout: float = 30.0) -> None:
        self.webhook_url = webhook_url.rstrip("/") + "/"
        self.timeout = timeout
        self._deal_fields: dict[str, Any] | None = None

    def close(self) -> None:
        pass

    def _call(self, method: str, payload: dict[str, Any]) -> Any:
        request = Request(
            self.webhook_url + method + ".json",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "bitrix24-classifier/0.1"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                data = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            raise BitrixError(f"Bitrix request {method} failed: {exc}") from exc
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

    def list_deals_created(self, date_from: datetime, date_to: datetime) -> list[int]:
        deal_ids: list[int] = []
        start = 0
        while True:
            rows = self._call(
                "crm.deal.list",
                {
                    "filter": {
                        ">=DATE_CREATE": date_from.isoformat(),
                        "<=DATE_CREATE": date_to.isoformat(),
                    },
                    "order": {"ID": "ASC"},
                    "select": ["ID"],
                    "start": start,
                },
            ) or []
            deal_ids.extend(int(row["ID"]) for row in rows)
            if len(rows) < 50:
                return deal_ids
            start += len(rows)

    def resolve_enumeration_value(self, field_title: str, value: str) -> tuple[str, str]:
        if self._deal_fields is None:
            self._deal_fields = self._call("crm.deal.fields", {}) or {}
        fields = self._deal_fields
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
