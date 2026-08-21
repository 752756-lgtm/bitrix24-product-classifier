from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .http import post_form


ACTIVITY_BINDING_PAGE_SIZE = 50
MAX_ACTIVITY_BINDINGS = 100


@dataclass(frozen=True)
class DealField:
    field_name: str
    field_type: str
    enum_by_label: dict[str, str]

    def encode(self, label: str) -> str:
        if self.field_type == "enumeration":
            try:
                return self.enum_by_label[label.casefold()]
            except KeyError as exc:
                raise ValueError(f"В поле {self.field_name} нет значения: {label}") from exc
        return label


class BitrixClient:
    def __init__(self, webhook_url: str, timeout: int = 45):
        self.webhook_url = webhook_url.rstrip("/") + "/"
        self.timeout = timeout

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        response = post_form(self.webhook_url + method + ".json", params or {}, self.timeout)
        if "error" in response:
            raise RuntimeError(f"Bitrix24 {method}: {response['error']} — {response.get('error_description', '')}")
        return response.get("result")

    def get_deal(self, deal_id: int) -> dict[str, Any]:
        return self.call("crm.deal.get", {"id": deal_id})

    def update_deal(self, deal_id: int, fields: dict[str, Any]) -> None:
        self.call("crm.deal.update", {"id": deal_id, "fields": fields})

    def add_timeline_comment(self, deal_id: int, comment: str) -> None:
        self.call("crm.timeline.comment.add", {"fields": {"ENTITY_ID": deal_id, "ENTITY_TYPE": "deal", "COMMENT": comment}})

    @staticmethod
    def _activity_binding_page(value: Any) -> list[dict[str, int]]:
        if (
            not isinstance(value, list)
            or len(value) > ACTIVITY_BINDING_PAGE_SIZE
            or any(not isinstance(row, dict) for row in value)
        ):
            raise RuntimeError("Bitrix24 вернул некорректные bindings activity")
        page: list[dict[str, int]] = []
        for row in value:
            if (
                set(row).intersection(
                    {
                        "OWNER_TYPE_ID",
                        "ownerTypeId",
                        "OWNER_ID",
                        "ownerId",
                    }
                )
                or not isinstance(row.get("entityTypeId"), int)
                or isinstance(row.get("entityTypeId"), bool)
                or not isinstance(row.get("entityId"), int)
                or isinstance(row.get("entityId"), bool)
                or int(row["entityTypeId"]) <= 0
                or int(row["entityId"]) <= 0
            ):
                raise RuntimeError(
                    "Bitrix24 вернул некорректные bindings activity"
                )
            page.append(
                {
                    "OWNER_TYPE_ID": int(row["entityTypeId"]),
                    "OWNER_ID": int(row["entityId"]),
                }
            )
        return page

    def list_activity_bindings(self, activity_id: int) -> list[dict[str, int]]:
        if isinstance(activity_id, bool) or int(activity_id) <= 0:
            raise ValueError("Некорректный ID activity")
        bindings: list[dict[str, int]] = []
        seen: set[tuple[int, int]] = set()
        for start in (0, ACTIVITY_BINDING_PAGE_SIZE, MAX_ACTIVITY_BINDINGS):
            page = self._activity_binding_page(
                self.call(
                    "crm.activity.binding.list",
                    {"activityId": int(activity_id), "start": start},
                )
            )
            if start == MAX_ACTIVITY_BINDINGS:
                if page:
                    raise RuntimeError("Превышен лимит bindings activity")
                return bindings
            for binding in page:
                identity = (
                    binding["OWNER_TYPE_ID"],
                    binding["OWNER_ID"],
                )
                if identity in seen:
                    raise RuntimeError("Bindings activity содержат дубликаты")
                seen.add(identity)
                bindings.append(binding)
            if len(page) < ACTIVITY_BINDING_PAGE_SIZE:
                return bindings
        raise RuntimeError("Некорректная пагинация bindings activity")

    def get_activity(self, activity_id: int) -> dict[str, Any]:
        if isinstance(activity_id, bool) or int(activity_id) <= 0:
            raise ValueError("Некорректный ID activity")
        requested_id = int(activity_id)
        activity = self.call("crm.activity.get", {"id": requested_id})
        if not isinstance(activity, dict):
            raise RuntimeError("Bitrix24 вернул некорректную activity")
        returned_ids = [
            activity[name] for name in ("ID", "id") if name in activity
        ]
        if not returned_ids or any(
            isinstance(raw_id, bool)
            or not str(raw_id).isdigit()
            or int(raw_id) != requested_id
            for raw_id in returned_ids
        ):
            raise RuntimeError("Bitrix24 вернул activity с другим ID")
        hydrated = dict(activity)
        hydrated["BINDINGS"] = self.list_activity_bindings(requested_id)
        return hydrated

    def list_deal_calls(self, deal_id: int) -> list[dict[str, Any]]:
        return self.call(
            "crm.activity.list",
            {
                "order": {"END_TIME": "DESC", "ID": "DESC"},
                "filter": {"OWNER_TYPE_ID": 2, "OWNER_ID": deal_id, "TYPE_ID": 2},
                "select": ["ID", "SUBJECT", "START_TIME", "END_TIME", "COMPLETED", "PROVIDER_ID"],
            },
        )

    def get_call_transcript(self, activity_id: int) -> str | None:
        result = self.call("crm.activity.call.getTranscript", {"activityId": activity_id})
        if not result:
            return None
        transcript = str(result.get("transcription", "")).strip()
        return transcript or None

    def resolve_deal_field(self, display_name: str, configured_field_name: str = "") -> DealField:
        fields = self.call("crm.deal.userfield.list", {"order": {"ID": "ASC"}})
        if configured_field_name:
            for field in fields:
                if field.get("FIELD_NAME") == configured_field_name:
                    return self._deal_field_from_details(self.call("crm.deal.userfield.get", {"id": field["ID"]}))
            raise RuntimeError(f"Не найден код поля сделки: {configured_field_name}")
        target = display_name.casefold()
        for field in fields:
            details = self.call("crm.deal.userfield.get", {"id": field["ID"]})
            labels = [_localized(details.get("EDIT_FORM_LABEL")), _localized(details.get("LIST_COLUMN_LABEL"))]
            if any(label.casefold() == target for label in labels):
                return self._deal_field_from_details(details)
        raise RuntimeError(f"Не найдено поле сделки: {display_name}")

    @staticmethod
    def _deal_field_from_details(field: dict[str, Any]) -> DealField:
        enum = {
            str(item.get("VALUE", "")).casefold(): str(item["ID"])
            for item in field.get("LIST", [])
            if item.get("VALUE") and item.get("ID")
        }
        return DealField(field["FIELD_NAME"], field.get("USER_TYPE_ID", "string"), enum)


def _localized(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return str(value.get("ru") or value.get("en") or "")
    return ""
