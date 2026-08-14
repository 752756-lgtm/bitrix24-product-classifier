from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


def _json_map(name: str) -> dict[str, str]:
    raw = os.getenv(name, "{}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(key): str(item) for key, item in value.items()}


@dataclass(frozen=True)
class Settings:
    bitrix_webhook_url: str
    yml_url: str
    category_field_id: str
    subcategory_field_id: str
    timeout: float = 30.0
    category_value_map: dict[str, str] = field(default_factory=dict)
    subcategory_value_map: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "Settings":
        required = {
            "BITRIX_WEBHOOK_URL": os.getenv("BITRIX_WEBHOOK_URL", "").strip(),
            "YML_URL": os.getenv("YML_URL", "").strip(),
            "CATEGORY_FIELD_ID": os.getenv("CATEGORY_FIELD_ID", "").strip(),
            "SUBCATEGORY_FIELD_ID": os.getenv("SUBCATEGORY_FIELD_ID", "").strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError("Missing required environment variables: " + ", ".join(missing))
        return cls(
            bitrix_webhook_url=required["BITRIX_WEBHOOK_URL"],
            yml_url=required["YML_URL"],
            category_field_id=required["CATEGORY_FIELD_ID"],
            subcategory_field_id=required["SUBCATEGORY_FIELD_ID"],
            timeout=float(os.getenv("HTTP_TIMEOUT", "30")),
            category_value_map=_json_map("CATEGORY_VALUE_MAP"),
            subcategory_value_map=_json_map("SUBCATEGORY_VALUE_MAP"),
        )

