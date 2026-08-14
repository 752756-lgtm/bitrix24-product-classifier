from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    bitrix_webhook_url: str
    openai_api_key: str
    yml_url: str
    openai_model: str = "gpt-5.6-luna"
    category_field_name: str = "Категория товаров (Сайт)"
    subcategory_field_name: str = "Подкатегория товаров (Сайт)"
    webhook_secret: str = ""
    title_max_length: int = 100
    http_timeout: int = 45

    @classmethod
    def from_env(cls) -> "Config":
        required = {
            "BITRIX_WEBHOOK_URL": os.getenv("BITRIX_WEBHOOK_URL", "").strip(),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", "").strip(),
            "YML_URL": os.getenv("YML_URL", "").strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError("Не заданы переменные: " + ", ".join(missing))
        return cls(
            bitrix_webhook_url=required["BITRIX_WEBHOOK_URL"].rstrip("/") + "/",
            openai_api_key=required["OPENAI_API_KEY"],
            yml_url=required["YML_URL"],
            openai_model=os.getenv("OPENAI_MODEL", "gpt-5.6-luna"),
            category_field_name=os.getenv("CATEGORY_FIELD_NAME", "Категория товаров (Сайт)"),
            subcategory_field_name=os.getenv("SUBCATEGORY_FIELD_NAME", "Подкатегория товаров (Сайт)"),
            webhook_secret=os.getenv("WEBHOOK_SECRET", ""),
            title_max_length=int(os.getenv("TITLE_MAX_LENGTH", "100")),
            http_timeout=int(os.getenv("HTTP_TIMEOUT", "45")),
        )

