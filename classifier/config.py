from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv(path: Path = Path(".env")) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


@dataclass(frozen=True)
class Settings:
    bitrix_webhook_url: str
    yml_url: str
    category_field_title: str
    subcategory_field_title: str
    timeout: float = 30.0

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv()
        required = {
            "BITRIX_WEBHOOK_URL": os.getenv("BITRIX_WEBHOOK_URL", "").strip(),
            "YML_URL": os.getenv("YML_URL", "").strip(),
            "CATEGORY_FIELD_TITLE": os.getenv(
                "CATEGORY_FIELD_TITLE", "Категория товаров (Сайт)"
            ).strip(),
            "SUBCATEGORY_FIELD_TITLE": os.getenv(
                "SUBCATEGORY_FIELD_TITLE", "Подкатегория товаров (Сайт)"
            ).strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError("Missing required environment variables: " + ", ".join(missing))
        return cls(
            bitrix_webhook_url=required["BITRIX_WEBHOOK_URL"],
            yml_url=required["YML_URL"],
            category_field_title=required["CATEGORY_FIELD_TITLE"],
            subcategory_field_title=required["SUBCATEGORY_FIELD_TITLE"],
            timeout=float(os.getenv("HTTP_TIMEOUT", "30")),
        )
