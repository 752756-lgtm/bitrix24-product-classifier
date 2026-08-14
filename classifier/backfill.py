from __future__ import annotations

import argparse
import json

from .ai import OpenAIAnalyzer
from .bitrix import BitrixClient
from .catalog import load_product_groups
from .config import Config
from .service import CallProcessingService


def main() -> None:
    parser = argparse.ArgumentParser(description="Обработать старую сделку по последнему звонку с расшифровкой")
    parser.add_argument("--deal-id", type=int, required=True)
    parser.add_argument("--write", action="store_true", help="Записать результат в Битрикс24; без флага используется dry-run")
    args = parser.parse_args()

    config = Config.from_env()
    bitrix = BitrixClient(config.bitrix_webhook_url, config.http_timeout)
    service = CallProcessingService(
        bitrix,
        OpenAIAnalyzer(config.openai_api_key, config.openai_model, config.http_timeout),
        load_product_groups(config.yml_url, config.http_timeout),
        config.category_field_name,
        config.subcategory_field_name,
        config.title_max_length,
    )
    result = service.process_existing_deal(args.deal_id, dry_run=not args.write)
    print(json.dumps({
        "deal_id": result.deal_id,
        "activity_id": result.activity_id,
        "title": result.analysis.title,
        "summary": result.analysis.summary,
        "category": result.analysis.category,
        "subcategory": result.analysis.subcategory,
        "updated_fields": result.updated_fields,
        "dry_run": not args.write,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
