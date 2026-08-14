from __future__ import annotations

import hmac

from fastapi import FastAPI, Header, HTTPException, Request

from .ai import OpenAIAnalyzer
from .bitrix import BitrixClient
from .catalog import load_product_groups
from .config import Config
from .payload import decode_payload
from .service import CallProcessingService, extract_event


config = Config.from_env()
bitrix = BitrixClient(config.bitrix_webhook_url, config.http_timeout)
service = CallProcessingService(
    bitrix,
    OpenAIAnalyzer(config.openai_api_key, config.openai_model, config.http_timeout),
    load_product_groups(config.yml_url, config.http_timeout),
    config.category_field_name,
    config.subcategory_field_name,
    config.title_max_length,
    config.category_field_id,
    config.subcategory_field_id,
)
app = FastAPI(title="Bitrix24 Call Summarizer")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "product_groups": len(service.groups)}


@app.post("/bitrix/call-transcript")
async def call_transcript(request: Request, x_webhook_secret: str = Header(default=""), dry_run: bool = False) -> dict:
    if config.webhook_secret and not hmac.compare_digest(config.webhook_secret, x_webhook_secret):
        raise HTTPException(status_code=401, detail="Неверный секрет вебхука")
    try:
        payload = decode_payload(await request.body(), request.headers.get("content-type", ""))
        deal_id, transcript = extract_event(payload, bitrix)
        result = service.process(deal_id, transcript, dry_run=dry_run)
        return {
            "ok": True,
            "deal_id": result.deal_id,
            "title": result.analysis.title,
            "summary": result.analysis.summary,
            "category": result.analysis.category,
            "subcategory": result.analysis.subcategory,
            "updated_fields": result.updated_fields,
            "dry_run": dry_run,
        }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/bitrix/backfill-deal/{deal_id}")
def backfill_deal(deal_id: int, x_webhook_secret: str = Header(default=""), dry_run: bool = True) -> dict:
    if config.webhook_secret and not hmac.compare_digest(config.webhook_secret, x_webhook_secret):
        raise HTTPException(status_code=401, detail="Неверный секрет вебхука")
    try:
        result = service.process_existing_deal(deal_id, dry_run=dry_run)
        return {
            "ok": True,
            "deal_id": result.deal_id,
            "activity_id": result.activity_id,
            "title": result.analysis.title,
            "summary": result.analysis.summary,
            "category": result.analysis.category,
            "subcategory": result.analysis.subcategory,
            "updated_fields": result.updated_fields,
            "dry_run": dry_run,
        }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
