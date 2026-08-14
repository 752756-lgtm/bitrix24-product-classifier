from __future__ import annotations

from dataclasses import dataclass

from .ai import CallAnalysis, OpenAIAnalyzer
from .bitrix import BitrixClient
from .catalog import ProductGroup


MARKER = "[CALL_AI_SUMMARY]"


@dataclass(frozen=True)
class ProcessingResult:
    deal_id: int
    analysis: CallAnalysis
    updated_fields: dict[str, str]
    activity_id: int | None = None


class CallProcessingService:
    def __init__(self, bitrix: BitrixClient, analyzer: OpenAIAnalyzer, groups: list[ProductGroup], category_field_name: str, subcategory_field_name: str, title_max_length: int = 100, category_field_id: str = "", subcategory_field_id: str = ""):
        self.bitrix = bitrix
        self.analyzer = analyzer
        self.groups = groups
        self.category_field_name = category_field_name
        self.subcategory_field_name = subcategory_field_name
        self.title_max_length = title_max_length
        self.category_field_id = category_field_id
        self.subcategory_field_id = subcategory_field_id

    def process(self, deal_id: int, transcript: str, dry_run: bool = False) -> ProcessingResult:
        transcript = " ".join(transcript.split())
        if len(transcript) < 20:
            raise ValueError("Расшифровка слишком короткая")
        self.bitrix.get_deal(deal_id)
        analysis = self.analyzer.analyze(transcript, self.groups)
        fields: dict[str, str] = {"TITLE": analysis.title.strip()[: self.title_max_length]}
        if analysis.product_specific and analysis.category and analysis.subcategory:
            category_field = self.bitrix.resolve_deal_field(self.category_field_name, self.category_field_id)
            subcategory_field = self.bitrix.resolve_deal_field(self.subcategory_field_name, self.subcategory_field_id)
            fields[category_field.field_name] = category_field.encode(analysis.category)
            fields[subcategory_field.field_name] = subcategory_field.encode(analysis.subcategory)
        if not dry_run:
            self.bitrix.update_deal(deal_id, fields)
            self.bitrix.add_timeline_comment(deal_id, f"{MARKER}\nКраткое резюме звонка:\n{analysis.summary}")
        return ProcessingResult(deal_id, analysis, fields)

    def process_existing_deal(self, deal_id: int, dry_run: bool = False) -> ProcessingResult:
        self.bitrix.get_deal(deal_id)
        calls = self.bitrix.list_deal_calls(deal_id)
        if not calls:
            raise ValueError(f"У сделки {deal_id} нет звонков")
        for activity in calls:
            activity_id = int(activity["ID"])
            transcript = self.bitrix.get_call_transcript(activity_id)
            if transcript:
                result = self.process(deal_id, transcript, dry_run=dry_run)
                return ProcessingResult(
                    result.deal_id,
                    result.analysis,
                    result.updated_fields,
                    activity_id=activity_id,
                )
        raise ValueError(f"У звонков сделки {deal_id} нет готовой расшифровки")


def extract_event(payload: dict, bitrix: BitrixClient) -> tuple[int, str]:
    flat = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    deal_id = _first(flat, "deal_id", "DEAL_ID", "entity_id", "ENTITY_ID", "data[FIELDS][OWNER_ID]")
    transcript = _first(flat, "transcript", "TRANSCRIPT", "text", "TEXT", "description", "DESCRIPTION", "data[FIELDS][DESCRIPTION]")
    activity_id = _first(flat, "activity_id", "ACTIVITY_ID", "ID", "data[FIELDS][ID]")
    if (not deal_id or not transcript) and activity_id:
        activity = bitrix.get_activity(int(activity_id))
        transcript = transcript or activity.get("DESCRIPTION")
        for binding in activity.get("BINDINGS", []):
            if str(binding.get("OWNER_TYPE_ID")) == "2":
                deal_id = binding.get("OWNER_ID")
                break
    if not deal_id or not transcript:
        raise ValueError("В событии нет deal_id и расшифровки либо activity_id с привязкой к сделке")
    return int(deal_id), str(transcript)


def _first(data: dict, *keys: str):
    for key in keys:
        if data.get(key) not in (None, ""):
            return data[key]
    return None
