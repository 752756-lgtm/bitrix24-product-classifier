from __future__ import annotations

import json
from dataclasses import dataclass

from .catalog import ProductGroup
from .http import post_json


@dataclass(frozen=True)
class CallAnalysis:
    title: str
    summary: str
    product_specific: bool
    category: str | None
    subcategory: str | None


class OpenAIAnalyzer:
    def __init__(self, api_key: str, model: str, timeout: int = 45):
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    def analyze(self, transcript: str, groups: list[ProductGroup]) -> CallAnalysis:
        allowed = [{"category": g.category, "subcategory": g.subcategory} for g in groups]
        schema = {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Суть звонка, до 100 символов"},
                "summary": {"type": "string", "description": "Краткое резюме: потребность, параметры, договоренности и следующий шаг"},
                "product_specific": {"type": "boolean"},
                "category": {"type": ["string", "null"]},
                "subcategory": {"type": ["string", "null"]},
            },
            "required": ["title", "summary", "product_specific", "category", "subcategory"],
            "additionalProperties": False,
        }
        prompt = (
            "Проанализируй расшифровку входящего звонка российского B2B-магазина оборудования. "
            "Сформулируй конкретный заголовок без имени менеджера и общих слов вроде 'входящий звонок'. "
            "Если клиент обсуждает конкретную товарную группу, выбери в точности одну пару из списка. "
            "Если конкретной группы нет, product_specific=false, category=null, subcategory=null. "
            "Не угадывай категорию по слабым признакам.\n\n"
            f"Разрешенные пары:\n{json.dumps(allowed, ensure_ascii=False)}\n\n"
            f"Расшифровка:\n{transcript}"
        )
        response = post_json(
            "https://api.openai.com/v1/responses",
            {
                "model": self.model,
                "input": prompt,
                "text": {"format": {"type": "json_schema", "name": "call_analysis", "strict": True, "schema": schema}},
            },
            {"Authorization": f"Bearer {self.api_key}"},
            self.timeout,
        )
        payload = json.loads(_output_text(response))
        analysis = CallAnalysis(**payload)
        self._validate_group(analysis, groups)
        return analysis

    @staticmethod
    def _validate_group(analysis: CallAnalysis, groups: list[ProductGroup]) -> None:
        if not analysis.product_specific:
            return
        allowed = {(g.category, g.subcategory) for g in groups}
        if (analysis.category, analysis.subcategory) not in allowed:
            raise ValueError("Модель вернула товарную группу, отсутствующую в YML")


def _output_text(response: dict) -> str:
    for item in response.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content["text"]
    raise RuntimeError("OpenAI API не вернул текст результата")

