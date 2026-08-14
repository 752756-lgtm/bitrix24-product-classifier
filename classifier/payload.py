from __future__ import annotations

import json
from urllib.parse import parse_qs


def decode_payload(body: bytes, content_type: str) -> dict:
    text = body.decode("utf-8")
    if "application/json" in content_type:
        value = json.loads(text or "{}")
        if not isinstance(value, dict):
            raise ValueError("Тело JSON должно быть объектом")
        return value
    parsed = parse_qs(text, keep_blank_values=True)
    return {key: values[-1] for key, values in parsed.items()}
