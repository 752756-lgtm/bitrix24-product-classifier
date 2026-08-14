from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def get_bytes(url: str, timeout: int) -> bytes:
    request = Request(url, headers={"User-Agent": "bitrix24-call-classifier/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def post_form(url: str, data: dict[str, Any], timeout: int) -> dict[str, Any]:
    encoded = urlencode(_flatten(data)).encode("utf-8")
    request = Request(url, data=encoded, headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def post_json(url: str, data: dict[str, Any], headers: dict[str, str], timeout: int) -> dict[str, Any]:
    request = Request(
        url,
        data=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _flatten(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            name = f"{prefix}[{key}]" if prefix else str(key)
            result.extend(_flatten(item, name))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.extend(_flatten(item, f"{prefix}[{index}]"))
    elif value is not None:
        result.append((prefix, str(value)))
    return result

