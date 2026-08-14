from __future__ import annotations

import unicodedata
import xml.etree.ElementTree as ET
from collections.abc import Iterable

from .domain import Offer


def normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(normalized.split())


class YmlCatalog:
    def __init__(self, offers: Iterable[Offer]) -> None:
        self.by_id: dict[str, Offer] = {}
        self.by_name: dict[str, Offer] = {}
        for offer in offers:
            self.by_id[offer.offer_id] = offer
            self.by_name.setdefault(normalize_name(offer.name), offer)

    def find(self, xml_id: str, name: str) -> Offer | None:
        if xml_id and xml_id in self.by_id:
            return self.by_id[xml_id]
        return self.by_name.get(normalize_name(name))


def parse_yml(content: bytes) -> YmlCatalog:
    root = ET.fromstring(content)
    category_names: dict[str, str] = {}
    category_parents: dict[str, str] = {}
    for item in root.findall(".//categories/category"):
        category_id = item.attrib.get("id", "")
        category_names[category_id] = (item.text or "").strip()
        parent_id = item.attrib.get("parentId")
        if parent_id:
            category_parents[category_id] = parent_id

    def path(category_id: str) -> tuple[str, str]:
        names: list[str] = []
        seen: set[str] = set()
        current = category_id
        while current and current not in seen:
            seen.add(current)
            name = category_names.get(current)
            if name:
                names.append(name)
            current = category_parents.get(current, "")
        names.reverse()
        if not names:
            return "", ""
        return names[0], names[-1] if len(names) > 1 else ""

    offers: list[Offer] = []
    for item in root.findall(".//offers/offer"):
        offer_id = item.attrib.get("id", "").strip()
        name = (item.findtext("name") or item.findtext("model") or "").strip()
        category, subcategory = path((item.findtext("categoryId") or "").strip())
        if offer_id and name and category:
            offers.append(Offer(offer_id, name, category, subcategory))
    return YmlCatalog(offers)

