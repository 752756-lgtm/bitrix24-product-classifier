from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass

from .http import get_bytes


@dataclass(frozen=True)
class ProductGroup:
    category: str
    subcategory: str


def load_product_groups(yml_url: str, timeout: int = 45) -> list[ProductGroup]:
    root = ET.fromstring(get_bytes(yml_url, timeout))
    nodes: dict[str, tuple[str, str | None]] = {}
    for node in root.findall(".//categories/category"):
        category_id = (node.get("id") or "").strip()
        name = " ".join((node.text or "").split())
        if category_id and name:
            nodes[category_id] = (name, node.get("parentId"))

    groups: set[ProductGroup] = set()
    for category_id in nodes:
        path = _category_path(category_id, nodes)
        if len(path) >= 2:
            groups.add(ProductGroup(path[0], path[1]))
        elif path:
            groups.add(ProductGroup(path[0], path[0]))
    return sorted(groups, key=lambda item: (item.category.casefold(), item.subcategory.casefold()))


def _category_path(category_id: str, nodes: dict[str, tuple[str, str | None]]) -> list[str]:
    path: list[str] = []
    seen: set[str] = set()
    current: str | None = category_id
    while current and current in nodes and current not in seen:
        seen.add(current)
        name, parent = nodes[current]
        path.append(name)
        current = parent
    return list(reversed(path))

