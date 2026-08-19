from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from .bitrix import BitrixClient
from .precision_plan import (
    ApprovedPlan,
    canonical_product_evidence,
    derive_plan_key,
    portal_identity,
)


LOG = logging.getLogger("precision-worker")

CATEGORY_FIELD = "UF_CRM_1776320088319"
SUBCATEGORY_FIELD = "UF_CRM_1568228872884"
STATE_SCHEMA_VERSION = 3
MAX_UNCONFIRMED_ATTEMPTS = 3

EXCLUDED_STAGE_NAMES = {
    "дубль",
    "реклама спам",
    "поставщик",
    "документы акты",
    "доставка",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalized(value: Any) -> str:
    return "" if value is None or value is False else str(value)


def normalize_stage_name(value: str) -> str:
    return re.sub(r"[^0-9a-zа-яё]+", " ", value.casefold()).strip()


def normalize_taxonomy_label(value: str) -> str:
    return re.sub(r"\s+", " ", str(value)).strip().casefold().replace("ё", "е")


def cached_plan_key(path: Path) -> bytes:
    try:
        value = bytes.fromhex(path.read_text().strip())
    except (OSError, ValueError) as exc:
        raise PermanentWorkerError("Некорректный постоянный ключ согласованного плана") from exc
    if len(value) != 32:
        raise PermanentWorkerError("Некорректная длина постоянного ключа плана")
    os.chmod(path, 0o600)
    return value


def persist_plan_key(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(value.hex() + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def chunks(values: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for value in values:
        batch.append(value)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def is_transient_error(exc: BaseException | str) -> bool:
    text = str(exc).casefold()
    if isinstance(exc, (TimeoutError, URLError)):
        return True
    if isinstance(exc, HTTPError):
        return exc.code == 429 or 500 <= exc.code < 600
    return any(
        marker in text
        for marker in (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "query_limit_exceeded",
            "operation_time_limit",
            "overload_limit",
            "internal_server_error",
            "error_unexpected_answer",
            "too many requests",
            "connection reset",
            "remote end closed",
            "http error 429",
            "http error 500",
            "http error 502",
            "http error 503",
            "http error 504",
        )
    )


def retry_delay(attempt: int, quota: bool = False) -> float:
    base = 180.0 if quota else 20.0
    raw = base * (2 ** min(max(attempt - 1, 0), 4)) * random.uniform(0.8, 1.2)
    return min(900.0, raw)


def is_quota_error(value: BaseException | str) -> bool:
    text = str(value).casefold()
    return any(marker in text for marker in ("limit", "429", "too many requests"))


class StopRequested(Exception):
    pass


class PermanentWorkerError(RuntimeError):
    pass


class RateLimitedBitrix:
    def __init__(self, webhook_url: str, timeout: int, min_interval: float, stop_check):
        self.client = BitrixClient(webhook_url, timeout)
        self.min_interval = max(0.0, min_interval)
        self.stop_check = stop_check
        self.last_started = 0.0

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        delay = self.last_started + self.min_interval - time.monotonic()
        while delay > 0:
            if self.stop_check():
                raise StopRequested()
            time.sleep(min(delay, 1.0))
            delay = self.last_started + self.min_interval - time.monotonic()
        self.last_started = time.monotonic()
        return self.client.call(method, params)


class State:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._create_schema()

    def close(self) -> None:
        self.db.close()

    def _create_schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS queue (
                deal_id INTEGER PRIMARY KEY,
                desired_category TEXT NOT NULL,
                desired_subcategory TEXT NOT NULL,
                original_category TEXT NOT NULL,
                original_subcategory TEXT NOT NULL,
                evidence_mode TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                stage_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS queue_status_next
            ON queue(status, next_attempt_at, deal_id);
            """
        )
        self.db.commit()

    def get_meta(self, key: str, default: str = "") -> str:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        self.db.commit()

    def bind_identity(self, identity: dict[str, Any]) -> None:
        encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        current = self.get_meta("worker_identity")
        if current and current != encoded:
            raise PermanentWorkerError(
                "Существующая SQLite относится к другому порталу, году или плану"
            )
        if not current:
            self.set_meta("worker_identity", encoded)

    def enqueue(
        self,
        deal_id: int,
        original: tuple[str, str],
        desired: tuple[str, str],
        stage_id: str,
        status: str,
        evidence_mode: str,
        source: str = "approved_plan_v2",
    ) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO queue(
                deal_id,desired_category,desired_subcategory,original_category,
                original_subcategory,evidence_mode,source,status,stage_id,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(deal_id) DO NOTHING
            """,
            (
                int(deal_id), desired[0], desired[1], original[0], original[1],
                evidence_mode, source, status, stage_id, now, now,
            ),
        )

    def commit(self) -> None:
        self.db.commit()

    def counts(self) -> dict[str, int]:
        return {
            str(row["status"]): int(row["count"])
            for row in self.db.execute(
                "SELECT status,COUNT(*) AS count FROM queue GROUP BY status"
            )
        }


def load_taxonomy(path: Path, year: int) -> tuple[dict[str, str], dict[str, str], tuple[tuple[str, str], ...], str]:
    raw = path.read_bytes()
    payload = json.loads(raw)
    if payload.get("format") != "bitrix24-precision-taxonomy-v1":
        raise PermanentWorkerError("Неподдерживаемый формат таксономии")
    if int(payload.get("version", 0)) != 1 or int(payload.get("year", 0)) != year:
        raise PermanentWorkerError("Версия или год таксономии не совпадает с worker")
    categories = {str(key): str(value) for key, value in payload["categories"].items()}
    subcategories = {str(key): str(value) for key, value in payload["subcategories"].items()}
    pairs = tuple((str(pair[0]), str(pair[1])) for pair in payload["pairs"])
    if not pairs or any(cat not in categories or sub not in subcategories for cat, sub in pairs):
        raise PermanentWorkerError("Таксономия содержит неполную пару")
    return categories, subcategories, pairs, hashlib.sha256(raw).hexdigest()


class PrecisionWorker:
    def __init__(
        self,
        state: State,
        bitrix: RateLimitedBitrix,
        approved_plan: ApprovedPlan,
        expected_categories: dict[str, str],
        expected_subcategories: dict[str, str],
        allowed_pairs: tuple[tuple[str, str], ...],
        identity: dict[str, Any],
        year: int,
        batch_size: int,
        write_interval: float,
        status_path: Path,
    ):
        self.state = state
        self.bitrix = bitrix
        self.approved_plan = approved_plan
        self.expected_categories = expected_categories
        self.expected_subcategories = expected_subcategories
        self.allowed_pairs = allowed_pairs
        self.identity = identity
        self.year = year
        self.batch_size = min(max(batch_size, 1), 50)
        self.write_interval = max(write_interval, 1.0)
        self.status_path = status_path
        self.stop = False
        self.excluded_stage_ids: set[str] = set()
        self.metadata_resolved_at = 0.0

    def request_stop(self, *_args) -> None:
        LOG.info("Получен сигнал остановки; незавершённая запись будет проверена после запуска")
        self.stop = True

    def sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + max(seconds, 0.0)
        while not self.stop and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
        if self.stop:
            raise StopRequested()

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        cooldown_until = float(self.state.get_meta("cooldown_until", "0") or 0)
        if cooldown_until > time.time():
            self.sleep(cooldown_until - time.time())
        try:
            return self.bitrix.call(method, params)
        except Exception as exc:
            if is_transient_error(exc):
                delay = retry_delay(1, quota=is_quota_error(exc))
                self.state.set_meta("cooldown_until", time.time() + delay)
            raise

    def _resolve_enum_field(self, field_name: str) -> dict[str, str]:
        rows = self.call(
            "crm.deal.userfield.list",
            {"filter": {"FIELD_NAME": field_name}, "order": {"ID": "ASC"}},
        ) or []
        field = next((row for row in rows if str(row.get("FIELD_NAME")) == field_name), None)
        if not field:
            raise PermanentWorkerError(f"Не найдено поле сделки {field_name}")
        if not field.get("LIST"):
            field = self.call("crm.deal.userfield.get", {"id": field["ID"]}) or {}
        result = {
            str(item["ID"]): str(item["VALUE"])
            for item in field.get("LIST", [])
            if item.get("ID") is not None and item.get("VALUE")
        }
        if not result:
            raise PermanentWorkerError(f"Поле {field_name} не содержит варианты списка")
        return result

    @staticmethod
    def _category_rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, dict):
            rows = value.get("categories", [])
        else:
            rows = value
        return rows if isinstance(rows, list) else []

    def _resolve_excluded_stages(self) -> set[str]:
        category_ids = {0}
        start = 0
        while True:
            categories_result = self.call(
                "crm.category.list", {"entityTypeId": 2, "start": start}
            )
            category_rows = self._category_rows(categories_result)
            for row in category_rows:
                if row.get("id") is not None or row.get("ID") is not None:
                    category_ids.add(int(row.get("id", row.get("ID"))))
            if len(category_rows) < 50:
                break
            start += len(category_rows)

        found: dict[str, set[str]] = {name: set() for name in EXCLUDED_STAGE_NAMES}
        for category_id in sorted(category_ids):
            entity_id = "DEAL_STAGE" if category_id == 0 else f"DEAL_STAGE_{category_id}"
            start = 0
            funnel_status_count = 0
            while True:
                rows = self.call(
                    "crm.status.list",
                    {
                        "filter": {"ENTITY_ID": entity_id},
                        "order": {"SORT": "ASC"},
                        "start": start,
                    },
                ) or []
                funnel_status_count += len(rows)
                for row in rows:
                    name = normalize_stage_name(str(row.get("NAME") or ""))
                    status_id = str(row.get("STATUS_ID") or "")
                    prefix = f"C{category_id}:"
                    if category_id and status_id and not status_id.startswith(prefix):
                        status_id = prefix + status_id
                    if name in found and status_id:
                        found[name].add(status_id)
                if len(rows) < 50:
                    break
                start += len(rows)
            if funnel_status_count == 0:
                raise PermanentWorkerError(
                    f"Воронка {entity_id} не вернула ни одного этапа; обработка остановлена"
                )

        missing = sorted(name for name, values in found.items() if not values)
        if missing:
            raise PermanentWorkerError(
                "Не найдены обязательные исключаемые этапы: " + ", ".join(missing)
            )
        return set().union(*found.values())

    @staticmethod
    def _validate_enum(expected: dict[str, str], live: dict[str, str], field_name: str) -> None:
        errors = []
        for item_id, expected_label in expected.items():
            live_label = live.get(item_id)
            if live_label is None:
                errors.append(f"{item_id}: отсутствует")
            elif normalize_taxonomy_label(live_label) != normalize_taxonomy_label(expected_label):
                errors.append(f"{item_id}: изменено название")
        if errors:
            raise PermanentWorkerError(
                f"Таксономия поля {field_name} изменилась: " + "; ".join(errors[:10])
            )

    def resolve_metadata(self) -> None:
        excluded = self._resolve_excluded_stages()
        categories = self._resolve_enum_field(CATEGORY_FIELD)
        subcategories = self._resolve_enum_field(SUBCATEGORY_FIELD)
        self._validate_enum(self.expected_categories, categories, CATEGORY_FIELD)
        self._validate_enum(self.expected_subcategories, subcategories, SUBCATEGORY_FIELD)
        self.excluded_stage_ids = excluded
        self.metadata_resolved_at = time.time()

    def scan_deals(self) -> None:
        if self.state.get_meta("deals_scan_complete") == "1":
            return
        last_id = int(self.state.get_meta("last_deal_id", "0") or 0)
        LOG.info("Сканирую сделки %s года, начиная после ID %s", self.year, last_id)
        while not self.stop:
            rows = self.call(
                "crm.deal.list",
                {
                    "filter": {
                        ">=DATE_CREATE": f"{self.year}-01-01T00:00:00+03:00",
                        "<DATE_CREATE": f"{self.year + 1}-01-01T00:00:00+03:00",
                        ">ID": last_id,
                    },
                    "order": {"ID": "ASC"},
                    "select": ["ID", "STAGE_ID", "TITLE", CATEGORY_FIELD, SUBCATEGORY_FIELD],
                    "start": 0,
                },
            ) or []
            candidates: list[
                tuple[dict[str, Any], tuple[str, str], tuple[str, str], str]
            ] = []
            product_evidence_ids: list[int] = []
            for deal in rows:
                deal_id = int(deal["ID"])
                last_id = max(last_id, deal_id)
                current = (
                    normalized(deal.get(CATEGORY_FIELD)),
                    normalized(deal.get(SUBCATEGORY_FIELD)),
                )
                target = self.approved_plan.target_for(
                    deal_id, self.allowed_pairs, current=current
                )
                if target is None:
                    continue
                desired = (target[0], target[1])
                evidence_mode = target[2]
                stage_id = normalized(deal.get("STAGE_ID"))
                candidates.append((deal, current, desired, evidence_mode))
                if (
                    evidence_mode == "products"
                    and stage_id not in self.excluded_stage_ids
                    and current != desired
                ):
                    product_evidence_ids.append(deal_id)

            product_evidence = self.fetch_product_evidence(product_evidence_ids)
            for deal, current, desired, evidence_mode in candidates:
                deal_id = int(deal["ID"])
                stage_id = normalized(deal.get("STAGE_ID"))
                if evidence_mode == "title":
                    evidence = normalized(deal.get("TITLE"))
                elif evidence_mode == "products":
                    evidence = product_evidence.get(deal_id, "")
                else:
                    evidence = ""
                if stage_id in self.excluded_stage_ids:
                    status = "excluded_stage"
                elif current == desired:
                    status = "verified"
                elif self.approved_plan.approves_transition(
                    deal_id, current, desired, evidence_mode, evidence
                ):
                    status = "pending"
                else:
                    status = "conflict"
                self.state.enqueue(
                    deal_id, current, desired, stage_id, status, evidence_mode
                )
            self.state.db.execute(
                "INSERT INTO meta(key,value) VALUES('last_deal_id',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(last_id),),
            )
            self.state.commit()
            self.write_status()
            discovered = self.state.db.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
            LOG.info("Сделки просканированы до ID %s; найдено строк плана: %s", last_id, discovered)
            if len(rows) < 50:
                self.state.set_meta("deals_scan_complete", 1)
                if discovered != self.approved_plan.count:
                    self.state.set_meta("scan_incomplete", 1)
                    LOG.warning(
                        "Обнаружено %s из %s строк согласованного плана; отсутствующие не записываются",
                        discovered,
                        self.approved_plan.count,
                    )
                self.write_status()
                return

    def fetch_live(self, deal_ids: list[int]) -> dict[int, tuple[str, str, str, str]]:
        if not deal_ids:
            return {}
        rows = self.call(
            "crm.deal.list",
            {
                "filter": {"@ID": deal_ids},
                "select": ["ID", "STAGE_ID", "TITLE", CATEGORY_FIELD, SUBCATEGORY_FIELD],
                "order": {"ID": "ASC"},
            },
        ) or []
        return {
            int(row["ID"]): (
                normalized(row.get(CATEGORY_FIELD)),
                normalized(row.get(SUBCATEGORY_FIELD)),
                normalized(row.get("STAGE_ID")),
                normalized(row.get("TITLE")),
            )
            for row in rows
        }

    def fetch_product_evidence(self, deal_ids: list[int]) -> dict[int, str]:
        if not deal_ids:
            return {}
        rows_by_deal: dict[int, list[dict[str, object]]] = {
            int(deal_id): [] for deal_id in deal_ids
        }
        start = 0
        while True:
            result = self.call(
                "crm.item.productrow.list",
                {
                    "filter": {"=ownerType": "D", "=ownerId": deal_ids},
                    "order": {"id": "ASC"},
                    "start": start,
                },
            ) or {}
            product_rows = result.get("productRows", []) if isinstance(result, dict) else []
            if not isinstance(product_rows, list):
                raise PermanentWorkerError("Bitrix24 вернул некорректные товарные строки")
            for row in product_rows:
                owner_id = int(row.get("ownerId") or 0)
                if owner_id in rows_by_deal:
                    rows_by_deal[owner_id].append(row)
            if len(product_rows) < 50:
                break
            start += len(product_rows)
        return {
            deal_id: canonical_product_evidence(product_rows)
            for deal_id, product_rows in rows_by_deal.items()
        }

    def classify_live(
        self,
        item: sqlite3.Row,
        live: tuple[str, str, str, str] | None,
        product_evidence: str | None = None,
    ) -> str:
        if live is None:
            return "missing"
        if live[2] in self.excluded_stage_ids:
            return "excluded_stage"
        current = live[:2]
        desired = (item["desired_category"], item["desired_subcategory"])
        original = (item["original_category"], item["original_subcategory"])
        if current == desired:
            return "verified"
        if current != original:
            return "conflict"
        evidence_mode = str(item["evidence_mode"])
        if evidence_mode == "title":
            evidence = live[3]
        elif evidence_mode == "products":
            if product_evidence is None:
                return "conflict"
            evidence = product_evidence
        else:
            evidence = ""
        if not self.approved_plan.approves_transition(
            int(item["deal_id"]), original, desired, evidence_mode, evidence
        ):
            return "conflict"
        return "write"

    def set_queue_status(
        self,
        deal_id: int,
        status: str,
        *,
        stage_id: str = "",
        error: str = "",
        attempts: int | None = None,
        next_attempt_at: float = 0,
    ) -> None:
        fields = ["status=?", "stage_id=?", "last_error=?", "next_attempt_at=?", "updated_at=?"]
        values: list[Any] = [status, stage_id, error[:1000], next_attempt_at, utc_now()]
        if attempts is not None:
            fields.append("attempts=?")
            values.append(attempts)
        values.append(deal_id)
        self.state.db.execute(f"UPDATE queue SET {','.join(fields)} WHERE deal_id=?", values)

    def recover_inflight(self) -> None:
        rows = self.state.db.execute(
            "SELECT * FROM queue WHERE status='inflight' ORDER BY deal_id"
        ).fetchall()
        for group in chunks(rows, 50):
            live = self.fetch_live([int(row["deal_id"]) for row in group])
            products = self.fetch_product_evidence(
                [
                    int(row["deal_id"])
                    for row in group
                    if row["evidence_mode"] == "products"
                ]
            )
            for row in group:
                deal_id = int(row["deal_id"])
                deal_live = live.get(deal_id)
                state = self.classify_live(row, deal_live, products.get(deal_id))
                if state == "write":
                    state = "pending"
                self.set_queue_status(
                    deal_id,
                    state,
                    stage_id=deal_live[2] if deal_live else "",
                )
            self.state.commit()

    def wait_for_write_slot(self) -> None:
        last = float(self.state.get_meta("last_write_started", "0") or 0)
        delay = last + self.write_interval - time.time()
        if delay > 0:
            self.sleep(delay)

    @staticmethod
    def _batch_errors(result: Any) -> dict[int, RuntimeError]:
        if not isinstance(result, dict):
            return {}
        errors = result.get("result_error") or {}
        parsed: dict[int, RuntimeError] = {}
        for command, value in errors.items():
            match = re.fullmatch(r"deal_(\d+)", str(command))
            if not match:
                continue
            if isinstance(value, dict):
                message = f"{value.get('error', '')}: {value.get('error_description', '')}".strip(": ")
            else:
                message = str(value)
            parsed[int(match.group(1))] = RuntimeError(message or "Неизвестная ошибка batch")
        return parsed

    def process_one_batch(self) -> bool:
        rows = self.state.db.execute(
            """
            SELECT * FROM queue
            WHERE status IN ('pending','retry_wait') AND next_attempt_at<=?
            ORDER BY attempts,deal_id LIMIT ?
            """,
            (time.time(), self.batch_size),
        ).fetchall()
        if not rows:
            return False

        # The final live guard is intentionally after the long write-slot wait.
        self.wait_for_write_slot()
        if time.time() - self.metadata_resolved_at >= 900:
            self.resolve_metadata()
        products = self.fetch_product_evidence(
            [
                int(row["deal_id"])
                for row in rows
                if row["evidence_mode"] == "products"
            ]
        )
        # Stage and category fields are fetched last, immediately before the
        # write decision, so metadata/evidence reads cannot stale this guard.
        live = self.fetch_live([int(row["deal_id"]) for row in rows])
        writable: list[sqlite3.Row] = []
        for row in rows:
            deal_id = int(row["deal_id"])
            deal_live = live.get(deal_id)
            state = self.classify_live(row, deal_live, products.get(deal_id))
            if state == "write":
                writable.append(row)
                self.set_queue_status(deal_id, "inflight", stage_id=deal_live[2])
            else:
                self.set_queue_status(
                    deal_id,
                    state,
                    stage_id=deal_live[2] if deal_live else "",
                )
        self.state.commit()
        if not writable:
            self.write_status()
            return True

        commands = {
            f"deal_{row['deal_id']}": "crm.deal.update?" + urlencode(
                [
                    ("id", str(row["deal_id"])),
                    (f"fields[{CATEGORY_FIELD}]", str(row["desired_category"])),
                    (f"fields[{SUBCATEGORY_FIELD}]", str(row["desired_subcategory"])),
                ]
            )
            for row in writable
        }
        self.state.set_meta("last_write_started", time.time())

        write_error: BaseException | None = None
        item_errors: dict[int, RuntimeError] = {}
        try:
            batch_result = self.call("batch", {"halt": 0, "cmd": commands})
            item_errors = self._batch_errors(batch_result)
        except Exception as exc:
            write_error = exc
            LOG.warning("Пакет не подтвердился: %s", exc)

        # A timed-out request can still complete on Bitrix24. Verification is
        # authoritative and always precedes a retry.
        self.sleep(3)
        verify_error: BaseException | None = None
        after_products: dict[int, str] = {}
        try:
            after = self.fetch_live([int(row["deal_id"]) for row in writable])
        except Exception as exc:
            after = {}
            verify_error = exc
        if verify_error is None:
            try:
                after_products = self.fetch_product_evidence(
                    [
                        int(row["deal_id"])
                        for row in writable
                        if row["evidence_mode"] == "products"
                    ]
                )
            except Exception as exc:
                verify_error = exc

        for row in writable:
            deal_id = int(row["deal_id"])
            deal_live = after.get(deal_id)
            needs_product = row["evidence_mode"] == "products"
            product_missing = needs_product and deal_id not in after_products
            current_is_desired = bool(
                deal_live
                and deal_live[:2]
                == (row["desired_category"], row["desired_subcategory"])
            )
            if verify_error is not None and (deal_live is None or (product_missing and not current_is_desired)):
                state = "unknown"
            else:
                state = self.classify_live(row, deal_live, after_products.get(deal_id))
            if state == "verified":
                self.set_queue_status(deal_id, "verified", stage_id=deal_live[2])
                continue
            if state in {"excluded_stage", "conflict", "missing"}:
                self.set_queue_status(
                    deal_id,
                    state,
                    stage_id=deal_live[2] if deal_live else "",
                )
                continue

            attempts = int(row["attempts"]) + 1
            error_value: BaseException | str = (
                item_errors.get(deal_id)
                or write_error
                or verify_error
                or "Bitrix24 не подтвердил обновление"
            )
            transient = is_transient_error(error_value)
            unconfirmed = not item_errors.get(deal_id) and write_error is None and verify_error is None
            if transient or (unconfirmed and attempts < MAX_UNCONFIRMED_ATTEMPTS):
                quota = is_quota_error(error_value)
                delay = retry_delay(attempts, quota=quota)
                self.set_queue_status(
                    deal_id,
                    "retry_wait",
                    attempts=attempts,
                    next_attempt_at=time.time() + delay,
                    error=str(error_value),
                    stage_id=deal_live[2] if deal_live else "",
                )
                if quota:
                    self.state.set_meta("cooldown_until", time.time() + delay)
            else:
                self.set_queue_status(
                    deal_id,
                    "permanent_error",
                    attempts=attempts,
                    error=str(error_value),
                    stage_id=deal_live[2] if deal_live else "",
                )
        self.state.commit()
        self.write_status()
        LOG.info("Состояние очереди: %s", self.state.counts())
        return True

    def write_status(self, error: str = "") -> None:
        discovered = int(self.state.db.execute("SELECT COUNT(*) FROM queue").fetchone()[0])
        payload = {
            "year": self.year,
            "updated_at": utc_now(),
            "plan_total": self.approved_plan.count,
            "discovered": discovered,
            "undiscovered": max(0, self.approved_plan.count - discovered),
            "scan_complete": self.state.get_meta("deals_scan_complete") == "1",
            "scan_incomplete": self.state.get_meta("scan_incomplete") == "1",
            "queue": self.state.counts(),
            "last_error": error,
        }
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.status_path)

    def initialize(self) -> None:
        while not self.stop:
            try:
                self.resolve_metadata()
                self.state.bind_identity(self.identity)
                self.scan_deals()
                self.recover_inflight()
                self.write_status()
                return
            except StopRequested:
                raise
            except Exception as exc:
                LOG.exception("Инициализация worker приостановлена: %s", exc)
                self.write_status(str(exc))
                if is_transient_error(exc):
                    delay = retry_delay(1, quota=is_quota_error(exc))
                    self.state.set_meta("cooldown_until", time.time() + delay)
                else:
                    delay = 3600.0
                self.sleep(delay)

    def run(self) -> None:
        try:
            self.initialize()
            while not self.stop:
                try:
                    did_work = self.process_one_batch()
                    if did_work:
                        continue
                    counts = self.state.counts()
                    if not any(counts.get(status, 0) for status in ("pending", "retry_wait", "inflight")):
                        LOG.info("Обработка завершена: %s", counts)
                        self.write_status()
                        self.sleep(3600)
                    else:
                        next_row = self.state.db.execute(
                            "SELECT MIN(next_attempt_at) FROM queue WHERE status='retry_wait'"
                        ).fetchone()[0]
                        delay = max(5.0, min(60.0, float(next_row or 0) - time.time()))
                        self.sleep(delay)
                except StopRequested:
                    break
                except Exception as exc:
                    LOG.exception("Временная ошибка worker: %s", exc)
                    self.write_status(str(exc))
                    delay = retry_delay(1, quota=is_quota_error(exc))
                    if is_transient_error(exc):
                        self.state.set_meta("cooldown_until", time.time() + delay)
                    else:
                        delay = 300.0
                    self.sleep(delay)
        except StopRequested:
            pass
        finally:
            self.write_status("Остановлен по сигналу" if self.stop else "")


@contextmanager
def single_instance(lock_path: Path, wait_for_lock: bool = False):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    os.chmod(lock_path, 0o600)
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError as exc:
            if not wait_for_lock:
                handle.close()
                raise RuntimeError("precision worker уже запущен") from exc
            LOG.info("Ожидаю освобождения блокировки действующим worker")
            time.sleep(1)
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        yield
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


def main() -> None:
    data_dir = Path(__file__).with_name("data")
    parser = argparse.ArgumentParser(description="Устойчивое заполнение категорий сделок Bitrix24")
    parser.add_argument("--year", type=int, default=int(os.getenv("PRECISION_YEAR", "2025")))
    parser.add_argument("--state", default=os.getenv("PRECISION_STATE_PATH", "/data/precision.sqlite3"))
    parser.add_argument("--status", default=os.getenv("PRECISION_STATUS_PATH", "/data/precision-status.json"))
    parser.add_argument("--lock", default=os.getenv("PRECISION_LOCK_PATH", ""))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("PRECISION_BATCH_SIZE", "20")))
    parser.add_argument(
        "--write-interval",
        type=float,
        default=float(os.getenv("PRECISION_WRITE_INTERVAL_SECONDS", "60")),
    )
    parser.add_argument(
        "--allowlist",
        default=os.getenv("PRECISION_ALLOWLIST_PATH", str(data_dir / "precision-2025.allowlist")),
    )
    parser.add_argument(
        "--taxonomy",
        default=os.getenv("PRECISION_TAXONOMY_PATH", str(data_dir / "precision-2025-taxonomy.json")),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-config", action="store_true")
    mode.add_argument("--scan-only", action="store_true")
    parser.add_argument("--wait-for-lock", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    webhook = os.getenv("BITRIX_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("Не задан BITRIX_WEBHOOK_URL")
    portal = portal_identity(webhook)
    state_path = Path(args.state)
    configured_plan_secret = os.getenv("PRECISION_PLAN_KEY", "").strip()
    plan_key_path = Path(
        os.getenv("PRECISION_PLAN_KEY_PATH", str(state_path.with_name("precision-plan.key")))
    )
    should_persist_plan_key = False
    if configured_plan_secret:
        plan_key = derive_plan_key(configured_plan_secret, portal)
    elif plan_key_path.exists():
        plan_key = cached_plan_key(plan_key_path)
    else:
        plan_key = derive_plan_key(webhook, portal)
        should_persist_plan_key = True
    approved = ApprovedPlan.load(
        Path(args.allowlist),
        plan_key,
        args.year,
        portal,
    )
    if should_persist_plan_key:
        persist_plan_key(plan_key_path, plan_key)
    categories, subcategories, pairs, taxonomy_digest = load_taxonomy(
        Path(args.taxonomy), args.year
    )
    identity = {
        "schema": STATE_SCHEMA_VERSION,
        "portal": portal,
        "year": args.year,
        "category_field": CATEGORY_FIELD,
        "subcategory_field": SUBCATEGORY_FIELD,
        "plan": approved.digest,
        "taxonomy": taxonomy_digest,
    }
    timeout = int(os.getenv("PRECISION_HTTP_TIMEOUT", os.getenv("HTTP_TIMEOUT", "90")))
    min_interval = float(os.getenv("PRECISION_API_INTERVAL_SECONDS", "1.2"))
    lock_path = Path(args.lock) if args.lock else state_path.with_suffix(".lock")

    with single_instance(lock_path, wait_for_lock=args.wait_for_lock):
        state = State(state_path)
        worker: PrecisionWorker | None = None
        try:
            worker = PrecisionWorker(
                state=state,
                bitrix=RateLimitedBitrix(
                    webhook,
                    timeout,
                    min_interval,
                    lambda: bool(worker and worker.stop),
                ),
                approved_plan=approved,
                expected_categories=categories,
                expected_subcategories=subcategories,
                allowed_pairs=pairs,
                identity=identity,
                year=args.year,
                batch_size=args.batch_size,
                write_interval=args.write_interval,
                status_path=Path(args.status),
            )
            signal.signal(signal.SIGTERM, worker.request_stop)
            signal.signal(signal.SIGINT, worker.request_stop)
            if args.check_config:
                worker.resolve_metadata()
                state.bind_identity(identity)
                worker.write_status()
                LOG.info("Конфигурация worker проверена; CRM не изменялась")
            elif args.scan_only:
                worker.resolve_metadata()
                state.bind_identity(identity)
                worker.scan_deals()
                worker.recover_inflight()
                worker.write_status()
                LOG.info("Согласованный план просканирован; CRM не изменялась")
            else:
                worker.run()
        finally:
            state.close()


if __name__ == "__main__":
    main()
