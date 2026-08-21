import unittest

from classifier.ai import CallAnalysis
from classifier.bitrix import BitrixClient, DealField
from classifier.catalog import ProductGroup, _category_path
from classifier.service import extract_event
from classifier.payload import decode_payload


class FakeBitrix:
    def get_activity(self, activity_id):
        return {
            "ID": str(activity_id),
            "DESCRIPTION": "Клиенту нужен штабелер грузоподъемностью 1,5 тонны",
            "BINDINGS": [{"OWNER_TYPE_ID": 2, "OWNER_ID": 321}],
        }


class ActivityReadBitrix(BitrixClient):
    def __init__(self, bindings, *, detail=None):
        super().__init__("https://example.bitrix24.test/rest/1/test-token/")
        self.bindings = list(bindings)
        self.detail = detail or {
            "ID": "99",
            "DESCRIPTION": "Точный текст activity",
        }
        self.calls = []

    def call(self, method, params=None):
        params = params or {}
        self.calls.append((method, params))
        if method == "crm.activity.get":
            return self.detail
        if method == "crm.activity.binding.list":
            start = int(params["start"])
            return self.bindings[start:start + 50]
        raise AssertionError(f"unexpected method: {method}")


class FakeBackfillBitrix:
    def get_deal(self, deal_id):
        return {"ID": str(deal_id)}

    def list_deal_calls(self, deal_id):
        return [{"ID": "20"}, {"ID": "10"}]

    def get_activity(self, activity_id):
        return {
            "ID": str(activity_id),
            "BINDINGS": [{"OWNER_TYPE_ID": 2, "OWNER_ID": 123456}],
        }

    def get_call_transcript(self, activity_id):
        return None if activity_id == 20 else "Клиенту требуется болгарский электрический тельфер грузоподъемностью одна тонна."

    def resolve_deal_field(self, name, configured_field_name=""):
        return DealField("UF_" + name, "string", {})

    def update_deal(self, deal_id, fields):
        raise AssertionError("dry-run не должен изменять сделку")

    def add_timeline_comment(self, deal_id, comment):
        raise AssertionError("dry-run не должен писать комментарий")


class FakeAnalyzer:
    def analyze(self, transcript, groups):
        return CallAnalysis("Запрос на болгарский электротельфер 1 т", "Клиент запросил электротельфер и ожидает ответ.", True, "Грузоподъемное оборудование", "Электрические тали")


class CoreTests(unittest.TestCase):
    def test_category_path(self):
        nodes = {"1": ("Складская техника", None), "2": ("Штабелеры", "1")}
        self.assertEqual(_category_path("2", nodes), ["Складская техника", "Штабелеры"])

    def test_enumeration_encoding_is_case_insensitive(self):
        field = DealField("UF_CRM_1", "enumeration", {"складская техника": "77"})
        self.assertEqual(field.encode("Складская техника"), "77")

    def test_extract_direct_event(self):
        self.assertEqual(extract_event({"deal_id": 12, "transcript": "Достаточно длинная расшифровка звонка"}, FakeBitrix()), (12, "Достаточно длинная расшифровка звонка"))

    def test_extract_activity_event(self):
        deal_id, transcript = extract_event({"activity_id": 99}, FakeBitrix())
        self.assertEqual(deal_id, 321)
        self.assertIn("штабелер", transcript)

    def test_activity_read_hydrates_all_binding_pages_and_explicit_eof(self):
        bindings = [
            {"entityTypeId": 2, "entityId": 321},
            *(
                {"entityTypeId": 3, "entityId": value}
                for value in range(1, 100)
            ),
        ]
        client = ActivityReadBitrix(bindings)
        activity = client.get_activity(99)
        self.assertEqual(len(activity["BINDINGS"]), 100)
        self.assertEqual(activity["BINDINGS"][0]["OWNER_ID"], 321)
        self.assertEqual(
            [
                params["start"]
                for method, params in client.calls
                if method == "crm.activity.binding.list"
            ],
            [0, 50, 100],
        )

    def test_activity_read_rejects_wrong_identity_and_invalid_bindings(self):
        wrong_identity = ActivityReadBitrix(
            [{"entityTypeId": 2, "entityId": 321}],
            detail={"ID": "100", "DESCRIPTION": "Текст"},
        )
        with self.assertRaisesRegex(RuntimeError, "другим ID"):
            wrong_identity.get_activity(99)
        self.assertFalse(
            any(
                method == "crm.activity.binding.list"
                for method, _params in wrong_identity.calls
            )
        )

        exact = [
            {"entityTypeId": 2, "entityId": 321},
            *(
                {"entityTypeId": 3, "entityId": value}
                for value in range(1, 100)
            ),
        ]
        invalid = (
            [*exact, {"entityTypeId": 4, "entityId": 999}],
            [*exact[:-1], exact[0]],
            [{"entityTypeId": 2, "entityId": 0}],
            [{"entityTypeId": "2", "entityId": 321}],
            [
                {
                    "entityTypeId": 2,
                    "entityId": 321,
                    "OWNER_ID": 999,
                }
            ],
        )
        for bindings in invalid:
            with self.subTest(bindings_count=len(bindings)):
                with self.assertRaises(RuntimeError):
                    ActivityReadBitrix(bindings).get_activity(99)

    def test_extract_activity_event_requires_unambiguous_matching_tuple(self):
        class BoundActivity:
            def __init__(self, bindings):
                self.bindings = bindings

            def get_activity(self, activity_id):
                return {
                    "ID": str(activity_id),
                    "DESCRIPTION": "Точный текст activity",
                    "BINDINGS": self.bindings,
                }

        multiple = BoundActivity(
            [
                {"OWNER_TYPE_ID": 2, "OWNER_ID": 321},
                {"OWNER_TYPE_ID": 2, "OWNER_ID": 654},
            ]
        )
        with self.assertRaisesRegex(ValueError, "неоднозначная"):
            extract_event({"activity_id": 99}, multiple)
        self.assertEqual(
            extract_event(
                {
                    "activity_id": 99,
                    "deal_id": 654,
                    "transcript": "Точный текст activity",
                },
                multiple,
            ),
            (654, "Точный текст activity"),
        )
        with self.assertRaisesRegex(ValueError, "указанной сделке"):
            extract_event(
                {
                    "activity_id": 99,
                    "deal_id": 777,
                    "transcript": "Точный текст activity",
                },
                multiple,
            )
        with self.assertRaisesRegex(ValueError, "не совпадает"):
            extract_event(
                {
                    "activity_id": 99,
                    "deal_id": 321,
                    "transcript": "Другой текст",
                },
                multiple,
            )
        with self.assertRaisesRegex(ValueError, "не привязана"):
            extract_event(
                {"activity_id": 99},
                BoundActivity(
                    [{"OWNER_TYPE_ID": 3, "OWNER_ID": 321}]
                ),
            )

    def test_decode_bitrix_form_payload(self):
        payload = decode_payload(b"data%5BFIELDS%5D%5BID%5D=99", "application/x-www-form-urlencoded")
        self.assertEqual(payload["data[FIELDS][ID]"], "99")

    def test_backfill_uses_latest_call_with_transcript(self):
        from classifier.service import CallProcessingService

        service = CallProcessingService(
            FakeBackfillBitrix(), FakeAnalyzer(),
            [ProductGroup("Грузоподъемное оборудование", "Электрические тали")],
            "Категория товаров (Сайт)", "Подкатегория товаров (Сайт)",
        )
        result = service.process_existing_deal(123456, dry_run=True)
        self.assertEqual(result.activity_id, 10)
        self.assertIn("электротельфер", result.analysis.title)

    def test_backfill_rejects_call_without_exact_deal_binding(self):
        from classifier.service import CallProcessingService

        class ForeignCallBitrix(FakeBackfillBitrix):
            def get_activity(self, activity_id):
                return {
                    "ID": str(activity_id),
                    "BINDINGS": [
                        {"OWNER_TYPE_ID": 2, "OWNER_ID": 999999}
                    ],
                }

        service = CallProcessingService(
            ForeignCallBitrix(), FakeAnalyzer(),
            [ProductGroup("Грузоподъемное оборудование", "Электрические тали")],
            "Категория товаров (Сайт)", "Подкатегория товаров (Сайт)",
        )
        with self.assertRaisesRegex(ValueError, "не привязан"):
            service.process_existing_deal(123456, dry_run=True)


if __name__ == "__main__":
    unittest.main()
