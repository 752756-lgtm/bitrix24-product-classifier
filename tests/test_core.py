import unittest

from classifier.ai import CallAnalysis
from classifier.bitrix import DealField
from classifier.catalog import ProductGroup, _category_path
from classifier.service import extract_event
from classifier.payload import decode_payload


class FakeBitrix:
    def get_activity(self, activity_id):
        return {"DESCRIPTION": "Клиенту нужен штабелер грузоподъемностью 1,5 тонны", "BINDINGS": [{"OWNER_TYPE_ID": 2, "OWNER_ID": 321}]}


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

    def test_decode_bitrix_form_payload(self):
        payload = decode_payload(b"data%5BFIELDS%5D%5BID%5D=99", "application/x-www-form-urlencoded")
        self.assertEqual(payload["data[FIELDS][ID]"], "99")


if __name__ == "__main__":
    unittest.main()
