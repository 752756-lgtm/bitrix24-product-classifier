import os
import unittest
from unittest.mock import patch

from classifier.config import Settings


class ConfigTests(unittest.TestCase):
    def test_requires_all_core_values(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "BITRIX_WEBHOOK_URL"):
                Settings.from_env()

    def test_parses_list_value_maps(self):
        env = {
            "BITRIX_WEBHOOK_URL": "https://x/rest/1/t/",
            "YML_URL": "https://x/feed.xml",
            "CATEGORY_FIELD_ID": "UF_A",
            "SUBCATEGORY_FIELD_ID": "UF_B",
            "CATEGORY_VALUE_MAP": '{"Склад":"42"}',
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(Settings.from_env().category_value_map["Склад"], "42")


if __name__ == "__main__":
    unittest.main()

