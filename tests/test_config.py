import os
import unittest
from unittest.mock import patch

from classifier.config import Settings


class ConfigTests(unittest.TestCase):
    def test_requires_all_core_values(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "BITRIX_WEBHOOK_URL"):
                Settings.from_env()

    def test_uses_default_field_titles(self):
        env = {
            "BITRIX_WEBHOOK_URL": "https://x/rest/1/t/",
            "YML_URL": "https://x/feed.xml",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = Settings.from_env()
            self.assertEqual(settings.category_field_title, "Категория товаров (Сайт)")
            self.assertEqual(settings.subcategory_field_title, "Подкатегория товаров (Сайт)")


if __name__ == "__main__":
    unittest.main()
