from decimal import Decimal
import unittest

from classifier.classify import classify_products
from classifier.domain import DealProduct, Offer
from classifier.yml_feed import YmlCatalog, normalize_name, parse_yml


class ClassifierTests(unittest.TestCase):
    def test_selects_category_with_largest_amount(self):
        catalog = YmlCatalog(
            [
                Offer("10", "Рохля", "Складская техника", "Тележки"),
                Offer("20", "Таль", "Грузоподъемное", "Тали"),
            ]
        )
        products = [
            DealProduct("1", "10", "Другое имя", Decimal("100"), Decimal("3")),
            DealProduct("2", "20", "Таль", Decimal("250"), Decimal("1")),
        ]
        result = classify_products(products, catalog)
        self.assertIsNotNone(result)
        self.assertEqual(result.category, "Складская техника")
        self.assertEqual(result.amount, Decimal("300"))

    def test_falls_back_to_normalized_name(self):
        catalog = YmlCatalog([Offer("10", "  РОХЛЯ 2,5 т ", "Склад", "Тележки")])
        product = DealProduct("1", "", "рохля 2,5 т", Decimal("1"), Decimal("1"))
        self.assertEqual(classify_products([product], catalog).subcategory, "Тележки")
        self.assertEqual(normalize_name("  РОХЛЯ  "), "рохля")

    def test_returns_none_without_matches(self):
        product = DealProduct("1", "x", "Нет", Decimal("1"), Decimal("1"))
        self.assertIsNone(classify_products([product], YmlCatalog([])))

    def test_parses_category_hierarchy(self):
        content = b'''<?xml version="1.0" encoding="utf-8"?>
        <yml_catalog><shop><categories>
          <category id="1">Warehouse</category>
          <category id="2" parentId="1">Pallet trucks</category>
        </categories><offers><offer id="sku-1">
          <name>Truck</name><categoryId>2</categoryId>
        </offer></offers></shop></yml_catalog>'''
        offer = parse_yml(content).find("sku-1", "")
        self.assertEqual((offer.category, offer.subcategory), ("Warehouse", "Pallet trucks"))


if __name__ == "__main__":
    unittest.main()

