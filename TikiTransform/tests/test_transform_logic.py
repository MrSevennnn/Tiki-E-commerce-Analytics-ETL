"""
Unit Tests for Tiki Data Transformation Logic
Vietnam E-commerce Analytics Platform - Phase 1

Run tests:
    python -m pytest tests/test_transform_logic.py -v
    python tests/test_transform_logic.py
"""

import unittest
import sys
import os
from datetime import date

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.transform_tiki import (
    parse_sales_volume,
    parse_discount_rate,
    extract_category_id,
    clean_price,
    parse_snapshot_date,
)


class TestParseSalesVolume(unittest.TestCase):
    """Test cases for parse_sales_volume function."""
    
    def test_simple_number(self):
        """Test parsing simple numbers."""
        self.assertEqual(parse_sales_volume("Đã bán 100"), 100)
        self.assertEqual(parse_sales_volume("Đã bán 1"), 1)
        self.assertEqual(parse_sales_volume("Đã bán 999"), 999)
    
    def test_thousands_k_suffix(self):
        """Test parsing numbers with 'k' suffix (thousands)."""
        self.assertEqual(parse_sales_volume("Đã bán 1k"), 1000)
        self.assertEqual(parse_sales_volume("Đã bán 1.5k"), 1500)
        self.assertEqual(parse_sales_volume("Đã bán 2,5k"), 2500)
        self.assertEqual(parse_sales_volume("Đã bán 10k"), 10000)
        self.assertEqual(parse_sales_volume("Đã bán 1.2K"), 1200)
    
    def test_millions_suffix(self):
        """Test parsing numbers with 'tr' or 'm' suffix (millions)."""
        self.assertEqual(parse_sales_volume("Đã bán 1tr"), 1000000)
        self.assertEqual(parse_sales_volume("Đã bán 2.5tr"), 2500000)
        self.assertEqual(parse_sales_volume("Đã bán 1m"), 1000000)
        self.assertEqual(parse_sales_volume("Đã bán 1.5 triệu"), 1500000)
    
    def test_null_and_empty_values(self):
        """Test handling of null and empty values."""
        self.assertEqual(parse_sales_volume(None), 0)
        self.assertEqual(parse_sales_volume(""), 0)
        self.assertEqual(parse_sales_volume("Đã bán"), 0)
        self.assertEqual(parse_sales_volume("No sales"), 0)
    
    def test_numeric_input(self):
        """Test handling of numeric input (already parsed)."""
        self.assertEqual(parse_sales_volume(100), 100)
        self.assertEqual(parse_sales_volume(1500.0), 1500)
    
    def test_edge_cases(self):
        """Test edge cases and unusual formats."""
        self.assertEqual(parse_sales_volume("1.5k sold"), 1500)
        self.assertEqual(parse_sales_volume("sold 500"), 500)
        self.assertEqual(parse_sales_volume("  Đã bán 200  "), 200)


class TestParseDiscountRate(unittest.TestCase):
    """Test cases for parse_discount_rate function."""
    
    def test_percentage_with_minus(self):
        """Test parsing discount rates with minus sign."""
        self.assertEqual(parse_discount_rate("-41%"), 41)
        self.assertEqual(parse_discount_rate("-25%"), 25)
        self.assertEqual(parse_discount_rate("-10%"), 10)
    
    def test_percentage_without_minus(self):
        """Test parsing discount rates without minus sign."""
        self.assertEqual(parse_discount_rate("41%"), 41)
        self.assertEqual(parse_discount_rate("25%"), 25)
    
    def test_number_only(self):
        """Test parsing discount rates as plain numbers."""
        self.assertEqual(parse_discount_rate("-25"), 25)
        self.assertEqual(parse_discount_rate("30"), 30)
    
    def test_null_and_empty_values(self):
        """Test handling of null and empty values."""
        self.assertEqual(parse_discount_rate(None), 0)
        self.assertEqual(parse_discount_rate(""), 0)
        self.assertEqual(parse_discount_rate("N/A"), 0)
    
    def test_numeric_input(self):
        """Test handling of numeric input."""
        self.assertEqual(parse_discount_rate(41), 41)
        self.assertEqual(parse_discount_rate(-25), 25)
        self.assertEqual(parse_discount_rate(30.5), 30)


class TestExtractCategoryId(unittest.TestCase):
    """Test cases for extract_category_id function."""
    
    def test_standard_tiki_url(self):
        """Test extracting category ID from standard Tiki URLs."""
        self.assertEqual(
            extract_category_id("https://tiki.vn/dien-thoai-may-tinh-bang/c1789"),
            1789
        )
        self.assertEqual(
            extract_category_id("https://tiki.vn/laptop-may-vi-tinh-linh-kien/c1846"),
            1846
        )
        self.assertEqual(
            extract_category_id("https://tiki.vn/tai-nghe/c8318"),
            8318
        )
    
    def test_url_with_query_params(self):
        """Test extracting category ID from URLs with query parameters."""
        self.assertEqual(
            extract_category_id("https://tiki.vn/dien-thoai-may-tinh-bang/c1789?page=2"),
            1789
        )
        self.assertEqual(
            extract_category_id("https://tiki.vn/category/c1234?sort=price&order=asc"),
            1234
        )
    
    def test_null_and_invalid_urls(self):
        """Test handling of null and invalid URLs."""
        self.assertIsNone(extract_category_id(None))
        self.assertIsNone(extract_category_id(""))
        self.assertIsNone(extract_category_id("https://tiki.vn/home"))
        self.assertIsNone(extract_category_id("not a url"))
    
    def test_edge_cases(self):
        """Test edge cases for category ID extraction."""
        # URL with multiple /c patterns (should match first)
        self.assertEqual(
            extract_category_id("/c123/subcategory/c456"),
            123
        )
        # Just the category part
        self.assertEqual(
            extract_category_id("/c9999"),
            9999
        )


class TestCleanPrice(unittest.TestCase):
    """Test cases for clean_price function."""
    
    def test_numeric_values(self):
        """Test handling of numeric values."""
        self.assertEqual(clean_price(1000000), 1000000)
        self.assertEqual(clean_price(3520000.0), 3520000)
        self.assertEqual(clean_price(500), 500)
    
    def test_string_values(self):
        """Test handling of string values with currency symbols."""
        self.assertEqual(clean_price("1000000"), 1000000)
        self.assertEqual(clean_price("1,000,000"), 1000000)
        self.assertEqual(clean_price("1.000.000 VND"), 1000000)
        self.assertEqual(clean_price("$1000"), 1000)
    
    def test_null_values(self):
        """Test handling of null values."""
        self.assertIsNone(clean_price(None))
        self.assertIsNone(clean_price(""))
        self.assertIsNone(clean_price("N/A"))


class TestParseSnapshotDate(unittest.TestCase):
    """Test cases for parse_snapshot_date function."""
    
    def test_iso_format_with_z(self):
        """Test parsing ISO format with Z suffix."""
        result = parse_snapshot_date("2026-01-18T16:49:55.805Z")
        self.assertEqual(result, date(2026, 1, 18))
    
    def test_iso_format_without_z(self):
        """Test parsing ISO format without Z suffix."""
        result = parse_snapshot_date("2026-01-21T10:30:00")
        self.assertEqual(result, date(2026, 1, 21))
    
    def test_iso_format_with_timezone(self):
        """Test parsing ISO format with timezone offset."""
        result = parse_snapshot_date("2026-01-22T08:00:00+07:00")
        self.assertEqual(result, date(2026, 1, 22))
    
    def test_null_values(self):
        """Test handling of null values."""
        self.assertIsNone(parse_snapshot_date(None))
        self.assertIsNone(parse_snapshot_date(""))
    
    def test_invalid_format(self):
        """Test handling of invalid date formats."""
        self.assertIsNone(parse_snapshot_date("not a date"))
        self.assertIsNone(parse_snapshot_date("2026/01/21"))


class TestIntegration(unittest.TestCase):
    """Integration tests for transformation logic."""
    
    def test_full_transformation_flow(self):
        """Test that all transformations work together correctly."""
        import pandas as pd
        
        # Sample raw data (simulating one product)
        raw_data = {
            'product_id': '278628866',
            'seller_product_id': '278630782',
            'name': 'Apple iPhone 17 Pro Max',
            'brand_name': 'Apple',
            'seller_name': 'Tiki Trading',
            'seller_id': 1,
            'price': 37250000,
            'original_price': 37990000,
            'discount_rate': '-2%',
            'sales_volume': 'Đã bán 1.5k',
            'rating_average': 5.0,
            'review_count': 47,
            'tiki_now': False,
            'product_url': 'https://tiki.vn/apple-iphone-17-pro-max-p278628866.html',
            'image_url': 'https://salt.tikicdn.com/cache/280x280/ts/product/85/50/d4/test.jpg',
            '_extracted_at': '2026-01-18T16:49:55.805Z',
            '_source_page': 7,
            '_category_url': 'https://tiki.vn/dien-thoai-may-tinh-bang/c1789',
            '_category_name': 'dien-thoai-may-tinh-bang',
        }
        
        # Apply transformations
        self.assertEqual(parse_sales_volume(raw_data['sales_volume']), 1500)
        self.assertEqual(parse_discount_rate(raw_data['discount_rate']), 2)
        self.assertEqual(extract_category_id(raw_data['_category_url']), 1789)
        self.assertEqual(clean_price(raw_data['price']), 37250000)
        self.assertEqual(
            parse_snapshot_date(raw_data['_extracted_at']),
            date(2026, 1, 18)
        )


class TestSalesVolumeEdgeCases(unittest.TestCase):
    """Additional edge case tests for sales volume parsing."""
    
    def test_vietnamese_formats(self):
        """Test various Vietnamese format variations."""
        test_cases = [
            ("Đã bán 456", 456),
            ("Đã bán 1,5k", 1500),
            ("Đã bán 2.5k", 2500),
            ("Đã bán 10.000", 10000),  # This should parse as 10.0 = 10
            ("Đã bán 3433", 3433),
            ("đã bán 100", 100),  # lowercase
            ("DA BAN 500", 500),  # uppercase (without diacritics)
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input=input_val):
                result = parse_sales_volume(input_val)
                self.assertEqual(result, expected, f"Failed for input: {input_val}")


def run_quick_tests():
    """Run quick smoke tests without unittest framework."""
    print("=" * 60)
    print("QUICK SMOKE TESTS")
    print("=" * 60)
    
    # Test parse_sales_volume
    print("\n[TEST] parse_sales_volume:")
    assert parse_sales_volume("Đã bán 1.5k") == 1500, "Failed: 1.5k"
    assert parse_sales_volume("Đã bán 100") == 100, "Failed: 100"
    assert parse_sales_volume(None) == 0, "Failed: None"
    print("  [PASS] All sales_volume tests passed")
    
    # Test parse_discount_rate
    print("\n[TEST] parse_discount_rate:")
    assert parse_discount_rate("-41%") == 41, "Failed: -41%"
    assert parse_discount_rate(None) == 0, "Failed: None"
    print("  [PASS] All discount_rate tests passed")
    
    # Test extract_category_id
    print("\n[TEST] extract_category_id:")
    assert extract_category_id("https://tiki.vn/dien-thoai-may-tinh-bang/c1789") == 1789, "Failed: c1789"
    assert extract_category_id(None) is None, "Failed: None"
    print("  [PASS] All category_id tests passed")
    
    # Test clean_price
    print("\n[TEST] clean_price:")
    assert clean_price(37250000) == 37250000, "Failed: numeric"
    assert clean_price(None) is None, "Failed: None"
    print("  [PASS] All clean_price tests passed")
    
    # Test parse_snapshot_date
    print("\n[TEST] parse_snapshot_date:")
    assert parse_snapshot_date("2026-01-18T16:49:55.805Z") == date(2026, 1, 18), "Failed: ISO date"
    assert parse_snapshot_date(None) is None, "Failed: None"
    print("  [PASS] All snapshot_date tests passed")
    
    print("\n" + "=" * 60)
    print("[SUCCESS] All smoke tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    # Run quick smoke tests first
    run_quick_tests()
    
    # Then run full unittest suite
    print("\n" + "=" * 60)
    print("RUNNING FULL UNITTEST SUITE")
    print("=" * 60 + "\n")
    
    unittest.main(verbosity=2)
