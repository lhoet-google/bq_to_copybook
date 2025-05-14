import unittest
from unittest.mock import Mock
from google.cloud.bigquery import SchemaField

# --- IMPORT THE SCRIPT TO BE TESTED ---
# Assumes the script is saved as bq_to_copybook.py
import bq_to_copybook
# -----------------------------------------


class TestCopybookGenerator(unittest.TestCase):
    def setUp(self):
        """Set up a default Mock Table and Config before each test."""
        self.mock_table = Mock()
        self.mock_table.project = "mock-project"
        self.mock_table.dataset_id = "mock_dataset"
        self.mock_table.table_id = "mock_table"

        # Default config, matching the script's defaults
        self.config = {"default_occurs": 25, "default_string_len": 256}

    def assertLinesInResult(self, expected_lines, result_string):
        """
        Helper to check that all expected lines (stripped of whitespace)
        are present in the result_string (also checked line-by-line, stripped)
        """
        actual_lines = {line.strip() for line in result_string.split("\n")}

        for expected in expected_lines:
            self.assertIn(
                expected.strip(), actual_lines, f"\nEXPECTED LINE:\n>>> {expected.strip()} <<<\nNOT FOUND IN ACTUAL OUTPUT.\n"
            )

    def test_flat_schema_basic_types(self):
        """
        Tests a flat structure with common BQ types
        and correct PIC/USAGE clauses.
        """
        # ARRANGE
        self.mock_table.schema = [
            SchemaField("user_id", "STRING", "NULLABLE"),
            SchemaField("notes", "STRING", "NULLABLE"),
            SchemaField("event_count", "INT64", "NULLABLE"),
            SchemaField("amount", "NUMERIC", "NULLABLE", precision=15, scale=2),  # P=15, S=2
            SchemaField("is_active", "BOOLEAN", "NULLABLE"),
            SchemaField("processing_date", "DATE", "NULLABLE"),
            SchemaField("score", "FLOAT64", "NULLABLE"),
        ]

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "FLAT-REC", self.config)

        # ASSERT
        expected = [
            "       01  FLAT-REC.",
            "       05  USER-ID                         PIC X(50).",
            "       05  NOTES                           PIC X(256).",  # Default
            "       05  EVENT-COUNT                     PIC S9(18) COMP.",
            "       05  AMOUNT                          PIC S9(13)V9(2) COMP-3.",
            "       05  IS-ACTIVE                       PIC X(1).",
            "       05  PROCESSING-DATE                 PIC X(10).",
            "       05  SCORE                           PIC S9(18)V9(06).",
        ]

        self.assertTrue(result.strip().startswith("******"))  # Header exists
        self.assertLinesInResult(expected, result)

    def test_nested_struct(self):
        """Tests a nested RECORD/STRUCT."""
        address_fields = (
            SchemaField("street_line", "STRING", "NULLABLE", max_length=100),
            SchemaField("zip_code", "INTEGER", "NULLABLE"),
        )
        self.mock_table.schema = [
            SchemaField("transaction_id", "STRING", "NULLABLE", max_length=36),
            SchemaField("billing_address", "STRUCT", "NULLABLE"),
        ]

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "NESTED-REC", self.config)

        # ASSERT
        expected = [
            "       01  NESTED-REC.",
            "       05  TRANSACTION-ID                  PIC X(36).",
            "       05  BILLING-ADDRESS.",
            "           10  STREET-LINE                 PIC X(100).",
            "           10  ZIP-CODE                    PIC S9(18) COMP.",
        ]
        self.assertLinesInResult(expected, result)

    def test_repeated_simple_type(self):
        """Tests a REPEATED scalar field (OCCURS)."""
        # ARRANGE
        self.mock_table.schema = [
            SchemaField("item_id", "INTEGER", "REPEATED"),
        ]

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "ARRAY-REC", self.config)

        # ASSERT
        expected = [
            "       01  ARRAY-REC.",
            "       05  ITEM-ID                         PIC S9(18) COMP OCCURS 25 TIMES.",
        ]
        self.assertLinesInResult(expected, result)

    def test_repeated_struct(self):
        """Tests a REPEATED STRUCT field (OCCURS on a group item)."""
        # ARRANGE
        item_fields = (
            SchemaField("sku", "STRING", "NULLABLE"),
            SchemaField("quantity", "INTEGER", "NULLABLE"),
        )
        self.mock_table.schema = [
            SchemaField("order_id", "STRING", "NULLABLE", max_length=30),
            SchemaField("line_items", "STRUCT", "REPEATED"),
        ]

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "ORDER-REC", self.config)

        # ASSERT
        expected = [
            "       01  ORDER-REC.",
            "       05  ORDER-ID                        PIC X(30).",
            "       05  LINE-ITEMS OCCURS 25 TIMES.",
            "           10  SKU                         PIC X(20).",
            "           10  QUANTITY                    PIC S9(18) COMP.",
        ]
        self.assertLinesInResult(expected, result)

    def test_config_overrides(self):
        """
        Tests that non-default config values for
        occurs and string length are used.
        """
        # ARRANGE
        self.mock_table.schema = [
            SchemaField("tag", "STRING", "REPEATED"),  # Repeated, no max_len
            SchemaField("comment", "STRING", "NULLABLE"),  # Scalar, no max_len
        ]

        custom_config = {"default_occurs": 55, "default_string_len": 150}

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "CONFIG-REC", custom_config)

        # ASSERT
        expected = [
            "       01  CONFIG-REC.",
            "       05  TAG                             PIC X(150) OCCURS 55 TIMES.",
            "       05  COMMENT                         PIC X(150).",
        ]
        self.assertLinesInResult(expected, result)

    def test_name_sanitization(self):
        """Tests conversion of BQ names with _ and mixed case."""
        # ARRANGE
        self.mock_table.schema = [
            SchemaField("This_Field_Has_CAPS_and_Underscores", "INTEGER", "NULLABLE"),
        ]

        # ACT
        result = bq_to_copybook.generate_copybook_string(self.mock_table, "NAMES-REC", self.config)

        # ASSERT
        expected = [
            "       01  NAMES-REC.",
            "       05  THIS-FIELD-HAS-CAPS-AND-UNDE    PIC S9(18) COMP.",  # Truncated to 30
        ]
        self.assertLinesInResult(expected, result)


# --- RUNNER ---
if __name__ == "__main__":
    unittest.main(verbosity=2)
