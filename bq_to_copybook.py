#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reads a BigQuery table schema and generates a COBOL
Copybook representation.
The generation function returns a string.
The script prints this string to STDOUT when run.
"""

import sys
import argparse
import re
from typing import List, Dict, Any, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound
from google.cloud.bigquery import SchemaField, Table

# --- CONFIGURABLE DEFAULTS ---
# These can be overridden by command-line args
DEFAULT_STRING_LEN = 256
DEFAULT_INTEGER_PICS = "S9(18)"
DEFAULT_FLOAT_PICS = "S9(18)V9(06)"
DEFAULT_OCCURS_COUNT = 25
DEFAULT_INTEGER_LENGTH = 18
# --- END DEFAULTS ---

# --- HELPER FUNCTIONS ---


def bq_to_cobol_name(bq_name: str) -> str:
    """
    Converts BigQuery field name to a COBOL-friendly name.
     - Uppercase
     - Underscore to Hyphen
     - Truncate to 30 Chars
     - Remove other invalid chars
    """
    if not bq_name:
        return "FILLER"
    # Replace underscores, convert to upper,
    cobol_name = re.sub(r"[^A-Z0-9-]", "-", bq_name.upper().replace("_", "-"))
    # Ensure it doesn't start/end with a hyphen
    cobol_name = cobol_name.strip("-")
    if not cobol_name:
        return "FILLER"

    return cobol_name[:30]


# def get_field_length(args, field: SchemaField) -> int:
#     client = bigquery.Client(project=args.project_id)
#     table_ref = client.dataset(args.dataset_id).table(args.table_id)
#     table = client.get_table(table_ref)
#     print("LLEGOA CA")
#     res = client.query(f"SELECT * FROM {table_ref.dataset_id}")
#     return 1


def get_numeric_digit_counts(args, field: SchemaField) -> Tuple[int, int]:
    """
    Gets the maximum number of digits in the integer part and the maximum
    number of digits in the decimal part for a specified BigQuery NUMERIC
    or BIGNUMERIC column.

    Args:
        args: An object with attributes project_id, dataset_id, and table_id.
        field: A BigQuery SchemaField object representing the numeric column to analyze.
               Expected field types: 'NUMERIC', 'BIGNUMERIC'.

    Returns:
        A tuple (max_integer_digits, max_decimal_digits).
        Returns (0, 0) if the column is empty, all values are NULL, or an error occurs.
    """
    client = bigquery.Client(project=args.project_id)
    table_ref = client.dataset(args.dataset_id).table(args.table_id)

    query = f""" SELECT 
    char_length(cast(trunc(max({field.name})) as string)) AS max_integer_digits,
    max(char_length(cast({field.name}-trunc({field.name}) as string))) as max_decimal_digits
 FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
"""

    query_job = client.query(query)
    result = query_job.result()  # Waits for the job to complete.

    for row in result:
        max_integer_digits = row["max_integer_digits"] if row["max_integer_digits"] is not None else 0
        max_decimal_digits = row["max_decimal_digits"] if row["max_decimal_digits"] is not None else 0
        return (max_integer_digits, max_decimal_digits)


def get_field_length(args, field: SchemaField, sql: str = None) -> int:
    """
    Gets the maximum length of a string in a specified BigQuery column.

    Args:
        args: An object with attributes project_id, dataset_id, and table_id.
        field: A BigQuery SchemaField object representing the column to analyze.

    Returns:
        The maximum string length found in the column, or 0 if the column
        is empty or does not contain strings.
    """
    client = bigquery.Client(project=args.project_id)
    table_ref = client.dataset(args.dataset_id).table(args.table_id)

    # Construct the SQL query to find the maximum length of the string column
    # We use COALESCE to handle NULL values, treating their length as 0.
    query = (
        sql
        or f"""
        SELECT MAX(LENGTH(CAST({field.name} AS STRING)))
        FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
        WHERE {field.name} IS NOT NULL
    """
    )

    try:
        query_job = client.query(query)
        result = query_job.result()  # Waits for the job to complete.

        for row in result:
            max_length = row[0]
            if max_length is not None:
                return max_length
            else:
                return 0  # Column is empty or all values are NULL

    except Exception as e:
        print(f"An error occurred: {e}")
        return 0  # Or raise the exception, depending on desired error handling

    return 0  # Default return if no rows are processed (e.g., empty table)


def get_integer_max_length(args, field: SchemaField) -> int:
    client = bigquery.Client(project=args.project_id)
    table_ref = client.dataset(args.dataset_id).table(args.table_id)

    query = f"""
        SELECT CHAR_LENGTH(FORMAT('%d', 
            (SELECT MAX({field.name}) FROM {table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id})
            )) AS max_integer_length
        """

    query_job = client.query(query)
    try:
        result = query_job.result()  # Waits for the job to complete.
    except BadRequest as e:
        if "invalidQuery" in str(e):
            return DEFAULT_INTEGER_LENGTH
        raise e

    for row in result:
        max_length = row["max_integer_length"]
        return max_length


def get_array_max_length(args, field: SchemaField) -> int:
    client = bigquery.Client(project=args.project_id)
    table_ref = client.dataset(args.dataset_id).table(args.table_id)

    query = f"""
             SELECT
             MAX(ARRAY_LENGTH({field.name})) AS max_array_length
             FROM {table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id};
        """

    query_job = client.query(query)
    try:
        result = query_job.result()  # Waits for the job to complete.
    except BadRequest as e:
        if "invalidQuery" in str(e):
            return DEFAULT_INTEGER_LENGTH
        raise e

    for row in result:
        max_length = row["max_array_length"]
        return max_length


def bq_type_to_pic(args, field: SchemaField, config: Dict[str, Any]) -> str:
    """
    Maps a BigQuery SchemaField to a COBOL PIC clause string.
    Returns None for STRUCT/RECORD types, as they are handled
    via recursion.
    Uses config dictionary for defaults.
    """
    f_type = field.field_type

    # --- Precise Types ---
    if f_type in ("NUMERIC", "BIGNUMERIC", "FLOAT", "FLOAT64"):
        int_size, decimal_size = get_numeric_digit_counts(args, field)
        return f"S9({int_size})V9({decimal_size}) COMP-3"

    # --- Standard Types (using defaults if needed) ---
    elif f_type in ("INTEGER", "INT64"):
        length = get_integer_max_length(args, field)
        return f"9({length})"

    elif f_type == "BOOLEAN":
        return "X(1)"

    elif f_type == "STRING":
        length = get_field_length(args, field) or 256
        return f"X({length})"

    elif f_type == "BYTES":
        sql = f"SELECT MAX(BYTE_LENGTH({field.name})) FROM `{args.project_id}.{args.dataset_id}.{args.table_id}`"
        length = get_field_length(args, field, sql) or 256
        return f"X({length})"

    elif f_type == "GEOGRAPHY":
        length = field.max_length if field.max_length else config["default_string_len"]
        return f"X({length})"

    elif f_type == "TIMESTAMP":
        return "X(26)"  # YYYY-MM-DDTHH:MM:SS.ffffff

    elif f_type == "DATE":
        return "X(10)"  # YYYY-MM-DD

    elif f_type == "DATETIME":
        return "X(19)"  # YYYY-MM-DDTHH:MM:SS

    elif f_type == "TIME":
        return "X(12)"  # HH:MM:SS.ffffff

    else:
        # Fallback for unknown/new types
        print(f"WARNING: Unknown BQ Type '{f_type}' for field '{field.name}'. Using default PIC X(256).", file=sys.stderr)
        return f"X({config['default_string_len']})"


def _generate_copybook_lines(args, schema: List[SchemaField], level: int, config: Dict[str, Any]) -> List[str]:
    """
    Recursively generates copybook lines for a schema.
    Returns a List of strings (the copybook lines).
    """
    lines = []
    level_str = f"{level:02d}"
    default_occurs = config["default_occurs"]

    for field in schema:
        cobol_name = bq_to_cobol_name(field.name)
        pic_clause = bq_type_to_pic(args, field, config)

        occurs_clause = ""
        if field.mode == "REPEATED" or field.field_type == "RECORD":
            occurs_times = get_array_max_length(args, field)
            occurs_clause = f" OCCURS {occurs_times} TIMES"

        indentation_per_level = 7
        if level_str == "01":
            line_prefix = f"{' ' * indentation_per_level}{level_str}  {cobol_name}"
        elif level_str:
            space_per_level = 5
            num_levels = (level / space_per_level) + 1
            indentation = int(indentation_per_level * num_levels)
            line_prefix = f"{' ' * indentation}{level_str}  {cobol_name}"

        if pic_clause and field.field_type != "RECORD":
            line = f"{line_prefix:<40} PIC {pic_clause}{occurs_clause}."
            lines.append(line)
        elif field.field_type in ("STRUCT", "RECORD"):
            line = f"{line_prefix} "
            lines.append(line)
            nested_lines = _generate_copybook_lines(args, list(field.fields), level + 5, config)
            lines.extend(nested_lines)
        else:
            # Should not happen
            warning_line = f"*{' ' * 6}WARNING: SKIPPED field {field.name} (Type: {field.field_type})"
            lines.append(warning_line)
            print(warning_line, file=sys.stderr)

    return lines


# --- MAIN GENERATION FUNCTION ---


def generate_copybook_string(args, table: Table, record_name: str, config: Dict[str, Any]) -> str:
    """
    Takes a BigQuery Table object and configuration,
    returns the full Copybook as a string.
    """

    lines = []
    # Header / Comments
    header = f"""       ******************************************************************
       * COBOL COPYBOOK GENERATED FROM 
       * BQ TABLE: {table.project}.{table.dataset_id}.{table.table_id}
       *
       * IMPORTANT:
       * - Verify PIC X(n) lengths. Defaults are used if
       * max_length is not specified in BQ schema.
       * - REPEATED fields use a FIXED OCCURS {config["default_occurs"]} clause.
       * Ensure this count is sufficient for processing.
       * - NUMERIC types are generated as COMP-3 (Packed Decimal).
       * - INTEGER types are generated as COMP (Binary).
       * - Adjust USAGE (COMP, COMP-3, DISPLAY) as needed.
       ******************************************************************"""
    lines.append(header)

    # 01 Level
    lines.append(f"{' ' * 7}01  {bq_to_cobol_name(record_name)}.")

    # 05+ Levels (Child Fields)
    field_lines = _generate_copybook_lines(args, table.schema, 5, config)
    lines.extend(field_lines)

    return "\n".join(lines)


# --- COMMAND LINE EXECUTION ---


def run_cli():
    """Parses CLI args, fetches schema, generates copybook, prints to stdout."""

    parser = argparse.ArgumentParser(
        description="Convert BigQuery Table Schema to COBOL Copybook (to STDOUT).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud Project ID.")
    parser.add_argument("dataset_id", help="BigQuery Dataset ID.")
    parser.add_argument("table_id", help="BigQuery Table ID.")
    parser.add_argument("--record-name", help="Top-level (01) Record Name for the Copybook.", default="BQ-RECORD")
    parser.add_argument(
        "--default-occurs",
        type=int,
        default=DEFAULT_OCCURS_COUNT,
        help=f"Fixed OCCURS count to use for REPEATED fields.\n(Default: {DEFAULT_OCCURS_COUNT})",
    )
    parser.add_argument(
        "--default-string-len",
        type=int,
        default=DEFAULT_STRING_LEN,
        help=f"PIC X(n) length to use for STRING/BYTES if not defined in BQ Schema.\n(Default: {DEFAULT_STRING_LEN})",
    )

    args = parser.parse_args()

    # Bundle config to pass down
    config = {"default_occurs": args.default_occurs, "default_string_len": args.default_string_len}

    try:
        client = bigquery.Client(project=args.project_id)
        table_ref = client.dataset(args.dataset_id).table(args.table_id)
        table = client.get_table(table_ref)

    except NotFound:
        print(f"\nERROR: Table not found: {args.project_id}.{args.dataset_id}.{args.table_id}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Failed to get table schema. Check credentials and IDs. \n {e}", file=sys.stderr)
        sys.exit(1)

    # --- Generate and Print ---
    copybook_output_string = generate_copybook_string(args, table, args.record_name, config)

    print(copybook_output_string)


if __name__ == "__main__":
    run_cli()
