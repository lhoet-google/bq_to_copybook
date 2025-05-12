import sys
import argparse
from google.cloud import bigquery


def get_indent(level):
    return " " * (level * 4)


def process_field(field, current_level):
    cobol_lines = []
    field_name_cobol = field.name.replace("_", "-").upper()
    data_type = field.field_type
    mode = field.mode

    # Determine the level number for the current field
    field_level = current_level + 5

    if mode == "REPEATED":
        # For repeated fields, create a group item with OCCURS
        cobol_lines.append(f"{get_indent(current_level)}   10 {field_name_cobol}-COUNT PIC S9(09) COMP.")
        cobol_lines.append(
            f"{get_indent(current_level)}   10 {field_name_cobol} OCCURS 999999 TIMES DEPENDING ON {field_name_cobol}-COUNT."
        )
        # Recurse for the actual repeated item's structure
        # BigQuery repeated fields contain the actual type definition within them
        # For a simple type (like ARRAY<STRING>), the field.fields list will have one entry
        if field.fields:
            # Handle nested structures within repeated fields
            cobol_lines.extend(process_field(field.fields[0], current_level + 1))  # Increment level for items within OCCURS
        else:
            # Fallback for simpler repeated types if field.fields is empty (shouldn't happen for basic types)
            # This case might need refinement based on specific repeated types
            cobol_lines.append(
                f"{get_indent(current_level + 1)}   {field_level} {field_name_cobol}-ITEM PIC X(256)."
            )  # Default

    elif data_type == "RECORD":
        # For RECORD types, create a group item and process its subfields
        cobol_lines.append(f"{get_indent(current_level)}   {field_level} {field_name_cobol}.")
        for sub_field in field.fields:
            cobol_lines.extend(process_field(sub_field, current_level + 1))  # Increment level for subfields
    else:
        # Handle primitive types
        picture_clause = ""
        usage_clause = ""

        if data_type == "STRING":
            # Use max_length if available, otherwise a reasonable default
            length = field.max_length if field.max_length is not None else 256
            picture_clause = f"PIC X({length})"
        elif data_type == "INTEGER":
            # BigQuery INTEGER is INT64. S9(18) can hold values up to 999,999,999,999,999,999
            # COMP-3 (Packed Decimal) is a common choice for integers in COBOL
            picture_clause = "PIC S9(18)"
            usage_clause = "COMP-3"
        elif data_type == "FLOAT":
            # BigQuery FLOAT is FLOAT64 (double-precision).
            # Representing arbitrary precision floats accurately in fixed-point COBOL is hard.
            # Using packed decimal with a reasonable precision/scale as a common compromise.
            # You might need to adjust this based on expected precision.
            # Here, we assume 9 integer digits and 9 decimal digits.
            picture_clause = "PIC S9(9)V9(9)"
            usage_clause = "COMP-3"
        elif data_type == "NUMERIC":
            # BigQuery NUMERIC: 38 digits of precision, 9 digits of scale.
            # We use precision and scale from the schema.
            integer_digits = field.precision - field.scale if field.precision is not None and field.scale is not None else 29
            decimal_digits = field.scale if field.scale is not None else 9
            picture_clause = f"PIC S9({integer_digits})V9({decimal_digits})"
            usage_clause = "COMP-3"
        elif data_type == "BIGNUMERIC":
            # BigQuery BIGNUMERIC: 76.76 digits of precision, 38 digits of scale.
            # Similar approach as NUMERIC, but for larger values.
            integer_digits = field.precision - field.scale if field.precision is not None and field.scale is not None else 38
            decimal_digits = field.scale if field.scale is not None else 38
            picture_clause = f"PIC S9({integer_digits})V9({decimal_digits})"
            usage_clause = "COMP-3"
        elif data_type == "BOOLEAN":
            picture_clause = "PIC X(1)"  # 'T' for True, 'F' for False
        elif data_type == "BYTES":
            # Use max_length if available, otherwise a reasonable default
            length = field.max_length if field.max_length is not None else 256
            picture_clause = f"PIC X({length})"
        elif data_type == "DATE":
            picture_clause = "PIC X(10)"  # YYYY-MM-DD
        elif data_type == "DATETIME":
            picture_clause = "PIC X(26)"  # YYYY-MM-DD HH:MM:SS.FFFFFF
        elif data_type == "TIME":
            picture_clause = "PIC X(8)"  # HH:MM:SS
        elif data_type == "TIMESTAMP":
            picture_clause = "PIC X(30)"  # YYYY-MM-DD HH:MM:SS.FFFFFF UTC. Often stored as epoch in COBOL, but X(30) is a safer default for string representation.
        else:
            # Fallback for unhandled types
            picture_clause = "PIC X(256)"
            # Optional: Add a comment for unknown types
            cobol_lines.append(f"{get_indent(current_level)}* WARNING: UNKNOWN BIGQUERY TYPE '{data_type}' MAPPED TO X(256)")

        cobol_lines.append(
            f"{get_indent(current_level)}   {field_level} {field_name_cobol} {picture_clause} {usage_clause}.".strip()
        )

    return cobol_lines


def bigquery_to_cobol(project_id: str, dataset_id: str, table_id: str, copybook_name: str = "TABLE-RECORD."):
    """
    Generates a COBOL copybook for a given BigQuery table, including
    string field lengths and numeric field scale and precision.

    Args:
        project_id: The ID of your Google Cloud project.
        dataset_id: The ID of the BigQuery dataset.
        table_id: The ID of the BigQuery table.
        copybook_name: The name to use for the top-level record in the copybook.

    Returns:
        A string containing the COBOL copybook definition.
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
    except Exception as e:
        return f"Error: Could not retrieve table information for {project_id}.{dataset_id}.{table_id}. {e}"

    copybook_lines = [f"       01 {copybook_name.upper()}"]

    for schema_field in table.schema:
        copybook_lines.extend(process_field(schema_field, 0))  # Start at level 0 for main fields

    return "\n".join(copybook_lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a COBOL copybook from a BigQuery table schema.")
    parser.add_argument("project_id", help="The ID of your Google Cloud project.")
    parser.add_argument("dataset_id", help="The ID of the BigQuery dataset.")
    parser.add_argument("table_id", help="The ID of the BigQuery table.")
    parser.add_argument(
        "--copybook_name",
        default="TABLE-RECORD.",
        help="The name to use for the top-level record in the copybook (default: TABLE-RECORD.).",
    )

    args = parser.parse_args()

    copybook_output = bigquery_to_cobol(args.project_id, args.dataset_id, args.table_id, args.copybook_name)

    if "Error:" in copybook_output:
        print(copybook_output)
    else:
        print("Generated COBOL Copybook:")
        print(copybook_output)

# Example usage with a hypothetical table:
# Assume a table named my_dataset.my_table in my-project
# with columns:
# - string_col STRING (max_length=50)
# - int_col INTEGER
# - float_col FLOAT
# - numeric_col NUMERIC (precision=20, scale=5)
# - bignumeric_col BIGNUMERIC (precision=50, scale=20)
# - date_col DATE
# - record_col RECORD
#   - nested_string STRING (max_length=10)
#   - nested_int INTEGER
# - repeated_string_col ARRAY<STRING> (max_length=20)

# You would run:
# python your_script_name.py projId datasetId tableId
# And replace the placeholder variables above.
