from typing import List


def process_table_names(table_names: List[str]) -> List[str]:
    """
    Processes a list of table names, removing extra whitespace and empty entries.

    Args:
        table_names: List of table names.

    Returns:
        List of cleaned table names.

    Raises:
        ValueError: If the input is not a list or the resulting list is empty.
    """

    if not isinstance(table_names, list):
        raise ValueError("Input must be a list of table names.")

    cleaned_table_names = [table_name.strip() for table_name in table_names if table_name.strip()]

    if not cleaned_table_names:
        raise ValueError("No valid table names provided after processing.")

    return cleaned_table_names


if __name__ == '__main__':
    tables = ["  table1  ", "table2", "  ", "table3  "]
    try:
        cleaned_tables = process_table_names(tables)
        print(f"Cleaned table names: {cleaned_tables}")

        empty_list = []
        process_table_names(empty_list)
    except ValueError as e:
        print(f"Error: {e}")