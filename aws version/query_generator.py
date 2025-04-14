from typing import List, Dict, Any, Optional


def generate_athena_query(tables: List[str], transformations: List[Dict[str, Any]]) -> str:
    """
    Generates an Athena SQL query to combine data from multiple tables with specified transformations.

    Args:
        tables: List of table names.
        transformations: List of transformations to apply.
                       Each transformation is a dictionary with 'coluna', 'acao', and 'motivo' keys.

    Returns:
        Generated SQL query.

    Raises:
        ValueError: If fewer than two tables are provided.
    """

    if not tables or len(tables) < 2:
        raise ValueError("At least two tables are required to generate a query.")

    transformations_dict: Dict[str, str] = {
        t['coluna']: t['acao'] for t in transformations if 'coluna' in t and 'acao' in t
    }

    select_clause_parts: List[str] = []
    for column, action in transformations_dict.items():
        if action == 'converter_para_string':
            select_clause_parts.append(f"CAST({column} AS STRING) AS {column}")
        elif action == 'converter_para_float':
            select_clause_parts.append(f"CAST({column} AS FLOAT) AS {column}")
        else:
            select_clause_parts.append(column)

    select_clause = ", ".join(select_clause_parts) if select_clause_parts else "*"

    from_clause = f"FROM {tables[0]}"

    join_clauses: List[str] = [
        f"FULL OUTER JOIN {table} USING (chave_comum)"  #  TODO:  Parameterize join keys!
        for table in tables[1:]
    ]
    join_clause = " ".join(join_clauses)

    query = f"""
    SELECT {select_clause}
    {from_clause}
    {join_clause}
    ;
    """
    return query.strip()


if __name__ == '__main__':
    sample_tables = ["table_a", "table_b", "table_c"]
    sample_transformations = [
        {"coluna": "col1", "acao": "converter_para_string"},
        {"coluna": "col2", "acao": "converter_para_float"},
        {"coluna": "col3", "acao": "unknown"}
    ]

    try:
        sql_query = generate_athena_query(sample_tables, sample_transformations)
        print("Generated SQL Query:\n", sql_query)

        generate_athena_query(["table1"])  #  Trigger error
    except ValueError as e:
        print("Error:", e)