from typing import List, Dict, Any, Optional


def analyze_data_types(metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyzes data types based on provided metadata and identifies inconsistencies.

    Args:
        metadata: List of table schemas (metadata).

    Returns:
        List of data type incompatibility dictionaries.
    """

    incompatibilities: List[Dict[str, Any]] = []

    for table_metadata in metadata:
        table_name: str = table_metadata.get('TableName', 'Unknown Table')
        rows: List[Dict[str, Any]] = table_metadata.get('Rows', [])

        for row in rows:
            data: Optional[List[Dict[str, Any]]] = row.get('Data')

            if not data or len(data) < 2:
                print(f"[WARNING] Incomplete data for table '{table_name}': {row}")
                continue

            try:
                column_name: Optional[str] = data[0].get('VarCharValue')
                data_type: Optional[str] = data[1].get('VarCharValue')

                if not column_name or not data_type:
                    print(f"[WARNING] Missing column or type in table '{table_name}': {row}")
                    continue

                if data_type not in ['string', 'int', 'float']:
                    incompatibilities.append({
                        'tabela': table_name,
                        'coluna': column_name,
                        'tipo': data_type,
                        'motivo': f"Unexpected data type: {data_type}"
                    })

            except Exception as e:
                print(f"[ERROR] Failed to process row in table '{table_name}': {row}. Details: {e}")

    return incompatibilities


if __name__ == '__main__':
    sample_metadata = [
        {
            'TableName': 'table_x',
            'Rows': [
                {'Data': [{'VarCharValue': 'col1'}, {'VarCharValue': 'string'}]},
                {'Data': [{'VarCharValue': 'col2'}, {'VarCharValue': 'int'}]},
                {'Data': [{'VarCharValue': 'col3'}, {'VarCharValue': 'date'}]},  #  Invalid type
            ]
        },
        {
            'TableName': 'table_y',
            'Rows': [
                {'Data': [{'VarCharValue': 'col1'}, {'VarCharValue': None}]},  #  Missing type
                {'Data': [{'VarCharValue': 'col2'}]},  #  Incomplete Data
            ]
        },
    ]

    detected_incompatibilities = analyze_data_types(sample_metadata)
    print("Detected Incompatibilities:\n", detected_incompatibilities)