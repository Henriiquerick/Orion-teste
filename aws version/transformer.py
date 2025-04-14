from typing import List, Dict, Any


def transform_data_types(incompatibilities: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Generates data transformation instructions to resolve type incompatibilities.

    Args:
        incompatibilities: List of type incompatibility dictionaries,
                         where each dict has 'coluna' and 'tipos_detectados' keys.

    Returns:
        List of transformation instructions,
        where each dict has 'coluna', 'acao', and 'motivo' keys.
    """

    transformations: List[Dict[str, str]] = []

    for incompatibility in incompatibilities:
        column_name: str = incompatibility.get('coluna')
        detected_types: List[str] = incompatibility.get('tipos_detectados', [])

        if not column_name or not detected_types:
            continue  #  Skip invalid incompatibility entries

        if 'string' in detected_types:
            action = 'converter_para_string'
            reason = 'Harmonize data to string due to multiple types detected.'
        elif any(dtype in detected_types for dtype in ['float', 'int']):
            action = 'converter_para_float'
            reason = 'Harmonize data to float to support int and float.'
        else:
            action = 'manter_indefinido'
            reason = f'Unknown types detected: {detected_types}. No transformation defined.'

        transformations.append(
            {'coluna': column_name, 'acao': action, 'motivo': reason}
        )

    return transformations


if __name__ == '__main__':
    sample_incompatibilities = [
        {'coluna': 'col1', 'tipos_detectados': ['string', 'int']},
        {'coluna': 'col2', 'tipos_detectados': ['float', 'int']},
        {'coluna': 'col3', 'tipos_detectados': ['unknown1', 'unknown2']},
        {'coluna': 'col4', 'tipos_detectados': []},
        {'coluna': None, 'tipos_detectados': ['string']},
    ]

    generated_transformations = transform_data_types(sample_incompatibilities)
    print("Generated Transformations:\n", generated_transformations)