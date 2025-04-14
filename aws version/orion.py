from typing import List, Dict, Any, Callable, Optional
from auth import authenticate_athena
from data_ingestion import process_table_names
from schema_verifier import get_athena_table_schema
from type_analysis import analyze_data_types
from transformer import transform_data_types
from query_generator import generate_athena_query


def execute_data_pipeline(
    pipeline_config: Dict[str, Any], log_callback: Callable[[str], None]
) -> Optional[str]:
    """
    Executes the main data processing pipeline.

    Args:
        pipeline_config: Dictionary containing user-provided configuration.
        log_callback: Callback function for logging messages to the UI.

    Returns:
        The final generated SQL query, or None if the pipeline fails.
    """

    try:
        # 1. Extract Configuration
        aws_credentials = pipeline_config.get('aws_credentials', {})
        aws_access_key: str = aws_credentials.get('access_key')
        aws_secret_key: str = aws_credentials.get('secret_key')
        aws_session_token: str = aws_credentials.get('session_token')
        region: str = pipeline_config.get('region')

        s3_config = pipeline_config.get('s3_config', {})
        s3_bucket: str = s3_config.get('bucket_name')
        database_name: str = pipeline_config.get('database')
        raw_table_names: List[str] = pipeline_config.get('tables', [])

        #  Validate essential configuration
        if not all(
            [aws_access_key, aws_secret_key, aws_session_token, region, s3_bucket, database_name, raw_table_names]
        ):
            log_callback("ERROR: Incomplete pipeline configuration.")
            return None

        # 2. Athena Authentication
        log_callback("INFO: Authenticating with AWS Athena...")
        athena_client = authenticate_athena(aws_access_key, aws_secret_key, aws_session_token, region)
        log_callback("INFO: Athena authentication successful.")

        # 3. Table Ingestion
        log_callback("INFO: Processing table names...")
        processed_table_names: List[str] = process_table_names(raw_table_names)
        log_callback(f"INFO: Processed tables: {processed_table_names}")

        # 4. Schema Verification
        log_callback("INFO: Verifying table schemas...")
        table_schemas: List[Dict[str, Any]] = []
        for table_name in processed_table_names:
            log_callback(f"INFO: Verifying schema for table: {table_name}")
            schema = get_athena_table_schema(athena_client, table_name, database_name)
            table_schemas.append(schema)
        log_callback("INFO: Table schema verification completed.")

        # 5. Type Analysis
        log_callback("INFO: Analyzing data types...")
        type_incompatibilities = analyze_data_types(table_schemas)
        if type_incompatibilities:
            log_callback("WARNING: Data type incompatibilities detected:")
            for incompatibility in type_incompatibilities:
                log_callback(
                    f"WARNING:  Column: {incompatibility['coluna']}, Type: {incompatibility['tipo']}, Table: {incompatibility['tabela']}"
                )
        else:
            log_callback("INFO: No data type incompatibilities detected.")

        # 6. Data Transformations
        log_callback("INFO: Applying data transformations...")
        transformations = transform_data_types(type_incompatibilities)
        for transform in transformations:
            log_callback(
                f"INFO:  Transforming Column: {transform['coluna']}, Action: {transform['acao']}, Reason: {transform['motivo']}"
            )

        # 7. Generate SQL Query
        log_callback("INFO: Generating final SQL query...")
        final_query: str = generate_athena_query(processed_table_names, transformations)
        log_callback("INFO: Final SQL Query:\n" + final_query)

        log_callback("INFO: Data pipeline execution completed successfully!")
        return final_query

    except Exception as e:
        log_callback(f"ERROR: An error occurred during pipeline execution: {e}")
        return None  # Indicate failure

if __name__ == '__main__':
    #  Example Usage (Replace with your actual configuration)
    pipeline_config = {
        'aws_credentials': {
            'access_key': 'YOUR_ACCESS_KEY',
            'secret_key': 'YOUR_SECRET_KEY',
            'session_token': 'YOUR_SESSION_TOKEN',
        },
        'region': 'YOUR_REGION',
        's3_config': {
            'bucket_name': 'YOUR_BUCKET_NAME',
        },
        'database': 'YOUR_ATHENA_DATABASE',
        'tables': ['table1', 'table2'],
    }

    def print_log_message(message: str):
        print(f"PIPELINE LOG: {message}")

    execute_data_pipeline(pipeline_config, print_log_message)