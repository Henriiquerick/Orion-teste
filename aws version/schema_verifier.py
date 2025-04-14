import time
import boto3
from typing import Dict, Any, List


ATHENA_WORKGROUP = 'analytics-workgroup-v3'  #  Consider configuration files/env vars
ATHENA_S3_BUCKET = 'analytics-query-result-athena-sa-east-1-261034348095'
ATHENA_REGION = 'sa-east-1'


def get_athena_table_schema(
    athena_client: boto3.client, table_name: str, database_name: str
) -> List[Dict[str, Any]]:
    """
    Retrieves the schema of an AWS Athena table.

    Args:
        athena_client: Authenticated Athena client.
        table_name: Name of the table.
        database_name: Name of the database.

    Returns:
        List of table schema information (DESCRIBE output).

    Raises:
        RuntimeError: If query execution fails.
    """

    query = f"DESCRIBE {table_name}"

    try:
        query_execution_response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={'OutputLocation': f's3://{ATHENA_S3_BUCKET}/'},
        )
        query_execution_id = query_execution_response['QueryExecutionId']

        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            if state in ['FAILED', 'CANCELLED']:
                raise RuntimeError(
                    f"Athena query failed with status: {query_status['QueryExecution']['Status']['State']}"
                )
            time.sleep(1)

        results_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results_response['ResultSet']['Rows']

    except Exception as e:
        raise RuntimeError(f"Error retrieving schema for table '{table_name}': {e}") from e


if __name__ == '__main__':
    #  Example Usage (Replace with actual credentials)
    #  NEVER hardcode credentials in production code!
    import boto3

    athena_client = boto3.client(
        'athena',
        aws_access_key_id="YOUR_ACCESS_KEY",
        aws_secret_access_key="YOUR_SECRET_KEY",
        aws_session_token="YOUR_SESSION_TOKEN",
        region_name=ATHENA_REGION,
    )

    try:
        schema_info = get_athena_table_schema(
            athena_client=athena_client, table_name='YOUR_TABLE_NAME', database_name='YOUR_DATABASE_NAME'
        )
        print("Table Schema:")
        for row in schema_info:
            print(row['Data'])
    except RuntimeError as e:
        print("Error:", e)