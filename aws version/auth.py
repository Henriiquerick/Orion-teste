import boto3
from botocore.exceptions import ClientError


def authenticate_athena(access_key: str, secret_key: str, session_token: str, region: str) -> boto3.client:
    """
    Authenticates with AWS Athena using provided credentials.

    Args:
        access_key: AWS Access Key ID.
        secret_key: AWS Secret Access Key.
        session_token: AWS Session Token.
        region: AWS region.

    Returns:
        Authenticated AWS Athena client.

    Raises:
        RuntimeError: If authentication fails.
    """
    try:
        client = boto3.client(
            'athena',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
        )
        # Test authentication (optional, but good practice)
        client.list_work_groups()  # A simple Athena call
        return client
    except ClientError as e:
        raise RuntimeError(f"Athena authentication failed: {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred during authentication: {e}") from e


if __name__ == '__main__':
    #  Example Usage (NEVER hardcode credentials in real code!)
    #  Consider environment variables or secure storage
    try:
        athena_client = authenticate_athena(
            access_key="YOUR_ACCESS_KEY",
            secret_key="YOUR_SECRET_KEY",
            session_token="YOUR_SESSION_TOKEN",
            region="YOUR_REGION"
        )
        print("Athena client authenticated successfully!")
    except RuntimeError as e:
        print(f"Error: {e}")