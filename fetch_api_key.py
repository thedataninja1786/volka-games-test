import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import get_session
import json
import os
from dotenv import load_dotenv

load_dotenv()


def get_env_variable(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Environment variable '{var_name}' is not set or empty.")
    return value


def get_api_key(
    secret_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str,
) -> None:
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    client = session.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response["SecretString"])
        with open("api_key.json", "w") as f:
            json.dump(secret_data, f)
        print(f"API key saved to 'api_key.json'")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    try:
        secret_name = get_env_variable("secret_name")
        aws_access_key_id = get_env_variable("aws_access_key_id")
        aws_secret_access_key = get_env_variable("aws_secret_access_key")
        region_name = get_env_variable("region_name")

        get_api_key(secret_name, aws_access_key_id, aws_secret_access_key, region_name)
    except ValueError as e:
        print(e)
