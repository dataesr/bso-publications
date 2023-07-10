import os
import boto3
from retry import retry
from io import BytesIO, TextIOWrapper
from project.server.main.logger import get_logger

client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT'),
    aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
    region_name=os.getenv('S3_REGION'),
)

logger = get_logger(__name__)

@retry(delay=2, tries=50, logger=logger)
def upload_s3(container: str, source: str, destination: str) -> str:
    logger.debug(f'Uploading {source} in {container} as {destination}')
    data = open(f'{source}', 'rb')
  
    client.put_object(Key=destination, Body=data, Bucket=container)
    return f'ok: 1'
