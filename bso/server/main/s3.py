import os
import boto3
from boto3.s3.transfer import TransferConfig
from retry import retry
from io import BytesIO, TextIOWrapper
from bso.server.main.logger import get_logger

# Set the desired multipart threshold value (5GB)
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=5*GB)

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
    client.upload_file(Filename=source, Bucket=container, Key=destination, Config=config, ExtraArgs={'ACL':'public-read'}) 
    #data = open(f'{source}', 'rb')
    #client.put_object(Key=destination, Body=data, Bucket=container, ACL='public-read')
    return f'ok: 1'
