import gzip
import os
import pandas as pd
import swiftclient
import time
from io import BytesIO, TextIOWrapper
from retry import retry

from bso.server.main.logger import get_logger

logger = get_logger(__name__)
SWIFT_SIZE = 10000
key = os.getenv('OS_PASSWORD')
project_name = os.getenv('OS_PROJECT_NAME')
project_id = os.getenv('OS_TENANT_ID')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME')
user = f'{tenant_name}:{username}'
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"
conn = None


def get_connection() -> swiftclient.Connection:
    global conn
    if conn is None:
        conn = swiftclient.Connection(
            authurl='https://auth.cloud.ovh.net/v3',
            user=user,
            key=key,
            os_options={
                'user_domain_name': 'Default',
                'project_domain_name': 'Default',
                'project_id': project_id,
                'project_name': project_name,
                'region_name': 'GRA'
            },
            auth_version='3'
        )
    return conn


@retry(delay=10, tries=10, backoff=2)
def upload_object(container: str, filename: str) -> str:
    time.sleep(3)
    object_name = filename.split('/')[-1]
    logger.debug(f'Uploading {filename} in {container} as {object_name}')
    cmd = init_cmd + f' upload {container} {filename} --object-name {object_name}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    os.system(cmd)
    time.sleep(3)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{object_name}'


@retry(delay=2, tries=50)
def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    os.system(cmd)


@retry(delay=2, tries=50)
def exists_in_storage(container: str, path: str) -> bool:
    try:
        connection = get_connection()
        connection.head_object(container, path)
        return True
    except:
        return False


@retry(delay=2, tries=50)
def get_objects(container: str, path: str) -> list:
    try:
        connection = get_connection()
        df = pd.read_json(BytesIO(connection.get_object(container, path)[1]), compression='gzip')
    except:
        df = pd.DataFrame([])
    return df.to_dict('records')


@retry(delay=2, tries=50)
def get_objects_by_prefix(container: str, prefix: str) -> list:
    logger.debug(f'Retrieving object from container {container} and prefix {prefix}')
    objects = []
    marker = None
    keep_going = True
    while keep_going:
        connection = get_connection()
        content = connection.get_container(container=container, marker=marker, prefix=prefix)[1]
        filenames = [file['name'] for file in content]
        objects += [get_objects(container=container, path=filename) for filename in filenames]
        keep_going = len(content) == SWIFT_SIZE
        if len(content) > 0:
            marker = content[-1]['name']
            logger.debug(f'Now {len(objects)} objects and counting')
    flat_list = [item for sublist in objects for item in sublist]
    return flat_list


@retry(delay=2, tries=50)
def get_objects_by_page(container: str, page: int, full_objects: bool, nb_objects=1000) -> list:
    logger.debug(f'Retrieving object from container {container} and page {page}')
    marker = None
    keep_going = True
    current_page = 0 
    while keep_going:
        connection = get_connection()
        content = connection.get_container(container=container, marker=marker, limit=nb_objects)[1]
        filenames = [file['name'] for file in content]
        if len(filenames) == 0:
            return []
        current_page += 1
        keep_going = (page > current_page)
        if len(content) > 0:
            marker = content[-1]['name']
    
    if full_objects:
        objects = [get_objects(container=container, path=filename) for filename in filenames]
        flat_list = [item for sublist in objects for item in sublist]
        return flat_list

    return filenames


@retry(delay=2, tries=50)
def set_objects(all_objects, container: str, path: str) -> None:
    logger.debug(f'Setting object {container} {path}')
    if isinstance(all_objects, list):
        all_notices_content = pd.DataFrame(all_objects)
    else:
        all_notices_content = all_objects
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        all_notices_content.to_json(TextIOWrapper(gz_file, 'utf8'), orient='records')
    connection = get_connection()
    connection.put_object(container, path, contents=gz_buffer.getvalue())
    logger.debug('Done')
    return


@retry(delay=2, tries=50)
def delete_object(container: str, folder: str) -> None:
    connection = get_connection()
    cont = connection.get_container(container)
    for n in [e['name'] for e in cont[1] if folder in e['name']]:
        logger.debug(n)
        connection.delete_object(container, n)
