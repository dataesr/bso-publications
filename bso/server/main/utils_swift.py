import gzip
import os
from io import BytesIO, TextIOWrapper

import pandas as pd
import swiftclient

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
      --key {key} --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"

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


def upload_object(container: str, filename: str) -> str:
    object_name = filename.split('/')[-1]
    logger.debug(f'uploading {filename} in {container} as {object_name}')
    cmd = init_cmd + f' upload {container} {filename} --object-name {object_name}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    os.system(cmd)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{object_name}'


def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    os.system(cmd)


def exists_in_storage(container: str, path: str) -> bool:
    try:
        conn.head_object(container, path)
        return True
    except:
        return False


def get_objects(container: str, path: str) -> list:
    try:
        df = pd.read_json(BytesIO(conn.get_object(container, path)[1]), compression='gzip')
    except:
        df = pd.DataFrame([])
    return df.to_dict('records')


def get_objects_by_prefix(container: str, prefix: str) -> list:
    logger.debug(f"retrieving object from container {container} and prefix {prefix}")
    objects = []
    marker = None
    keep_going = True
    while keep_going:
        content = conn.get_container(container=container, marker=marker)[1]
        filenames = [file['name'] for file in content if file['name'].startswith(prefix)]
        objects += [get_objects(container=container, path=filename) for filename in filenames]
        keep_going = len(content) == SWIFT_SIZE
        marker = content[-1]['name']
        logger.debug(f"now {len(objects)} objects and counting")
    return objects


def set_objects(all_objects, container: str, path: str) -> None:
    logger.debug(f'setting object {container} {path}')
    if isinstance(all_objects, list):
        all_notices_content = pd.DataFrame(all_objects)
    else:
        all_notices_content = all_objects
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        all_notices_content.to_json(TextIOWrapper(gz_file, 'utf8'), orient='records')
    conn.put_object(container, path, contents=gz_buffer.getvalue())
    logger.debug('done')
    return


def delete_object(container: str, folder: str) -> None:
    cont = conn.get_container(container)
    for n in [e['name'] for e in cont[1] if folder in e['name']]:
        logger.debug(n)
        conn.delete_object(container, n)
