import datetime
import csv
import gzip
import json
import os
import re
import requests
import shutil

from typing import Union
from urllib import parse

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import upload_object

FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']
logger = get_logger(__name__)


def get_filename_from_cd(cd: str) -> Union[str, None]:
    """ Get filename from content-disposition """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def download_file(url: str, upload_to_object_storage: bool = True, destination: str = None) -> str:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    start = datetime.datetime.now()
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
        logger.debug(f'Start downloading {local_filename} at {start}')
        local_filename = f'{MOUNTED_VOLUME}{local_filename}'
        if destination:
            local_filename = destination
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'End download in {delta}')
    if upload_to_object_storage:
        upload_object(container='unpaywall', filename=local_filename)
    return local_filename


def dump_to_object_storage() -> list:
    # 1. Dump ES bso-publications index data into temp file
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@cluster.elasticsearch.dataesr.ovh/'
    es_index = 'bso-publications'
    container = 'bso_dump'
    today = datetime.date.today()
    today_date = f'{today.year}{today.month}{today.day}'
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_json_file = f'{MOUNTED_VOLUME}{es_index}_{today_date}.jsonl'
    output_csv_file = f'{MOUNTED_VOLUME}{es_index}_{today_date}.csv'
    cmd_elasticdump = f'elasticdump --input={es_host}{es_index} --output={output_json_file} --type=data'
    os.system(cmd_elasticdump)
    # 2. Convert JSON file into CSV by selecting fields
    file = open(output_json_file, 'r')
    content = file.read()
    lines = content.splitlines()
    file.close()
    headers = list(lines[0].keys())
    rows = [headers]
    for line in lines:
        source = json.loads(line).get('_source', {})
        row = list(source.values())
        rows.append(row)
    data_file = open(output_csv_file, 'w')
    csv_writer = csv.writer(data_file)
    csv_writer.writerows(rows)
    data_file.close()
    # 3. Upload these files into OS
    with open(output_json_file, 'rb') as f_in:
        with gzip.open(f'{output_json_file}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    uploaded_file_json = upload_object(container=container, filename=f'{output_json_file}.gz')
    with open(output_csv_file, 'rb') as f_in:
        with gzip.open(f'{output_csv_file}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    uploaded_file_csv = upload_object(container=container, filename=f'{output_csv_file}.gz')
    # 4. Clean temporary files
    os.system(f'rm {output_json_file}')
    os.system(f'rm {output_json_file}.gz')
    os.system(f'rm {output_csv_file}')
    os.system(f'rm {output_csv_file}.gz')
    return [uploaded_file_json, uploaded_file_csv]
