import datetime
import csv
import gzip
import json
import os
import re
import requests
import shutil
import pandas as pd

from typing import Union
from urllib import parse

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object

FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']
logger = get_logger(__name__)


def get_dois_from_input(container: str, filename: str) -> list:
    target = f'{MOUNTED_VOLUME}/{filename}'
    download_object(container=container, filename=filename, out=target) 
    if 'xls' in filename.lower():
        df = pd.read_excel(target, engine='openpyxl')
    else:
        df = pd.read_csv(target)
    doi_columns = [c for c in df.columns if 'doi' in c.lower()]
    if len(doi_columns) > 0:
        doi_column = doi_columns[0]
        logger.debug(f'doi column: {doi_column}')
    else:
        return []
    dois = list(set([d.lower().strip() for d in df[doi_column].dropna().tolist()]))
    return dois


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


def dump_to_object_storage(args: dict) -> list:
    index_name = args.get('index_name', 'bso-publications')
    # 1. Dump ES bso-publications index data into temp file
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@cluster.elasticsearch.dataesr.ovh/'
    container = 'bso_dump'
    today = datetime.date.today().isoformat().replace('-', '')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_json_file = f'{MOUNTED_VOLUME}{es_index}_{today}.jsonl.gz'
    output_csv_file = f'{MOUNTED_VOLUME}{es_index}_{today}.csv.gz'
    cmd_elasticdump = f'elasticdump --input={es_host}{es_index} --output={output_json_file} --type=data --sourceOnly=true --fsCompress=gzip'
    logger.debug(cmd_elasticdump)
    os.system(cmd_elasticdump)
    logger.debug('Elasticdump is done')
    # 2. Convert JSON file into CSV by selecting fields
    last_oa_details='2021Q3'
    cmd_jq = f"zcat {output_json_file} | jq -r -c '[.doi,.title,.oa_details[].observation_date] | @csv' | gzip > {output_csv_file}"
    logger.debug(cmd_jq)
    os.system(cmd_jq)
    logger.debug('csv file is created')
    # 3. Upload these files into OS
    uploaded_file_json = upload_object(container=container, filename=f'{output_json_file}')
    uploaded_file_csv = upload_object(container=container, filename=f'{output_csv_file}')
    # 4. Clean temporary files
    os.system(f'rm -rf {output_json_file}')
    os.system(f'rm -rf {output_csv_file}')
    return [uploaded_file_json, uploaded_file_csv]
