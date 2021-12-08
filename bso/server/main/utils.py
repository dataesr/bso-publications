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

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object, get_objects_by_page

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
    es_index = args.get('index_name', 'bso-publications')
    size = args.get('size', -1)
    # 1. Dump ES bso-publications index data into temp file
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    container = 'bso_dump'
    today = datetime.date.today().isoformat().replace('-', '')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_json_file = f'{MOUNTED_VOLUME}{es_index}_{today}.jsonl.gz'
    output_csv_file = f'{MOUNTED_VOLUME}{es_index}_{today}.csv'
    cmd_elasticdump = f'elasticdump --input={es_host}{es_index} --output={output_json_file} --type=data --sourceOnly=true --fsCompress=gzip --limit 10000'
    logger.debug(cmd_elasticdump)
    os.system(cmd_elasticdump)
    logger.debug('Elasticdump is done')
    # 2. Convert JSON file into CSV by selecting fields
    last_oa_details=args.get('last_oa_details',"2021Q4")

    cmd_header = f"echo 'doi,year,title,journal_issns,journal_issn_l,journal_name,publisher,publisher_dissemination,hal_id,pmid,bso_classification,bsso_classification,domains,lang,genre,amount_apc_EUR,detected_countries,bso_local_affiliations,is_oa,journal_is_in_doaj,journal_is_oa,observation_date,oa_host_type,oa_colors,licence_publisher,licence_repositories,repositories' > {output_csv_file}"
    logger.debug(cmd_header)
    os.system(cmd_header)
    
    cmd_jq = f"zcat {output_json_file} |  jq -rc '[.doi,.year,.title,.journal_issns,.journal_issn_l,.journal_name,.publisher,.publisher_dissemination,.hal_id,.pmid,.bso_classification,((.bsso_classification.field)?|join(\";\"))//null,((.domains)?|join(\";\"))//null,.lang,.genre,.amount_apc_EUR,((.detected_countries)?|join(\";\"))//null,((.bso_local_affiliations)?|join(\";\"))//null,[.oa_details[]|select(.observation_date==\"{last_oa_details}\")|.is_oa,.journal_is_in_doaj,.journal_is_oa,.observation_date,([.oa_host_type]|flatten)[0],((.oa_colors)?|join(\";\"))//null,((.licence_publisher)?|join(\";\"))//null,((.licence_repositories)?|join(\";\"))//null,((.repositories)?|join(\";\"))//null]]|flatten|@csv' >> {output_csv_file}"
    logger.debug(cmd_jq)
    os.system(cmd_jq)
        
    local_bso_filenames = []

    for page in range(1, 1000000):
        filenames = get_objects_by_page(container = 'bso-local', page=page, full_objects=False)
        if len(filenames) == 0:
            break
        for filename in filenames:
            logger.debug(f'dump bso-local {filename}')
            local_bso_filenames += filename.split('.')[0].split('_')
    local_bso_filenames = list(set(local_bso_filenames))

    for local_affiliation in local_bso_filenames:
        logger.debug(f'bso-local files creation for {local_affiliation}')
        cmd_local_json = f'zcat {output_json_file} | fgrep {local_affiliation} > enriched_{local_affiliation}.jsonl'
        cmd_local_csv_header = f'head -n 1 {output_csv_file} > enriched_{local_affiliation}.csv'
        cmd_local_csv = f'cat {output_csv_file} | fgrep {local_affiliation} >> enriched_{local_affiliation}.csv' 
        os.system(cmd_local_json)
        os.system(cmd_local_csv_header)
        os.system(cmd_local_csv)
        upload_object(container=container, filename=f'enriched_{local_affiliation}.jsonl')
        upload_object(container=container, filename=f'enriched_{local_affiliation}.csv')
        os.system(f'rm -rf enriched_{local_affiliation}.jsonl')
        os.system(f'rm -rf enriched_{local_affiliation}.csv')
    
    cmd_gzip = f'gzip {output_csv_file}'
    logger.debug(cmd_gzip)
    os.system(cmd_gzip)
    logger.debug('global csv file is created')
    # 3. Upload these files into OS
    uploaded_file_json = upload_object(container=container, filename=f'{output_json_file}')
    uploaded_file_csv = upload_object(container=container, filename=f'{output_csv_file}.gz')
    # 4. Clean temporary files
    os.system(f'rm -rf {output_json_file}')
    os.system(f'rm -rf {output_csv_file}.gz')
    return [uploaded_file_json, uploaded_file_csv]
