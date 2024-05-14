import datetime
import os
import pandas as pd
import re
import requests
import shutil
import hashlib
import json
import string

from typing import Union
from urllib import parse

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object, get_objects_by_page

FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']
logger = get_logger(__name__)

def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
        elif (isinstance(elt[f], str) and len(elt[f])==0):
            del elt[f]
        elif (isinstance(elt[f], list) and len(elt[f])==0):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

def to_json(input_list, output_file, ix):
    if ix == 0:
        mode = 'w'
    else:
        mode = 'a'
    with open(output_file, mode) as outfile:
        if ix == 0:
            outfile.write('[')
        for jx, entry in enumerate(input_list):
            if ix + jx != 0:
                outfile.write(',\n')
            entry = {f: entry[f] for f in entry if entry[f]==entry[f] }
            json.dump(entry, outfile)

def get_code_etab_nnt(x, nnt_etab_dict):
    #format nnt YYYY ETAB ABCD
    # cf https://documentation.abes.fr/sudoc/regles/CodesUnivEtab.htm
    if not isinstance(x, str):
        return None
    #if x[0:3] != 'nnt':
    #    return None
    # on essaie sur 5 ou 6 caractères pour gérer les cas comme LYSE1
    # on garde ce cas si le code est dans le dictionnaire des code etab connus des bso locaux
    for nb_caracters in [5, 6]:
        etab = x[4:4+nb_caracters].lower()
        if etab in nnt_etab_dict:
            return etab
    # par défaut, on renvoie les 4 caractères conformément à la doc de l'ABES
    etab=x[4:8].lower()
    return etab



def get_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

def is_valid(identifier, identifier_type):
    if not isinstance(identifier, str):
        return False
    if identifier_type == 'doi':
        if '/' not in identifier:
            return False
        if identifier[0:3] != '10.':
            return False
        for k in ['xxxxxx', 'nnnnnn']:
            if k in identifier.lower():
                return False
        return True
    #TODO utiliser des regex
    if identifier_type == 'hal_id':
        if '.' in identifier:
            return False
        for sep in ['-', '_']:
            if sep in identifier:
                return True
        return False
    #TODO utiliser des regex
    if identifier_type == 'nnt_id':
        try:
            # s54678 pour theses en cours
            assert(identifier[0:1] == 's')
            end = int(identifier[1:])
            return True
        except:
            pass
        year=0
        try:
            year = int(identifier[0:4])
        except:
            return False
        if not(1900 < year < 2100):
            return False
        return True
    return True

DOI_PREFIX = re.compile("(10\.)(.*?)( |$)")
def clean_doi(doi):
    res = doi.lower().strip()
    res = res.replace('%2f', '/')
    doi_match = DOI_PREFIX.search(res)
    if doi_match:
        return doi_match.group().strip()
    return None

def clean_hal_id(hal_id):
    res = hal_id.lower().strip()
    res = res.replace('%2f', '/')
    if '-' not in res:
        return None
    if res[-2] == 'v':
        return res[0:-2]
    return res

def get_clean_id(e):
    res = str(e).replace('.0','').strip().lower()
    res = res.split(',')[0].strip()
    if 'hal-' in res and res[-2] == 'v':
        res = res[0:-2]
    return res

def get_dois_from_input(filename: str) -> list:
    target = f'{MOUNTED_VOLUME}/bso_local/{filename}'
    logger.debug(f'reading {target}')
    if 'xls' in filename.lower():
        df = pd.read_excel(target, engine='openpyxl')
    else:
        df = pd.read_csv(target, sep=',')
        doi_columns = [c for c in df.columns if 'doi' in c.lower()]
        if doi_columns and ';' in doi_columns[0]:
            df = pd.read_csv(target, sep=';')

    doi_columns = [c for c in df.columns if 'doi' in c.lower()]
    if len(doi_columns) > 0:
        doi_column = doi_columns[0]
    else:
        logger.debug(f'ATTENTION !! Pas de colonne avec doi détectée pour {filename}')
        return []
    df['doi'] = df[doi_column]
    filtered_columns = ['doi']
    if 'project_id' in df.columns:
        logger.debug(f'funding data detected in file {filename}')
        assert('agency' in df.columns)
        df['project_id'] = df['project_id'].astype(str)
        filtered_columns += ['project_id', 'agency']
        if 'funding_year' in  df.columns:
            filtered_columns += ['funding_year']
    if 'bso_country' in df.columns:
        logger.debug(f'bso_country detected in file {filename}')
        filtered_columns += ['bso_country']
    elts_with_id = []
    grant_ids = []
    for row in df[filtered_columns].itertuples():
        clean_id = None
        if isinstance(row.doi, str):
            clean_id = clean_doi(row.doi)
            elt = {'id': f'doi{clean_id}', 'doi': clean_id}
        #elif isinstance(row.hal_id, str):
        #    clean_id = clean_hal_id(row.hal_id)
        #    elt = {'id': f'hal{clean_id}', 'hal_id': clean_id}
        if clean_id is None or len(clean_id)<5:
            continue
        if 'project_id' in filtered_columns:
            if isinstance(row.project_id, str):
                current_grant = {'grantid': str(row.project_id), 'agency': row.agency}
                if 'funding_year' in filtered_columns:
                    current_grant['funding_year'] = row.funding_year
                elt['grants'] = [current_grant]
                elt['has_grant'] = True
                grant_ids.append(row.project_id)
        elt['bso_country'] = ['fr']
        if 'bso_country' in filtered_columns:
            if isinstance(row.bso_country, str):
                elt['bso_country'] = [row.bso_country]
        elt['sources'] = [filename]
        elts_with_id.append(elt)
    nb_grants = len(set(grant_ids))
    res = {'doi': elts_with_id}
    for f in ['hal_struct_id', 'nnt_etab', 'hal_coll_code', 'nnt_id', 'hal_id']:
        if f in df.columns:
            data_column = [get_clean_id(e) for e in df[f].dropna().tolist()]
            res[f] = data_column
            logger.debug(f'{len(data_column)} {f} for {filename}')
    logger.debug(f'doi column: {doi_column} for {filename} with {len(elts_with_id)} dois and {nb_grants} funding')
    return res


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
    # 1. Dump ES bso-publications index data into temp file
    es_url_without_http = ES_URL.replace('https://', '').replace('http://', '')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    container = 'bso_dump'
    today = datetime.date.today().isoformat().replace('-', '')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_json_file = f'{MOUNTED_VOLUME}{es_index}_{today}.jsonl.gz'
    output_csv_file = f'{MOUNTED_VOLUME}{es_index}_{today}.csv'
    cmd_elasticdump = f'elasticdump --input={es_host}{es_index} --output={output_json_file} ' \
                      f'--type=data --sourceOnly=true --fsCompress=gzip --limit 10000'
    logger.debug(cmd_elasticdump)
    os.system(cmd_elasticdump)
    logger.debug('Elasticdump is done')
    # 2. Convert JSON file into CSV by selecting fields
    last_oa_details = args.get('last_oa_details', '2021Q4')
    cmd_header = f"echo 'doi,year,title,journal_issns,journal_issn_l,journal_name,publisher,publisher_dissemination," \
                 f"hal_id,pmid,bso_classification,bsso_classification,domains,lang,genre,amount_apc_EUR," \
                 f"detected_countries,bso_local_affiliations,is_oa,journal_is_in_doaj,journal_is_oa,observation_date," \
                 f"oa_host_type,oa_colors,licence_publisher,licence_repositories,repositories' > {output_csv_file}"
    logger.debug(cmd_header)
    os.system(cmd_header)
    cmd_jq = f"zcat {output_json_file} | jq -rc '[.doi,.year,.title,.journal_issns,.journal_issn_l,.journal_name," \
             f".publisher,.publisher_dissemination,.hal_id,.pmid,.bso_classification,((.bsso_classification.field)" \
             f"?|join(\";\"))//null,((.domains)?|join(\";\"))//null,.lang,.genre,.amount_apc_EUR," \
             f"((.detected_countries)?|join(\";\"))//null,((.bso_local_affiliations)?|join(\";\"))//null," \
             f"[.oa_details[]|select(.observation_date==\"{last_oa_details}\")|.is_oa,.journal_is_in_doaj," \
             f".journal_is_oa,.observation_date,([.oa_host_type]|flatten)[0],((.oa_colors)?|join(\";\"))//null," \
             f"((.licence_publisher)?|join(\";\"))//null,((.licence_repositories)?|join(\";\"))//null," \
             f"((.repositories)?|join(\";\"))//null]]|flatten|@csv' >> {output_csv_file}"
    logger.debug(cmd_jq)
    os.system(cmd_jq)
    local_bso_filenames = []
    for page in range(1, 1000000):
        filenames = get_objects_by_page(container='bso-local', page=page, full_objects=False)
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
