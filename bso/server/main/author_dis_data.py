import pandas as pd
import os
import requests
import pymongo
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object, delete_object, delete_objects, init_cmd, download_container, upload_object_with_destination
from bso.server.main.utils import to_jsonl
from bso.server.main.utils_upw import chunks
import hashlib
import json
import gzip

logger = get_logger(__name__)

def extract_from_theses():
    res = []
    for ix, f in enumerate(os.listdir('/upw_data/theses/20251007/parsed')):
        print(f)
        current_path = f'/upw_data/theses/20251007/parsed/{f}'
        res = extract_from_file_these(current_path)
        pd.DataFrame(res).to_json(f'/upw_data/french_etd_{ix}.jsonl.gz', orient='records', lines=True)
        upload_object_with_destination('author-disambiguation-data', f'/upw_data/french_etd_{ix}.jsonl.gz', f'french_etd/french_etd_{ix}.jsonl.gz')
    #return res

def extract_from_file_these(f):
    res = []
    with gzip.open(f'{f}', 'r') as fin:
        current_publications = json.loads(fin.read().decode('utf-8'))
    for p in current_publications:
        new_p = {}
        for f in ['nnt_id', 'genre', 'title', 'year']:
            if f in p:
                new_p[f] = p[f]
        authors = []
        if isinstance(p.get('authors'), list):
            for a in p['authors']:
                new_a = {}
                for f in ['first_name', 'last_name', 'full_name', 'idref', 'role']:
                    if a.get(f):
                        new_a[f] = a[f]
                authors.append(new_a)
        new_p['authors'] = authors
        res.append(new_p)
    return res

def extract_from_hal():
    res = []
    for ix, f in enumerate(os.listdir('/upw_data/hal/20251021/parsed')):
        print(f)
        current_path = f'/upw_data/hal/20251021/parsed/{f}'
        res = extract_from_file_hal(current_path)
        pd.DataFrame(res).to_json(f'/upw_data/hal_{ix}.jsonl.gz', orient='records', lines=True)
        upload_object_with_destination('author-disambiguation-data', f'/upw_data/hal_{ix}.jsonl.gz', f'hal/hal_{ix}.jsonl.gz')
    #return res

def extract_from_file_hal(f):
    res = []
    with gzip.open(f'{f}', 'r') as fin:
        current_publications = json.loads(fin.read().decode('utf-8'))
    for p in current_publications:
        new_p = {}
        for f in ['hal_id', 'genre', 'title', 'year']:
            if f in p:
                new_p[f] = p[f]
        authors = []
        one_auth_has_id = False
        if isinstance(p.get('authors'), list):
            for a in p['authors']:
                new_a = {}
                for f in ['first_name', 'last_name', 'full_name', 'idref', 'orcid', 'id_hal_s', 'role']:
                    if a.get(f):
                        new_a[f] = a[f]
                        if f in ['idref', 'orcid', 'id_hal_s']:
                            one_auth_has_id = True
                authors.append(new_a)
        new_p['authors'] = authors
        if one_auth_has_id:
            res.append(new_p)
    return res
