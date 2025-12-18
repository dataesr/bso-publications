import pandas as pd
import os
import requests
import pymongo
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object, delete_object, delete_objects, init_cmd, download_container
from bso.server.main.utils import to_jsonl
from bso.server.main.utils_upw import chunks
import hashlib
import json
import gzip

logger = get_logger(__name__)

def extract_from_theses():
    res = []
    for f in os.listdir('/upw_data/theses/20251007/parsed'):
        print(f)
        current_path = f'/upw_data/theses/20251007/parsed/{f}'
        res += extract_from_file_these(current_path)
    pd.DataFrame(res).to_json('/upw_data/french_etd.json.gz')
    upload_object('author-disambiguation-data', '/upw_data/french_etd.json.gz')
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

