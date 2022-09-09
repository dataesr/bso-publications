import datetime
import os
import pandas as pd
import re
import requests
import shutil
import hashlib
import json

from bso.server.main.logger import get_logger
from bso.server.main.utils import clean_doi
from bso.server.main.utils_swift import download_object, upload_object, get_objects_by_page

logger = get_logger(__name__)

from pyzotero import zotero
library_type = 'group'
ZOTERO_KEY = os.getenv('ZOTERO_KEY')
#ANR_LIBRARY_ID = os.getenv('ANR_LIBRARY_ID')

def parse_zotero(items, code_type, group_type):
    elts = []
    for item in items:
        data = item['data']
        elt = {}
        doi = None
        for f in data:
            if f.lower() == 'doi':
                doi = clean_doi(data[f])
                if doi:
                    elt['doi'] = doi
        if not doi:
            continue
            #logger.debug(f'no valid doi for item {item}')
        source = None
        for tag_elem in data.get('tags', []):
            tag = tag_elem['tag']
            for k in ['hal', 'wos', 'final', 'rapport']:
                if k in tag.lower():
                    source = tag
                    break
        if source:
            elt['datasource_anr'] = source
        else:
            logger.debug(f'no DATASOURCE for {item}')
        for tag_elem in data.get('tags', []):
            tag = tag_elem['tag']
            if tag[0:4]=='ANR-':
                code_decision = tag.strip()
                elt['project_id'] = code_decision
                funding_year = '20' + code_decision[4:6]
                elt['funding_year'] = funding_year
                if code_decision in code_type:
                    elt['agency'] = code_type[code_decision]
                else:
                    logger.debug(f'{code_decision};{doi}; is in ZOTERO but is not in open data from data.gouv !!')
                    group = '-'.join(code_decision.split('-')[0:3])
                    if group in group_type:
                        elt['agency'] = group_type[group]
                        logger.debug(f'==> using group {group} to get {group_type[group]}')
                    else:
                        logger.debug(f'{group};{doi}; is in ZOTERO but is not in open data from data.gouv !!')
                if elt.get('agency') and elt.get('doi'):
                    elt_generic = elt.copy()
                    elt_generic['agency'] = 'ANR'
                    elts.append(elt)
                    elts.append(elt_generic)
    return elts

def get_open_data():
    code_type = {}
    group_type = {}
    urls = [
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/87d29a24-392e-4a29-a009-83eddcff3e66', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/74a59cc0-ef79-458a-83e0-f181f9da459f', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/aca6972b-577c-496a-aa26-009f81256dcb', 'type': 'DGPIE'}
    ]
    for elt in urls:
        anr_type = 'ANR '+elt['type']
        url = elt['url']
        logger.debug(f'getting open data from {url} for {anr_type}')
        df = pd.read_csv(url, sep=';')
        for code in df['Projet.Code_Decision_ANR'].tolist():
            code_clean = code.strip().upper()
            code_type[code_clean] = anr_type
            group = '-'.join(code_clean.split('-')[0:3]) #ANR-22-XXXX
            if group not in group_type:
                group_type[group] = anr_type
            elif code_type[code_clean] != group_type[group]:
                logger.debug(f'PROBLEM !!! group {group} classified both DOS and DGPIE : {code_type[code_clean]} vs {group_type[group]}')
    return code_type, group_type

def make_file_ANR(args):
    code_type, group_type = get_open_data()
    zot = zotero.Zotero(args.get('ANR_LIBRARY_ID'), library_type, ZOTERO_KEY)
    data = []
    first_data = zot.top(limit=100)
    data += parse_zotero(first_data, code_type, group_type)
    for i in range(0, 10000):
        logger.debug(f'getting zotero data {i}')
        try:
            next_data = zot.follow()
        except:
            logger.debug(f'stopping get next data from zotero at {i}')
            break
        data += parse_zotero(next_data, code_type, group_type)
    df = pd.DataFrame(data)
    df.to_csv('ANR.csv', sep=',', index=False)
    upload_object('bso-local', 'ANR.csv')
