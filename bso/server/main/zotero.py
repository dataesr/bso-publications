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
ANR_LIBRARY_ID = os.getenv('ANR_LIBRARY_ID')

def parse_zotero(items):
    elts = []
    for item in items:
        data = item['data']
        elt = {}
        for f in data:
            if f.lower() == 'doi':
                doi = clean_doi(data[f])
                if doi:
                    elt['doi'] = data[f].strip()
        for tag in data.get('tags', []):
            if tag.get('tag')[0:4]=='ANR-':
                code_decision = tag['tag'].strip()
                elt['project_id'] = code_decision
                elt['agency'] = 'ANR'
                funding_year = '20' + code_decision[4:6]
                elt['funding_year'] = funding_year
        if 'doi' in elt:
            elts.append(elt)
    return elts

def make_file_ANR(args):
    zot = zotero.Zotero(ANR_LIBRARY_ID, library_type, ZOTERO_KEY)
    data = []
    first_data = zot.top(limit=100)
    data += parse_zotero(first_data)
    for i in range(0, 10000):
        logger.debug(f'getting zotero data {i}')
        try:
            next_data = zot.follow()
        except:
            logger.debug(f'stopping get next data from zotero at {i}')
            break
        data += parse_zotero(next_data)
    df = pd.DataFrame(data)
    df.to_csv('ANR.csv', sep=',', index=False)
    upload_object('bso-local', 'ANR.csv')
