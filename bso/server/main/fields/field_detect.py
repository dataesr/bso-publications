import os
import requests
import numpy as np
import pandas as pd
import ast
import json
from retry import retry

from bso.server.main.logger import get_logger

SCIENTIFIC_TAGGER_SERVICE = os.getenv('SCIENTIFIC_TAGGER_SERVICE')
EMBEDDINGS_SERVICE = os.getenv('EMBEDDINGS_SERVICE')

logger = get_logger(__name__)

df_dewey = pd.read_csv('bso/server/main/fields/dewey.csv',sep=';') 
dewey_code_dict = {}
for i, row in df_dewey.iterrows():
    codes = ast.literal_eval(row.codes)
    for c in codes:
        dewey_code_dict[c] = {"discipline": row.discipline, "macro":row.macro}

hal_bso = json.load(open('bso/server/main/fields/hal_bso.json', 'r'))

def get_classification_hal(hal_classification):
    max_hal_depth = np.max([len(k.get('code','').split('.')) for k in hal_classification])
    hal_classification_filtered = [k for k in hal_classification if len(k.get('code','').split('.')) == max_hal_depth]
    classif = {'hal_code': 'unknown', 'discipline': 'unknown'}
    for c in hal_bso:
        for k in hal_classification_filtered:
            if c in k.get('code'):
                classif = {'hal_code': c, 'discipline': hal_bso[c]}
    return classif

def get_classification_dewey(publi_codes):
    thesis_classification = {"discipline": "unknown", "macro":"unknown"}
    for c in publi_codes:
        if c['reference'] != 'dewey':
            continue
        if c['code'] in dewey_code_dict:
            thesis_classification = dewey_code_dict[c['code']]
            break
        if c['code'][0:1] in dewey_code_dict:
            thesis_classification = dewey_code_dict[c['code'][0:1]]
            break
    return thesis_classification


@retry(delay=2, tries=50)
def get_embeddings(a_publication):
    embeddings = None
    texts = []
    #title
    if isinstance(a_publication.get('title'), str):
        texts.append(a_publication['title'])
    #domains
    if isinstance(a_publication.get('classifications'), list):
        for c in a_publication['classifications']:
            if isinstance(c.get('label'), str):
                texts.append(c['label'])
    #abstract
    abstracts = []
    if isinstance(a_publication.get('abstracts'), list):
        abstracts = a_publication['abstracts']
    elif len(abstracts) == 0 and isinstance(a_publication.get('abstract'), list):
        abstracts = a_publication['abstract']
    for ix, abstr in enumerate(abstracts):
        current_abs = None
        if isinstance(abstr, dict) and isinstance(abstr.get('abstract'), str) and len(abstr.get('abstract')) > 10:
            current_abs = abstr.get('abstract')
            texts.append(current_abs)
    if texts:
        text = ' '.join(texts).replace('\\\\\\\\', '')[0:2000]
        try:
            embeddings = requests.post(f'{EMBEDDINGS_SERVICE}/embeddings', json={'text': text, 'embedding_type': 'large'}).json()['embeddings']
        except:
            logger.debug(f'embeddings error for text {text}')
            embeddings=None
    return embeddings

@retry(delay=2, tries=50)
def call_classifier(a_publication, classif_type):
    r_classif = requests.post(f'{SCIENTIFIC_TAGGER_SERVICE}/classify_one', json={'publications': [a_publication], 'details': True,
                                                                                     'type': classif_type})
    return r_classif

def detect_fields(a_publication, classification_types):
    for classif_type in classification_types:
        if classif_type == 'thesis':
            a_publication['thesis_classification'] = get_classification_dewey(a_publication.get('classifications', []))
            assert(isinstance(a_publication['thesis_classification'], dict))
        elif classif_type == 'bso' and a_publication.get('hal_classification', []):
            a_publication['bso_classification'] = get_classification_hal(a_publication['hal_classification'])['discipline']
            assert(isinstance(a_publication['bso_classification'], str))
        elif classif_type == 'embeddings':
            embeddings = get_embeddings(a_publication)
            if embeddings:
                a_publication['embeddings'] = embeddings
        else:
            r_classif = {}
            try:
                r_classif = call_classifier(a_publication, classif_type).json()
            except:
                logger.debug(f'Error1 in classif {classif_type} for {a_publication}')
                return a_publication
            try:
                a_publication = r_classif.get('publications')[0]
            except:
                logger.debug(f'Error2 in classif {classif_type} for {a_publication} ==> {r_classif}')
                return a_publication
    return a_publication
