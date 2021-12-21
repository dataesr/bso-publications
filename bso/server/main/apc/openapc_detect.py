import io

import numpy as np
import pandas as pd
import requests

from bso.server.main.logger import get_logger
from bso.server.main.publisher.publisher_detect import detect_publisher

logger = get_logger(__name__)

logger.debug('loading open apc data')
# téléchargement des dernières données openAPC
s_apc = requests.get('https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/apc_de.csv').content
# s_ta = requests.get('https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/transformative_agreements/transformative_agreements.csv').content
s_bpc = requests.get('https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/bpc.csv').content

apc = {}
cols_apc = ['doi', 'euro', 'issn', 'issn_l', 'issn_print', 'issn_electronic', 'period', 'publisher']
cols_bpc = ['doi', 'euro', 'isbn', 'isbn_print', 'isbn_electronic', 'period', 'publisher']
df_apc = pd.read_csv(io.StringIO(s_apc.decode('utf-8')))[cols_apc]
df_bpc = pd.read_csv(io.StringIO(s_bpc.decode('utf-8')))[cols_bpc]
# df_ta = pd.read_csv(io.StringIO(s_ta.decode('utf-8')))[cols_apc]
# df_ta['transformative_agreement'] = True
df_apc['transformative_agreement'] = False
df_bpc['transformative_agreement'] = False

df_openapc = pd.concat([df_apc, df_bpc])

openapc_doi = {}
for i, row in df_openapc.iterrows():
    # if not amount is given, continue
    if pd.isnull(row.get('euro')):
        continue
    doi = None 
    if not pd.isnull(row['doi']):
        doi = row['doi'].lower().strip()
        if doi.startswith('10.'):
            openapc_doi[doi] = row['euro']

    # on n'utilise pas les TA pour construire les moyennes par issn, éditeur, année
    if row.transformative_agreement:
        continue

    year_ok, publisher_ok = None, None
    keys = []
    if row.get('period') and len(str(row['period'])) >= 4:
        year_ok = str(row['period'])[0:4]
    if isinstance(row.get('publisher'), str) and row.get('publisher'):
        publisher_ok = detect_publisher(row['publisher'].strip(), year_ok, doi)['publisher_group']
        
    if year_ok and publisher_ok:
        key_publisher_year = f'PUBLISHER{publisher_ok};YEAR{year_ok}'
        keys.append(key_publisher_year)
    
    if publisher_ok:
        key_publisher = f'PUBLISHER{publisher_ok}'
        keys.append(key_publisher)

    if year_ok:
        key_year = f'YEAR{year_ok}'
        keys.append(key_year)
    for issn in ['issn', 'issn_l', 'issn_print', 'issn_electronic']:
        if not pd.isnull(row[issn]):
            issn_ok = row[issn].strip()
            if issn_ok and year_ok:
                key_issn_year = f'ISSN{issn_ok};YEAR{year_ok}'
                keys.append(key_issn_year)
            if issn_ok:
                key_issn = f'ISSN{issn_ok}'
                keys.append(key_issn)
    keys = list(set(keys))       
    for key in keys:
        if key and key not in apc:
            apc[key] = []
        apc[key].append(row['euro'])
            
apc_avg = {}

THRESHOLD_NB_ISSN = 10
THRESHOLD_NB_PUBLISHER = 50

for k in apc:
    estimation = np.mean(apc[k])
    count = len(apc[k])
    if count < THRESHOLD_NB_ISSN and 'ISSN' in k:
        continue
    if count < THRESHOLD_NB_PUBLISHER and 'PUBLISHER' in k:
        estimation = 0.0
    apc_avg[k] = {'count': count, 'estimation': estimation}
logger.debug(f'open apc data loaded, {len(apc_avg)} keys stored for estimations')

def detect_openapc(doi: str, issns: list, publisher: str, date_str: str) -> dict:
    # si le doi est dans la base openAPC, on récupère directement les informations
    if doi in openapc_doi:
        return {
            'has_apc': True,
            'amount_apc_EUR': openapc_doi[doi],
            'apc_source': 'openAPC',
            'amount_apc_openapc_EUR': openapc_doi[doi]
        }
    # sinon, si des APC sont renseignés pour des articles avec le même ISSN (même revue) et la même année de publication
    # on estime pour ce DOI les APC à la moyenne des APC des articles de la même revue la même année de publication
    # à défaut, on prend la moyenne des APC rencontrés pour cet ISSN (quelle que soit l'année)
    # en dernier recours, en cas de revue inconnue dans openAPC, on assigne la moyenne des APC de l'année
    issns = [issn for issn in issns if isinstance(issn, str)]
    year_ok = None
    if isinstance(date_str, str) and date_str:
        year_ok = date_str[0:4]
    keys_to_try = []
    for issn in issns:
        if year_ok:
            keys_to_try.append({'method': 'issn_year', 'key': f'ISSN{issn.strip()};YEAR{year_ok}'})
    for issn in issns:
        keys_to_try.append({'method': 'issn', 'key': f'ISSN{issn.strip()}'})
    if isinstance(publisher, str) and publisher:
        publisher_ok = detect_publisher(publisher.strip(), year_ok, doi)['publisher_group']
        if year_ok:
            keys_to_try.append({'method': 'publisher_year', 'key': f'PUBLISHER{publisher_ok};YEAR{year_ok}'})
        keys_to_try.append({'method': 'publisher', 'key': f'PUBLISHER{publisher_ok}'})
    #if year_ok:
    #    keys_to_try.append({'method': 'year', 'key': f'YEAR{year_ok}'})
    #else:
    #    keys_to_try.append({'method': 'year', 'key': f'YEAR2020'})
                
    for k in keys_to_try:
        current_key = k['key']
        current_method= k['method']
        if current_key in apc_avg:
            estimation = apc_avg[current_key]['estimation']
            count = apc_avg[current_key]['count']
            return {
                    'has_apc': bool(estimation > 0),
                    'amount_apc_EUR': estimation,
                    'apc_source': 'openAPC_estimation_'+current_method,
                    'amount_apc_openapc_EUR': estimation,
                    'count_apc_openapc_key': count
                }
    return {'has_apc': None}
