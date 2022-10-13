import requests
import pandas as pd
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

code_details = {}
def get_anr_open_data():
    all_df=[]
    code_details = {}
    urls = [
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/87d29a24-392e-4a29-a009-83eddcff3e66', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/74a59cc0-ef79-458a-83e0-f181f9da459f', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/aca6972b-577c-496a-aa26-009f81256dcb', 'type': 'DGPIE'}
    ]
    for elt in urls:
        anr_type = 'ANR '+elt['type']
        url = elt['url']
        print(f'getting open data from {url} for {anr_type}')
        df = pd.read_csv(url, sep=';')
        all_df.append(df)
        for i, row in df.iterrows():
            code = row['Projet.Code_Decision_ANR']
            code_clean = code.strip().upper()
            code_details[code_clean] = {'sub_agency': anr_type,
                                        'agency': 'ANR',
                                        'grantid': code_clean,
                                        'funding_year': int('20'+code_clean.split('-')[1])
                                       }
            if 'Programme.Acronyme' in row and isinstance(row['Programme.Acronyme'], str):
                code_details[code_clean]['program'] = row['Programme.Acronyme'].strip()
            elif 'Action.Titre.Francais' in row and isinstance(row['Action.Titre.Francais'], str):
                code_details[code_clean]['program'] = row['Action.Titre.Francais'].strip()
    return code_details

def get_anr_details(code):
    global code_details
    if not code_details:
        code_details = get_anr_open_data()
    if code in code_details:
        return code_details[code]
    else:
        logger.debug(f'code {code} not in ANR open data')
    return None

def normalize_grant(grant):
    if not isinstance(grant.get('grantid'), str):
        return None
    if 'funding_year' in grant and not isinstance(grant['funding_year'], int):
        grant['funding_year'] = int(grant['funding_year'])
    grantid = grant['grantid']
    if grantid[0:3]=='ANR':
        return get_anr_details(grantid)
    if not isinstance(grant.get('agency'), str):
        return None
    agency = grant['agency']
    if 'NIH HHS' in agency:
        new_grant = grant.copy()
        new_grant['agency'] = 'NIH HHS'
        new_grant['sub_agency'] = agency
        return new_grant
    return None

