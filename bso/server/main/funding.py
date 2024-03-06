import pandas as pd
import re

from bso.server.main.logger import get_logger

logger = get_logger(__name__)

code_details = {}
def get_anr_open_data():
    all_df=[]
    code_details = {}
    urls = [
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/87d29a24-392e-4a29-a009-83eddcff3e66', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/74a59cc0-ef79-458a-83e0-f181f9da459f', 'type': 'DOS'},
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/aca6972b-577c-496a-aa26-009f81256dcb', 'type': 'PIA'}
    ]
    for elt in urls:
        anr_type = 'ANR '+elt['type']
        url = elt['url']
        logger.debug(f'Getting open data from {url} for {anr_type}')
        try:
            df = pd.read_csv(url, sep=';')
        except:
            logger.debug(f'PROBLEM WITH ANR DATA download ! {elt}')
            continue
        all_df.append(df)
        for i, row in df.iterrows():
            code = row['Projet.Code_Decision_ANR']
            code_clean = code.strip().upper()
            code_details[code_clean] = {
                'sub_agency': anr_type,
                'agency': 'ANR',
                'grantid': code_clean,
                'funding_year': int('20'+code_clean.split('-')[1])
            }
            if 'Programme.Acronyme' in row and isinstance(row['Programme.Acronyme'], str):
                code_details[code_clean]['program'] = get_anr_program(row['Programme.Acronyme'].strip())
            elif 'Action.Titre.Francais' in row and isinstance(row['Action.Titre.Francais'], str):
                code_details[code_clean]['program'] = get_anr_program(row['Action.Titre.Francais'].strip())
    return code_details

def get_anr_program(x):
    if 'AAPG' in x or 'générique' in x.lower():
        return 'Appel à projets générique'
    if 'blanc' in x.lower():
        return 'BLANC'
    if x in ['JC', 'JCJC']:
        return 'JCJC'
    return x

def get_anr_details(code):
    global code_details
    if not code_details:
        code_details = get_anr_open_data()
    if code in code_details:
        return code_details[code]
    else:
        logger.debug(f'Code {code} not in ANR open data')
        if isinstance(code, str):
            code_clean = code.strip().upper() # ANR-10-BLANC-0417-01-SOLICRISTAL
            res = {
                'sub_agency': 'unknown',
                'agency': 'ANR',
                'grantid': code.strip().upper()
            }
            try:
                funding_year = int('20'+code_clean.split('-')[1])
                if len(funding_year) == 4 and funding_year.isdigit():
                    res['funding_year'] = funding_year
            except:
                pass
            return res
    return None

def normalize_grant(grant):
    grants = []
    if not isinstance(grant.get('grantid'), str):
        return [grant]
    for grantid in re.split(';|,| ', grant['grantid']):
        grantid = grantid.strip()
        if not grantid:
            continue
        current_grant = grant.copy()
        current_grant['grantid'] = grantid
        if 'funding_year' in grant and not isinstance(grant['funding_year'], int):
            current_grant['funding_year'] = int(grant['funding_year'])
        if grantid[0:4].upper()=='ANR-':
            current_grant = get_anr_details(grantid)
            if current_grant:
                grants.append(current_grant)
        elif isinstance(grant.get('agency'), str):
            agency = grant['agency']
            if 'NIH HHS' in agency:
                current_grant['agency'] = 'NIH HHS'
                current_grant['sub_agency'] = agency
                grants.append(current_grant)
            if 'H2020' in agency:
                current_grant['agency'] = 'H2020'
                current_grant['sub_agency'] = 'H2020'
                grants.append(current_grant)
    return grants

