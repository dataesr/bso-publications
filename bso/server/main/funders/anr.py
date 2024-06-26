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
                'grantid': code_clean
            }
            funding_year = get_funding_year(code_clean)
            if funding_year:
                code_details[code_clean]['funding_year'] = funding_year
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

def get_funding_year(code):
    try:
        funding_year = '20'+code.split('-')[1]
        if len(funding_year) == 4:
            res = int(funding_year)
            return res
    except:
        return None

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
            funding_year = get_funding_year(code_clean)
            if funding_year:
                res['funding_year'] = funding_year
            return res
    return None
