import pandas as pd
import re

from bso.server.main.logger import get_logger

logger = get_logger(__name__)

anses_code_details = {}
def get_anses_open_data():
    all_df=[]
    anses_code_details = {}
    urls = [
        {'url': 'https://www.data.gouv.fr/fr/datasets/r/d6d580fb-9703-4f39-8df2-585e44594091', 'type': 'PNR EST'}
    ]
    for elt in urls:
        anses_type = 'ANSES '+elt['type']
        url = elt['url']
        logger.debug(f'Getting open data from {url} for {anses_type}')
        try:
            df = pd.read_csv(url, sep=';', encoding='iso-8859-1')
        except:
            logger.debug(f'PROBLEM WITH ANSES DATA download ! {elt}')
            continue
        all_df.append(df)
        for i, row in df.iterrows():
            code = row['code convention homogénéisé']
            code_clean = code.strip().upper()
            anses_code_details[code_clean] = {
                'sub_agency': anses_type,
                'agency': 'ANSES',
                'grantid': code_clean
            }
            funding_year = get_funding_year(code_clean)
            if funding_year:
                anses_code_details[code_clean]['funding_year'] = funding_year
            #if 'Programme.Acronyme' in row and isinstance(row['Programme.Acronyme'], str):
            #    code_details[code_clean]['program'] = get_anr_program(row['Programme.Acronyme'].strip())
            #elif 'Action.Titre.Francais' in row and isinstance(row['Action.Titre.Francais'], str):
            #    code_details[code_clean]['program'] = get_anr_program(row['Action.Titre.Francais'].strip())
    return anses_code_details

#def get_anses_program(x):
#    if 'AAPG' in x or 'générique' in x.lower():
#        return 'Appel à projets générique'
#    if 'blanc' in x.lower():
#        return 'BLANC'
#    if x in ['JC', 'JCJC']:
#        return 'JCJC'
#    return x

def get_funding_year(code):
    try:
        funding_year = '20'+code.split('-')[1]
        if len(funding_year) == 4:
            res = int(funding_year)
            return res
    except:
        return None

def get_anses_details(code):
    global anses_code_details
    if not anses_code_details:
        anses_code_details = get_anses_open_data()
    if code in anses_code_details:
        return anses_code_details[code]
    else:
        logger.debug(f'Code {code} not in ANSES open data')
        if isinstance(code, str):
            code_clean = code.strip().upper()
            res = {
                'sub_agency': 'unknown',
                'agency': 'ANSES',
                'grantid': code.strip().upper()
            }
            funding_year = get_funding_year(code_clean)
            if funding_year:
                res['funding_year'] = funding_year
            return res
    return None
