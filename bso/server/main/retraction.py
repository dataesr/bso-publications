import requests
import pandas as pd
from bso.server.main.logger import get_logger
from retry import retry

logger = get_logger(__name__)

retracted = {}
retraction_notes = {}

@retry(delay=200, tries=5)
def get_retraction_data():
    url = 'https://api.labs.crossref.org/data/retractionwatch?bso@recherche.gouv.fr'
    try:
        df = pd.read_csv(url, sep=',')
        logger.debug(f'{len(df)} records downloaded from retractionwatch')
    except:
        try:
            df = pd.read_csv(url, sep=',', encoding='iso-8859-1')
            logger.debug(f'{len(df)} records downloaded from retractionwatch')
        except:
            logger.debug(f'PROBLEM WITH retraction watch DATA download ! {url}')
            return {}, {}
    retracted = {}
    retraction_notes = {}
    for i, row in df.iterrows():
        retraction_id = row['Record ID']
        try:
            retraction_doi = row['RetractionDOI'].lower().strip()
        except:
            retraction_doi = None
            #print(f'no retraction_doi for retraction {retraction_id}')
 
        try:
            original_doi = row['OriginalPaperDOI'].lower().strip()
        except:
            original_doi = None
            if retraction_doi:
                original_doi = retraction_doi
                retraction_doi = None
        
        retraction_details = {}
        retraction_details['is_retracted'] = True
        if 'RetractionNature' in row:
            retraction_details['retraction_nature'] = row['RetractionNature']
        if 'Reason' in row:
            retraction_details['retraction_reason'] = row['Reason']
        if 'RetractionDate' in row: 
            retraction_details['retraction_date'] = row['RetractionDate']
        if 'Notes' in row:
            retraction_details['retraction_notes'] = row['Notes']
        retraction_details['retraction_doi'] = retraction_doi
        retraction_details['retraction_id'] = str(retraction_id)
        retracted[original_doi] = retraction_details
        if retraction_doi:
            retraction_notes[retraction_doi] = 1

    return retracted, retraction_notes

def detect_retraction(doi):
    global retracted
    global retraction_notes
    if not retracted:
        retracted, retraction_notes = get_retraction_data()
    if doi in retracted:
        retraction_details = retracted[doi]
    else:
        retraction_details = {'is_retracted': False}
    res = {'retraction_details': retraction_details}
    if doi in retraction_notes:
        res['genre'] = 'retraction_note'
    return res
