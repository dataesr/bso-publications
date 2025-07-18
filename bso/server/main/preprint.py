import requests
import pandas as pd
from bso.server.main.logger import get_logger
from retry import retry

logger = get_logger(__name__)

preprints = {}

@retry(delay=200, tries=5)
def get_preprint_data():
    url = 'https://zenodo.org/records/15124417/files/crossref-preprint-article-relationships-Feb-2025.csv?download=1'
    df = pd.read_csv(url, sep=',')
    logger.debug(f'{len(df)} records downloaded from crossref preprint dataset')
    preprints = {}
    #retraction_notes = {}
    for i, row in df.iterrows():
        preprint_doi = row['preprint_doi'].lower().strip()
        article_doi = row['article_doi'].lower().strip()
        if (not isinstance(article_doi, str)) and ((not isinstance(preprint_doi, str))):
            continue
        preprint_details = {}
        preprint_details['has_preprint'] = True
        if 'deposited_by_article_publisher' in row and isinstance(row['deposited_by_article_publisher'], bool):
            preprint_details['deposited_by_article_publisher'] = row['deposited_by_article_publisher']
        if 'deposited_by_preprint_publisher' in row and isinstance(row['deposited_by_preprint_publisher'], bool):
            preprint_details['deposited_by_preprint_publisher'] = row['deposited_by_preprint_publisher']
        preprint_details['article_doi'] = article_doi
        preprint_details['preprint_doi'] = preprint_doi
        preprints[article_doi] = preprint_details
    return preprints

def detect_preprint(doi):
    global preprints
    if not preprints:
        preprints = get_preprint_data()
    if doi in preprints:
        preprint_details = preprints[doi]
    else:
        preprint_details = {'has_preprint': False}
    res = {'preprint_details': preprint_details}
    return res
