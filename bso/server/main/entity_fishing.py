import os
import requests
import time

from bso.server.main.logger import get_logger


ENTITY_FISHING_SERVICE = os.getenv('ENTITY_FISHING_SERVICE')


logger = get_logger(__name__)


def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            logger.error(f'{func.__name__} raises an error through decorator "exception_handler".')
            logger.error(exception)
            return None
    return inner_function


@exception_handler
def get_entity_fishing(publication: dict) -> dict:
    ans = {}

    lang = publication.get('lang')
    if lang not in ['en', 'fr']:
        lang = 'en'

    title = publication.get('title')
    if not title:
        title = ''
    keywords = ' '.join([k['keyword'] for k in publication.get('keywords', []) if k and 'keyword' in k)
    abstract = ' '.join([k['abstract'] for k in publication.get('abstracts', []) if k and 'abstract' in k)
    text = f"{title} {keywords} {abstract}".strip()

    if text:
        json = {
        "text": text,
        "language": {"lang": lang},
        "mentions": [ "wikipedia"] 
        }
        r = requests.post(f"{ENTITY_FISHING_SERVICE}/service/disambiguate", "params":json)
        res = r.json()

        classifications = publication.get('classifications', [])
        global_categories = [{'label': r['category'], 'code':r['page_id'], 'reference': r['source']} for r in res['global_categories']]
        wikidataIds = [{'code': r['wikidataId'], 'label': r['rawName'], 'reference': 'wikidata'} for r in res['entities']]

        domains = []
        for r in res['entities']:
            for d in r['domains']:
                domains.append({'label': d, 'code': r['wikipediaExternalRef'], 'reference': 'wikipedia'})

        classifications += global_categories + wikidataIds + domains
        ans = {'classifications': classifications }
    return ans
