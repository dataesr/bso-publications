import os
import requests

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


def get_from_mongo(pid, myclient):
    mydb = myclient['scanr']
    collection_name = 'classifications'
    mycoll = mydb[collection_name]
    res = mycoll.find_one({'id': pid})
    if res:
        return {'classifications': res['cache']}
    return


@exception_handler
def get_entity_fishing(compute_new, publication: dict, myclient) -> dict:
    
    is_ok_to_use_cache = True

    if is_ok_to_use_cache:
        pre_computed = get_from_mongo(publication['id'], myclient)
        if pre_computed and isinstance(pre_computed.get('classifications'), list):
            return pre_computed

    if compute_new is False:
        return {}

    logger.debug(f"compute classifications from entity fishing for {publication['id']}")
    ans = {}

    lang = publication.get('lang')
    if lang not in ['en', 'fr']:
        lang = 'en'

    title = publication.get('title')
    if not title:
        title = ''
    keywords = ' '.join([k['keyword'] for k in publication.get('keywords', []) if (k and ('keyword' in k))])
    abstracts = []
    if isinstance(publication.get('abstracts'), list):
        abstracts = publication['abstracts']
    elif len(abstracts) == 0 and isinstance(publication.get('abstract'), list):
        abstracts = publication['abstract']
    abstract = ' '.join([k['abstract'] for k in abstracts if (k and ('abstract' in k))])
    text = f"{title} {keywords} {abstract}".strip()

    if text and isinstance(text, str) and len(text)>20:
        params = {
        "text": text,
        "language": {"lang": lang},
        "mentions": [ "wikipedia"] 
        }
        r = requests.post(f"{ENTITY_FISHING_SERVICE}/service/disambiguate", json = params)
        res = r.json()

        classifications = publication.get('classifications', [])
        global_categories = [{'label': r['category'], 'code':r['page_id'], 'reference': r['source']} for r in res.get('global_categories', []) if 'category' in r]
        wikidataIds = [{'code': r['wikidataId'], 'label': r['rawName'], 'reference': 'wikidata'} for r in res.get('entities', []) if 'wikidataId' in r]

        domains = []
        for r in res.get('entities', []):
            for d in r.get('domains', []):
                domains.append({'label': d, 'code': r['wikipediaExternalRef'], 'reference': 'wikipedia'})

        classifications += global_categories + wikidataIds + domains
        ans = {'classifications': classifications }
    return ans
