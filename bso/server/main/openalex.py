from bso.server.main.unpaywall_mongo import get_openalex
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def enrich_with_openalex(publications):
    logger.debug('enrich_with_openalex')
    ids = [k['id'] for k in publications]
    res = get_openalex(ids)
    current_dict = {}
    for k in res:
        current_dict[k['id']] = k
    for p in publications:
        if p['id'] in current_dict:
            p.update(current_dict[p['id']])
    return publications
