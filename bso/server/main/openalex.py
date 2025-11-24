from bso.server.main.unpaywall_mongo import get_openalex
from bso.server.main.utils_upw import chunks
from bso.server.main.logger import get_logger
import os
import requests
import pandas as pd

crawler_url = os.getenv('CRAWLER_SERVICE')
OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')
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

def get_new_from_openalex(fr_only, from_date):
    cursor = '*'
    all_results = []
    cond = ''
    if fr_only:
        cond = ',institutions.country_code:FR'
    for i in range(0, 10000):
        url = f"https://api.openalex.org/works?filter=from_created_date:{from_date},from_publication_date:2013-01-01{cond}&select=doi,title&api_key={OPENALEX_API_KEY}&per-page=200&cursor={cursor}"
        res = requests.get(url).json()
        if 'results' not in res:
            break
        current_results = res['results']
        all_results += current_results
        print(len(all_results), end=' ; ')
        next_cursor = res['meta']['next_cursor']
        if next_cursor == cursor:
            break
        else:
            cursor = next_cursor
    df = pd.DataFrame(all_results).dropna().drop_duplicates()
    if 'doi' in df.columns():
        df = df[~pd.isnull(df.doi)]
        df['url'] = df['doi']
        del df['doi']
        data = df.to_dict(orient='records')
        crawl_list = []
        for c in chunks(data, 5000):
            crawl_list = list(c)
            logger.debug(f'posting {len(crawl_list)} elements to crawl')
            requests.post(f'{crawler_url}/crawl', json={'list': crawl_list})
