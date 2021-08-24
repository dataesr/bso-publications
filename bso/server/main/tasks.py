import datetime
import json
import os
import requests
import pandas as pd
import dateutil.parser

from bso.server.main.elastic import load_in_es, reset_index, update_alias
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_prefix, get_objects_by_page
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import download_file

PV_MOUNT = '/upw_data'
logger = get_logger(__name__)


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    return enrich(publications=publications)


def create_task_download_unpaywall(args: dict) -> str:
    if args.get('type') == 'snapshot':
        snap = download_snapshot(asof=args.get('asof'))
    elif args.get('type') == 'daily':
        today = datetime.date.today()
        snap = download_daily(date=f'{today}')
    else:
        snap = None
    return snap

def create_task_unpaywall_to_crawler(arg):
    UPW_API_KEY = os.getenv('UPW_API_KEY')
    CRAWLER_URL = os.getenv('CRAWLER_SERVICE')
    daily_files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={UPW_API_KEY}&interval=day'
    daily_files = requests.get(daily_files_url).json()['list']
    destination=f'{PV_MOUNT}/daily_upw.jsonl.gz'
    download_file(daily_files[0]['url'], upload_to_object_storage=False, destination=destination)
    df = pd.read_json(destination, lines=True)
    logger.debug(f'{len(df)} lines in daily upw file')
    for i, row in df[df.year >= 2013].iterrows():
        title = row.title
        doi = row.doi
        if title and doi:
            title = title.strip()
            doi = doi.strip()
            url = f'http://doi.org/{doi}'
            logger.debug(f"sending doi {doi} ({title}) to crawler")
            requests.post(f"{CRAWLER_URL}/tasks", json={'url': url, 'title': title})



def create_task_load_mongo(args: dict) -> None:
    asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    filename = download_snapshot(asof).split('/')[-1]
    logger.debug(f'Filename after download is {filename}')
    path = f'{PV_MOUNT}/{filename}'
    if os.path.exists(path=path):
        snapshot_to_mongo(f=path, global_metadata=True, delete_input=False)
        snapshot_to_mongo(f=path, global_metadata=False, delete_input=False)
        logger.debug(f'Deleting file {path}')
        os.remove(path)


def create_task_etl(args: dict) -> None:
    current_date = datetime.date.today().isoformat()
    index = args.get('index', f'bso-publications-{current_date}')
    alias = 'bso-publications'
    logger.debug(f'Reset index {index}')
    reset_index(index=index)
    start_string = args.get('start', '2013-01-01')
    end_string = args.get('end', datetime.date.today().isoformat())
    start_date = dateutil.parser.parse(start_string).date()
    end_date = dateutil.parser.parse(end_string).date()
    nb_days = (end_date - start_date).days
    prefix_format = args.get('prefix_format', '%Y/%m')
    prefixes = list(set([(start_date + datetime.timedelta(days=days)).strftime(prefix_format)
                         for days in range(nb_days)]))
    prefixes.sort()
    doi_in_index = []
    # pubmed data
    for prefix in prefixes:
        logger.debug(f'Getting parsed objects for {prefix} from object storage (pubmed)')
        publications = get_objects_by_prefix(container='pubmed', prefix=f'parsed/fr/{prefix}')
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        enriched_publications = enrich(publications=publications)
        logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
        load_in_es(data=enriched_publications, index=index)
        doi_in_index += [p['doi'] for p in enriched_publications]
    logger.debug('Pubmed publications indexed. now indexing other french publications')
    doi_in_index_set = set(doi_in_index)
    # crawled data
    for page in range(1, 100000):
        logger.debug(f'Getting parsed objects for page {page} from object storage (crawled)')
        publications = get_objects_by_page(container='parsed_fr', page=page)
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        if len(publications) == 0:
            break
        publications_not_indexed_yet = [p for p in publications if p['doi'] not in doi_in_index_set]
        logger.debug(f'{len(publications_not_indexed_yet)} publications not indexed yet')
        enriched_publications = enrich(publications=publications_not_indexed_yet)
        logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
        load_in_es(data=enriched_publications, index=index)
        doi_in_index += [p['doi'] for p in enriched_publications]
    # other dois
    download_object('publications-related', 'dois_fr.json', f'{PV_MOUNT}/dois_fr.json')
    fr_dois = json.load(open(f'{PV_MOUNT}/dois_fr.json', 'r'))
    doi_in_index_set = set(doi_in_index)
    fr_dois_set = set(fr_dois)
    remaining_dois = list(fr_dois_set - doi_in_index_set)
    logger.debug(f'DOI already in index: {len(doi_in_index_set)}')
    logger.debug(f'French DOI: {len(fr_dois_set)}')
    logger.debug(f'Remaining dois to index: {len(remaining_dois)}')
    for chunk in chunks(remaining_dois, 5000):
        enriched_publications = enrich(publications=[{'doi': d} for d in chunk])
        logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
        load_in_es(data=enriched_publications, index=index)
    update_alias(alias=alias, old_index='bso-publications-*', new_index=index)
