import datetime
import json
import os
import pandas as pd
import requests

from dateutil import parser

from bso.server.main.elastic import load_in_es, reset_index, update_alias
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_mongo import get_not_crawled
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import download_file
from bso.server.main.inventory import update_inventory

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
    upw_api_key = os.getenv('UPW_API_KEY')
    crawler_url = os.getenv('CRAWLER_SERVICE')
    weekly_files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={upw_api_key}&interval=week'
    #daily_files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={upw_api_key}&interval=day'
    # START_YEAR = 2013
    START_YEAR = 2021
    weekly_files = requests.get(weekly_files_url).json()['list']
    destination = f'{PV_MOUNT}/weekly_upw.jsonl.gz'
    download_file(weekly_files[0]['url'], upload_to_object_storage=False, destination=destination)
    chunks = pd.read_json(destination, lines=True, chunksize = 5000)
    for c in chunks:
        sub_df = c[c.year >= START_YEAR]
        element_to_crawl = get_not_crawled(sub_df.doi.tolist())
        logger.debug(f'{len(c)} lines in weekly upw file')
        publis_with_affiliation = []
        for i, row in sub_df.iterrows():
            title = row.title
            doi = row.doi
            if doi not in element_to_crawl:
                continue
            # crawler
            if title and doi:
                title = title.strip()
                doi = doi.strip()
                url = f'http://doi.org/{doi}'
                requests.post(f'{crawler_url}/tasks', json={'url': url, 'title': title})
            
            ## récupération des affiliations de crossref
            affiliations = []
            if not isinstance(row.z_authors, list):
                continue
            for a in row.z_authors:
                if 'affiliation' in a:
                    for aff in a['affiliation']:
                        if aff not in affiliations:
                            affiliations.append(aff)
            if affiliations:
                p = {'doi': row.doi, 'affiliations': affiliations, 'authors': row.z_authors}
                publis_with_affiliation.append(p)


        update_inventory([{'doi': doi, 'crawl': True, 'crawl_update': datetime.datetime.today().isoformat()} for doi in element_to_crawl])


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
    start_date = parser.parse(start_string).date()
    end_date = parser.parse(end_string).date()
    nb_days = (end_date - start_date).days
    prefix_format = args.get('prefix_format', '%Y/%m')
    prefixes = list(set([(start_date + datetime.timedelta(days=days)).strftime(prefix_format)
                         for days in range(nb_days)]))
    prefixes.sort()
    doi_in_index = []
    # Pubmed data
    for prefix in prefixes:
        logger.debug(f'Getting parsed objects for {prefix} from object storage (pubmed)')
        publications = get_objects_by_prefix(container='pubmed', prefix=f'parsed/fr/{prefix}')
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        enriched_publications = enrich(publications=publications)
        logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
        loaded = load_in_es(data=enriched_publications, index=index)
        doi_in_index += [p['doi'] for p in loaded]
    logger.debug('Pubmed publications indexed. now indexing other french publications')
    doi_in_index_set = set(doi_in_index)
    # Crawled data
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
        loaded = load_in_es(data=enriched_publications, index=index)
        doi_in_index += [p['doi'] for p in loaded]
    # Other dois
    download_object(container='publications-related', filename='dois_fr.json', out=f'{PV_MOUNT}/dois_fr.json')
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
