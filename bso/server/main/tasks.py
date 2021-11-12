import datetime
import json
import os
import pandas as pd
import requests

from dateutil import parser

from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import download_file, get_dois_from_input

HTML_PARSER_SERVICE = os.getenv('HTML_PARSER_SERVICE')
logger = get_logger(__name__)
START_YEAR = 2020
parser_endpoint_url = f'{HTML_PARSER_SERVICE}/parse'



def send_to_parser(publication_json):
    if HTML_PARSER_SERVICE:
        r = requests.post(parser_endpoint_url, json={'doi': publication_json['doi'], 'json': publication_json})
        task_id = r.json()['data']['task_id']
        logger.debug(f'New task {task_id} for parser')


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    observations = args.get('observations', [])
    affiliation_matching = args.get('affiliation_matching', False)
    return enrich(publications=publications, observations=observations, datasource='user', affiliation_matching=affiliation_matching)


def create_task_download_unpaywall(args: dict) -> str:
    download_type = args.get('type')
    if download_type == 'snapshot':
        snap = download_snapshot(asof=args.get('asof'))
    elif download_type == 'daily':
        today = datetime.date.today()
        snap = download_daily(date=f'{today}')
    else:
        snap = None
    return snap


def create_task_unpaywall_to_crawler():
    upw_api_key = os.getenv('UPW_API_KEY')
    crawler_url = os.getenv('CRAWLER_SERVICE')
    weekly_files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={upw_api_key}&interval=week'
    weekly_files = requests.get(weekly_files_url).json()['list']
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    destination = f'{MOUNTED_VOLUME}/weekly_upw.jsonl.gz'
    download_file(weekly_files[0]['url'], upload_to_object_storage=False, destination=destination)
    chunks = pd.read_json(destination, lines=True, chunksize=5000)
    for c in chunks:
        sub_df = c[c.year >= START_YEAR]
        element_to_crawl = get_not_crawled(sub_df.doi.tolist())
        logger.debug(f'{len(c)} lines in weekly upw file')
        for i, row in sub_df.iterrows():
            title = row.title
            doi = row.doi
            if doi not in element_to_crawl:
                continue
            # Crawler
            if title and doi:
                title = title.strip()
                doi = doi.strip()
                url = f'http://doi.org/{doi}'
                requests.post(f'{crawler_url}/tasks', json={'url': url, 'title': title})
            # Récupération des affiliations de crossref
            affiliations = []
            if not isinstance(row.z_authors, list):
                continue
            for a in row.z_authors:
                if 'affiliation' in a:
                    for aff in a['affiliation']:
                        if isinstance(aff, str):
                            aff = {'name': aff} 
                        if aff not in affiliations:
                            affiliations.append(aff)
            if affiliations:
                p = {'doi': row.doi, 'affiliations': affiliations, 'authors': row.z_authors}
                send_to_parser(p)  # Match country and store results in crossref object storage
        update_inventory([{
            'doi': doi,
            'crawl': True,
            'crawl_update': datetime.datetime.today().isoformat()} for doi in element_to_crawl
        ])


def create_task_load_mongo(args: dict) -> None:
    asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    filename = download_snapshot(asof).split('/')[-1]
    logger.debug(f'Filename after download is {filename}')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    path = f'{MOUNTED_VOLUME}/{filename}'
    if os.path.exists(path=path):
        snapshot_to_mongo(f=path, global_metadata=True, delete_input=False)
        snapshot_to_mongo(f=path, global_metadata=False, delete_input=False)
        logger.debug(f'Deleting file {path}')
        os.remove(path)


def create_task_etl(args: dict) -> None:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    observations = args.get('observations', [])
    datasources = args.get('datasources', ['pubmed_fr', 'parsed_fr', 'crossref_fr', 'dois_fr'])
    affiliation_matching = args.get('affiliation_matching', False)
    erase_index = args.get('erase_index', True)
    current_date = datetime.date.today().isoformat()
    index = args.get('index', f'bso-publications-{current_date}')
    collection_name = args.get('collection_name')
    file_part = 0
    if erase_index:
        logger.debug(f'Reset index {index}')
        reset_index(index=index)
    doi_in_index = set()
    # Pubmed data
    if 'pubmed_fr' in datasources:
        start_string = args.get('start', '2013-01-01')
        end_string = args.get('end', datetime.date.today().isoformat())
        start_date = parser.parse(start_string).date()
        end_date = parser.parse(end_string).date()
        nb_days = (end_date - start_date).days
        prefix_format = args.get('prefix_format', '%Y/%m')
        prefixes = list(set([(start_date + datetime.timedelta(days=days)).strftime(prefix_format)
                             for days in range(nb_days)]))
        prefixes.sort()
        for prefix in prefixes:
            logger.debug(f'Getting parsed objects for {prefix} from object storage (pubmed)')
            publications = get_objects_by_prefix(container='pubmed', prefix=f'parsed/fr/{prefix}')
            publications_not_indexed_yet = [p for p in publications if (p.get('doi') and p['doi'] not in doi_in_index)]
            logger.debug(f'{len(publications)} publications retrieved from object storage')
            enriched_publications = enrich(publications=publications_not_indexed_yet, observations=observations, datasource='pubmed_fr', affiliation_matching=affiliation_matching)
            logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
            loaded = load_in_es(data=enriched_publications, index=index)
            doi_in_index.update([p['doi'] for p in loaded])
        logger.debug('Pubmed publications indexed. now indexing other french publications')
    # Crawled data and affiliation data in crossref
    for container in ['parsed_fr', 'crossref_fr']:
        if container not in datasources:
            continue
        for page in range(1, 1000000):
            logger.debug(f'Getting parsed objects for page {page} from object storage ({container})')
            publications = get_objects_by_page(container=container, page=page, full_objects=True)
            logger.debug(f'{len(publications)} publications retrieved from object storage')
            if len(publications) == 0:
                break
            publications_not_indexed_yet = [p for p in publications if (p.get('doi') and p['doi'] not in doi_in_index)]
            logger.debug(f'{len(publications_not_indexed_yet)} publications not indexed yet')
            enriched_publications = enrich(publications=publications_not_indexed_yet, observations=observations, datasource=container, affiliation_matching=affiliation_matching)
            logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
            loaded = load_in_es(data=enriched_publications, index=index)
            doi_in_index.update([p['doi'] for p in loaded])
    # Other dois
    # tmp_dois_fr a n'utiliser que pour ajouter des DOIs sur un index déjà existant
    # pour ne pas avoir à re-builder l'intégralité de l'index
    for extra_file in ['dois_fr', 'tmp_dois_fr']:
        if extra_file in datasources:
            download_object(container='publications-related', filename=f'{extra_file}.json', out=f'{MOUNTED_VOLUME}/{extra_file}.json')
            fr_dois = json.load(open(f'{MOUNTED_VOLUME}/{extra_file}.json', 'r'))
            fr_dois_set = set(fr_dois)
            remaining_dois = list(fr_dois_set - doi_in_index)
            logger.debug(f'DOI already in index: {len(doi_in_index)}')
            logger.debug(f'French DOI: {len(fr_dois_set)}')
            logger.debug(f'Remaining dois to index: {len(remaining_dois)}')
            for chunk in chunks(remaining_dois, 5000):
                publications_not_indexed_yet = [{'doi': d} for d in chunk]
                enriched_publications = enrich(publications=publications_not_indexed_yet, observations=observations, datasource=f'{extra_file}', affiliation_matching=False)
                logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
                loaded = load_in_es(data=enriched_publications, index=index)
                doi_in_index.update([p['doi'] for p in loaded])
    if 'bso-local' in datasources:
        for page in range(1, 1000000):
            filenames = get_objects_by_page(container = 'bso-local', page=page, full_objects=False)
            for filename in filenames:
                logger.debug(f'processing bso-local {filename}')
                local_affiliations = filename.split('.')[0].split('_')
                current_dois = get_dois_from_input(container='bso-local', filename=filename)
                current_dois_set = set(current_dois)
                for chunk in chunks(current_dois, 500):
                    publications_not_indexed_yet = [{'doi': d} for d in get_doi_not_in_index(index=index, dois=chunk)]
                    enriched_publications = enrich(publications=publications_not_indexed_yet, observations=observations, datasource=f'bso-local', affiliation_matching=False)
                    logger.debug(f'Now indexing {len(enriched_publications)} in {index}')
                    loaded = load_in_es(data=enriched_publications, index=index)
                    doi_in_index.update([p['doi'] for p in loaded])
                    # now all dois are in index
                    # just tag them with the local_affiliations
                    update_local_affiliations(index=index,current_dois=chunk, local_affiliations=local_affiliations)

    # alias update is done manually !
    # update_alias(alias=alias, old_index='bso-publications-*', new_index=index)
