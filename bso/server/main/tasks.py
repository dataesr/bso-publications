import datetime
import json
import os
import pandas as pd
import requests
import pymongo

from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo, load_collection_from_object_storage
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import download_file, get_hash
from bso.server.main.etl import etl
from bso.server.main.affiliation_matcher import get_query_from_affiliation

HTML_PARSER_SERVICE = os.getenv('HTML_PARSER_SERVICE')
logger = get_logger(__name__)
START_YEAR = 2022
parser_endpoint_url = f'{HTML_PARSER_SERVICE}/parse'

def create_task_load_collection_from_object_storage(args):
    logger.debug('function create_task_load_collection_from_object_storage')
    collection_name = args.get('collection_name')
    logger.debug(collection_name)
    if collection_name:
        load_collection_from_object_storage(collection_name)

def to_mongo_cache(input_list, collection_name, upsert=False):
    logger.debug(f'importing {len(input_list)} {collection_name}')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}{collection_name}_cache.jsonl'
    pd.DataFrame(input_list).to_json(output_json, lines=True, orient='records')
    #to_jsonl(input_list, output_json, 'w')
    #collection_name = 'classifications'
    #collection_name = 'affiliations'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    if upsert:
        mongoimport = mongoimport + ' --upsert --upsertFields id'
    #logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    #logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    for f in ['id']:
        mycol.create_index(f)
    #logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    myclient.close()

def create_task_cache_affiliations(args):
    index_name = args.get('index')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    resume_after = args.get('resume_after', 0)
    #collection_name = 'classifications'
    #collection_name = 'affiliations'
    mycolls = {}
    collection_names = ['affiliations', 'classifications']
    for collection_name in collection_names:
        mycolls[collection_name] = mydb[collection_name]
        if '_split_0' in index_name:
            logger.debug(f'dropping {collection_name}')
            mycolls[collection_name].drop()
    input_file = f'/upw_data/scanr-split/{index_name}.jsonl'
    logger.debug(f'reading {input_file}') 
    CHUNK_SIZE = args.get('chunk_size', 10000)
    full = pd.read_json(input_file, lines=True, chunksize=CHUNK_SIZE)
    ix = -1
    for df in full:
        existing_hash = {}
        ix += 1
        if ix * CHUNK_SIZE < resume_after:
            continue
        logger.debug(f'iteration {ix}, {ix*CHUNK_SIZE} publications treated')
        to_save = {}
        for collection_name in collection_names:
            to_save[collection_name] = []
        publis = df.to_dict(orient='records')
        for px, p in enumerate(publis):
            #for f in p:
            #    if isinstance(p[f], list) and len(p[f]) > 150:
            #        logger.debug(f"{p['id']}, {f}, {len(p[f])}") 
            for collection_name in collection_names:
                data = p.get(collection_name)
                if not isinstance(data, list):
                    data = []
                if collection_name == 'affiliations':
                    affiliations = data
                    for aff in affiliations:
                        ids = []
                        # make sure of unicity
                        for x in aff.get('ids', []):
                            if x not in ids:
                                ids.append(x)

                        query = get_hash(get_query_from_affiliation(aff))
                        if query and query not in existing_hash:
                            to_save[collection_name].append({'id': query, 'cache': ids})
                            existing_hash[query] = 1
                if collection_name == 'classifications':
                    to_save[collection_name].append({'id': p['id'], 'cache': data})
                #if collection_name == 'embeddings':
                #    to_save[collection_name].append({'id': p['id'], 'cache': data})
        to_mongo_cache(input_list = to_save['affiliations'],    collection_name = 'affiliations',    upsert=False)
        to_mongo_cache(input_list = to_save['classifications'], collection_name = 'classifications', upsert=False)
        #to_mongo_cache(input_list = to_save['embeddings'], collection_name = 'embeddings', upsert=False)
    myclient.close()

def send_to_parser(publication_json):
    if HTML_PARSER_SERVICE:
        r = requests.post(parser_endpoint_url, json={'doi': publication_json['doi'], 'json': publication_json})
        task_id = r.json()['data']['task_id']
        #logger.debug(f'New task {task_id} for parser')


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    observations = args.get('observations', [])
    affiliation_matching = args.get('affiliation_matching', False)
    entity_fishing = args.get('entity_fishing', False)
    datasource = args.get('datasource', 'user')
    last_observation_date_only = args.get('last_observation_date_only', False)
    hal_dates = args.get('hal_dates', [])
    return enrich(publications=publications, observations=observations, datasource=datasource, affiliation_matching=affiliation_matching,
            entity_fishing=entity_fishing,
            last_observation_date_only=last_observation_date_only, hal_dates=hal_dates)


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


def create_task_unpaywall_to_crawler(is_daily):
    upw_api_key = os.getenv('UPW_API_KEY')
    crawler_url = os.getenv('CRAWLER_SERVICE')
    parser_url = HTML_PARSER_SERVICE
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    destination = f'{MOUNTED_VOLUME}/weekly_upw.jsonl.gz'
    if is_daily:
        files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={upw_api_key}&interval=day'
    else:
        files_url = f'https://api.unpaywall.org/feed/changefiles?api_key={upw_api_key}&interval=week'
    files_to_download = requests.get(files_url, verify=False).json()['list']
    download_file(files_to_download[0]['url'], upload_to_object_storage=False, destination=destination)
    crawl_all = True
    chunks = pd.read_json(destination, lines=True, chunksize=5000)
    for c in chunks:
        c['doi'] = c.doi_url.apply(lambda x:x.replace('https://doi.org/', ''))
        crawl_list = []
        parse_list = []
        sub_df = c[c.year >= START_YEAR]
        if crawl_all:
            element_to_crawl = sub_df.doi.tolist()
        else:
            element_to_crawl = get_not_crawled(sub_df.doi.tolist())
        logger.debug(f'{len(c)} lines in upw file')
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
                crawl_list.append({'url': url, 'title': title})
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
                parse_list.append({'doi': p['doi'], 'json': p})
        logger.debug(f'posting {len(crawl_list)} elements to crawl')
        requests.post(f'{crawler_url}/crawl', json={'list': crawl_list})
        logger.debug(f'posting {len(parse_list)} elements to parse')
        requests.post(f'{parser_url}/parse_list', json={'list': parse_list})
        #update_inventory([{
        #    'doi': doi,
        #    'crawl': True,
        #    'crawl_update': datetime.datetime.today().isoformat()} for doi in element_to_crawl
        #])


def create_task_load_mongo(args: dict) -> None:
    asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    if args.get('filename') is None:
        filename = download_snapshot(asof).split('/')[-1]
    else:
        filename = args.get('filename')
    logger.debug(f'Filename after download is {filename}')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    path = f'{MOUNTED_VOLUME}/{filename}'
    if os.path.exists(path=path):
        snapshot_to_mongo(f=path, global_metadata=True, delete_input=False)
        snapshot_to_mongo(f=path, global_metadata=False, delete_input=False)
        logger.debug(f'Deleting file {path}')
        os.remove(path)

def create_task_et(args: dict) -> None:
    etl(args)
