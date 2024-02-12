import json
import gzip
import os
import pymongo
import random

from retry import retry
from typing import Union

from bso.server.main.config import MONGO_URL, MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import upload_object

client = None
logger = get_logger(__name__)


def get_client() -> Union[pymongo.MongoClient, None]:
    global client
    if client is None:
        client = pymongo.MongoClient(MONGO_URL, connectTimeoutMS=60000)
    return client


def get_database(database: str = 'unpaywall') -> Union[pymongo.database.Database, None]:
    _client = get_client()
    db = _client[database]
    return db


@retry(delay=200, tries=2)
def get_collection(collection_name: str) -> Union[pymongo.collection.Collection, None]:
    db = get_database()
    collection = db[collection_name]
    return collection


def drop_collection(collection_name: str) -> None:
    logger.debug(f'Dropping {collection_name}')
    collection = get_collection(collection_name=collection_name)
    collection.drop()


def clean(res: dict, coll: str) -> dict:
    if res:
        if '_id' in res:
            del res['_id']
        res['asof'] = coll
    return res


@retry(delay=60, tries=5)
def get_not_crawled(doi) -> dict:
    collection_name = 'inventory'
    collection = get_collection(collection_name=collection_name)
    crawled = set([e['doi'] for e in collection.find({'doi': {'$in': doi}})])
    not_crawled = set(doi) - set(crawled)
    return not_crawled


def get_unpaywall_infos(publications, collection_name, file_part) -> None:
    dois = []
    input_infos = {}
    for p in publications:
        doi = p.get('doi')
        if doi:
            dois.append(doi)
            input_infos[doi] = p
    unpaywall_info = get_doi(dois, collection_name)
    for p in unpaywall_info:
        doi = p['doi']
        p.update(input_infos[doi])
    write_file = f'{MOUNTED_VOLUME}bso_extract_{collection_name}_{file_part}.jsonl.gz'
    with gzip.open(write_file, 'wt', encoding="ascii") as zipfile:
        for p in unpaywall_info:
            json.dump(p, zipfile)
            zipfile.write('\n')
    upload_object('bso_dump', write_file)
    os.remove(write_file)
    return unpaywall_info


@retry(delay=60, tries=5)
def get_doi(doi, collection_name: str) -> dict:
    collection = get_collection(collection_name=collection_name)
    res = {}
    if isinstance(doi, str):
        res = collection.find_one({'doi': doi})
        res = clean(res, collection_name)
    elif isinstance(doi, list):
        res = [e for e in collection.find({'doi': {'$in': doi}})]
        for ix, e in enumerate(res):
            res[ix] = clean(e, collection_name)
    return res


@retry(delay=60, tries=5)
def get_doi_from_issn(issns) -> dict:
    collection = get_collection(collection_name='global')
    res = {}
    res = list(collection.find({'journal_issn_l': {'$in': issns}, 'year': { '$gte': 2013 }}))
    return res


def get_dois_meta(dois):
    assert(isinstance(dois, list))
    logger.debug(f'getting metadata title / authors for {len(dois)} dois')
    metadatas = get_doi(dois, 'global')
    ans = {}
    for res in metadatas:
        doi = res['doi']
        final_res = {'doi': doi}
        authors = []
        if 'z_authors' in res and isinstance(res['z_authors'], list):
            for a in res['z_authors']:
                aut = {}
                if a.get('given'):
                    aut['first_name'] = a['given']
                if a.get('family'):
                    aut['last_name'] = a['family']
                if aut:
                    authors.append(aut)
        if authors:
            final_res['authors'] = authors
        final_res['title'] = res.get('title')
        ans[doi] = final_res
    return ans

@retry(delay=120, tries=5)
def get_unpaywall_history(dois: list, observations: list, last_observation_date_only: bool) -> dict:
    logger.debug(f'Getting doi info for {len(dois)} dois')
    db = get_database()
    res = {}
    for d in dois:
        res[d] = {}
    collections = db.list_collection_names()
    collections_dates = [col for col in collections if col[0:2] == '20']

    for collection in collections:
        if collection in ['pubmed', 'inventory']:
            continue
        if observations and (collection not in observations) and (collection != 'global'):
            continue
        if last_observation_date_only and (collection in collections_dates) and (collection != max(collections_dates)):
            continue
        logger.debug(f'Collection: {collection}')
        current_list = get_doi(dois, collection)
        for e in current_list:
            d = e['doi']
            asof = e['asof']
            del e['asof']
            if asof != 'global':
                del e['doi']
            res[d].update({asof: e})
    logger.debug('Getting doi infos DONE')
    return res


def aggregate(coll: str, pipeline: str, output: str) -> str:
    db = get_database()
    logger.debug(f'Aggregate {pipeline}')
    pipeline_type = type(pipeline)
    logger.debug(f'Pipeline_type = {pipeline_type}')
    if isinstance(pipeline, str):
        pipeline = json.loads(pipeline.replace("'", '"'))
    pipeline_type = type(pipeline)
    logger.debug(f'Pipeline_type = {pipeline_type}')
    rdm = random.randint(1, 10000)
    results_col = f'results_{output}_{rdm}'
    pipeline.append({"$out": results_col})
    logger.debug(pipeline)
    db[coll].aggregate(pipeline, allowDiskUse=True)
    output_json = f'{MOUNTED_VOLUME}{results_col}'
    export_cmd = f"mongoexport --forceTableScan --uri mongodb://mongo:27017/unpaywall -c {results_col}  " \
                 f"--out={output_json}"
    os.system(export_cmd)
    db[results_col].drop()
    res = upload_object('tmp', output_json)
    os.remove(output_json)
    return res
