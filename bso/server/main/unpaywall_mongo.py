import json
import os
import random

from pymongo import MongoClient

from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import upload_object

PV_MOUNT = '/upw_data/'
logger = get_logger(__name__)


def drop_collection(collection: str) -> None:
    logger.debug(f'Dropping {collection}')
    client = MongoClient('mongodb://mongo:27017/')
    db = client.unpaywall
    collection = db[collection]
    collection.drop()


def clean(res: dict, coll: str) -> dict:
    if res:
        if '_id' in res:
            del res['_id']
        res['asof'] = coll
    return res


def get_doi(doi, coll: str):
    client = MongoClient('mongodb://mongo:27017/')
    db = client.unpaywall
    collection = db[coll]
    if isinstance(doi, str):
        res = collection.find_one({'doi': doi})
        res = clean(res, coll)
        return res
    elif isinstance(doi, list):
        res = [e for e in collection.find({'doi': {"$in": doi}})]
        for ix, e in enumerate(res):
            res[ix] = clean(e, coll)
        return res
    return {}


def get_doi_full(dois: list) -> dict:
    client = MongoClient('mongodb://mongo:27017/')
    db = client.unpaywall
    res = {}
    for d in dois:
        res[d] = {}
    for coll in db.list_collection_names():
        if coll in ["pubmed"]:
            continue
        current_list = get_doi(dois, coll)
        for e in current_list:
            d = e['doi']
            asof = e['asof']
            del e['asof']
            if asof != "global":
                del e['doi']
            res[d].update({asof: e})
    return res


def aggregate(coll: str, pipeline: str, output: str) -> str:
    logger.debug(f'aggregate {pipeline}')
    pipeline_type = type(pipeline)
    logger.debug(f'pipeline_type = {pipeline_type}')
    if isinstance(pipeline, str):
        pipeline = json.loads(pipeline.replace("'", '"'))
    pipeline_type = type(pipeline)
    logger.debug(f'pipeline_type = {pipeline_type}')
    client = MongoClient('mongodb://mongo:27017/')
    db = client.unpaywall
    rdm = random.randint(1, 10000)
    results_col = f'results_{output}_{rdm}'
    pipeline.append({"$out": results_col})
    logger.debug(pipeline)
    db[coll].aggregate(pipeline, allowDiskUse=True)
    output_json = f'{PV_MOUNT}{results_col}'
    export_cmd = f"mongoexport --forceTableScan --uri mongodb://mongo:27017/unpaywall -c {results_col}  " \
                 f"--out={output_json}"
    os.system(export_cmd)
    db[results_col].drop()
    res = upload_object('tmp', output_json)
    os.system(f'rm -rf {output_json}')
    return res
