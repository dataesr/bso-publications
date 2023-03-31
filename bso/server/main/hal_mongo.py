import json
import gzip
import os
import pymongo
import random

from retry import retry
from typing import Union

from bso.server.main.config import MONGO_URL, MOUNTED_VOLUME
from bso.server.main.logger import get_logger

client = None
logger = get_logger(__name__)


def get_client() -> Union[pymongo.MongoClient, None]:
    global client
    if client is None:
        client = pymongo.MongoClient(MONGO_URL, connectTimeoutMS=60000)
    return client


def get_database(database: str = 'hal') -> Union[pymongo.database.Database, None]:
    _client = get_client()
    db = _client[database]
    return db


@retry(delay=200, tries=2)
def get_collection(collection_name: str) -> Union[pymongo.collection.Collection, None]:
    db = get_database()
    collection = db[collection_name]
    return collection


def clean(res: dict, coll: str) -> dict:
    if res:
        if '_id' in res:
            del res['_id']
        res['asof'] = coll
    return res


def get_hal_id(hal_id, collection_name: str) -> dict:
    collection = get_collection(collection_name=collection_name)
    res = {}
    if isinstance(hal_id, str):
        res = collection.find_one({'hal_id': hal_id})
        res = clean(res, collection_name)
    elif isinstance(hal_id, list):
        res = [e for e in collection.find({'hal_id': {'$in': hal_id}})]
        for ix, e in enumerate(res):
            res[ix] = clean(e, collection_name)
    return res

def get_hal_ids_full(hal_ids: list, observations: list, last_observation_date_only: bool) -> dict:
    logger.debug(f'Getting hal_id info for {len(hal_ids)} hal_ids')
    db = get_database()
    res = {}
    for d in hal_ids:
        res[d] = {}
    collections = db.list_collection_names()
    collections_dates = [col for col in collections if col[0:2] == '20']

    for collection in collections:
        if observations and (collection not in observations) and (collection != 'global'):
            continue
        if last_observation_date_only and (collection in collections_dates) and (collection != max(collections_dates)):
            continue
        logger.debug(f'Collection: {collection}')
        current_list = get_hal_id(hal_ids, collection)
        for e in current_list:
            d = e['hal_id']
            res[d].update(e['oa_details'])
    logger.debug('Getting doi infos DONE')
    return res
