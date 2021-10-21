import os
import pymongo

import pandas as pd

from bso.server.main.config import PV_MOUNT
from bso.server.main.logger import get_logger

logger = get_logger(__name__)


def update_inventory(elts: list) -> None:
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['unpaywall']
    output_json = f'{PV_MOUNT}current_list_inventory_mongo.jsonl'
    pd.DataFrame(elts).to_json(output_json, lines=True, orient='records')
    collection_name = 'inventory'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/unpaywall --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('doi')
    mycol.create_index('crawl')
    logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
