import pandas as pd
import os
import requests
import pymongo
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_mongo import get_acknowledgments

logger = get_logger(__name__)

def import_acknowledgments():
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'acknowledgments'
    mydb[collection_name].drop()
    output_json = f'/upw_data/acknowledgments.jsonl'
    os.system(f'rm -rf {output_json}')
    for d in os.listdir('/upw_data/bso3_publications_dump/final_for_bso_2025'):
        cmd = f"cat /upw_data/bso3_publications_dump/final_for_bso_2025/{d} | "
        cmd += "jq -r -c '{doi, acknowledgments}' | grep -v null "
        cmd += f" > {output_json}"
        os.system(cmd)
        mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
        os.system(mongoimport)
        os.system(f'rm -rf {output_json}')
    mycol = mydb[collection_name]
    for f in ['doi']:
        mycol.create_index(f)
    myclient.close()

def enrich_with_acknowledgments(publications):
    logger.debug('enrich_with_acknowledgments')
    dois = [k['id'][3:] for k in publications if k['id'][0:3]=='doi']
    res = get_acknowledgments(dois)
    current_dict = {}
    for k in res:
        current_dict[k['doi']] = k
    for p in publications:
        if p['id'][3:] in current_dict:
            p.update(current_dict[p['id'][3:]])
    return publications
