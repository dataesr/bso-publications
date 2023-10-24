import datetime
import os
import pymongo
import requests
from urllib import parse

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.elastic import reset_index
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_mongo import drop_collection
from bso.server.main.utils import download_file
from bso.server.main.utils_swift import download_object, get_list_files

logger = get_logger(__name__)
UPW_API_KEY = os.getenv('UPW_API_KEY')
small_url = f'http://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2021-01-08T080001' \
            f'.jsonl.gz?api_key={UPW_API_KEY}'
medium_url = f'http://api.unpaywall.org/feed/changefile/changed_dois_with_versions_2020-12-15T080001_to_2020-12' \
             f'-24T080001.jsonl.gz?api_key={UPW_API_KEY}'
url_snapshot = f'http://api.unpaywall.org/feed/snapshot?api_key={UPW_API_KEY}'
url = url_snapshot

def load_collection_from_object_storage(collection_name: str) -> None:
    # 1. Drop mongo collection
    logger.debug(f'Dropping {collection_name} collection before insertion')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    myclient['unpaywall'][collection_name].drop()
    # 2. Find the file path for the Unpaywall snapshot on OVH Object Storage
    full_date = f'{collection_name[0:4]}-{collection_name[4:6]}-{collection_name[6:8]}'
    files = get_list_files(container='unpaywall', prefix=f'unpaywall_snapshot_{full_date}T')
    if len(files) == 0:
        logger.debug(f'no unpaywall snapshot found for {full_date}')
        return
    if len(files) > 1:
        logger.debug(f'more than one unpaywall snapshot found for {full_date}: {files}')
        return
    assert(len(files) == 1)
    # 3. Download the file
    file_path = f'/upw_data/unpaywall_snapshot_{full_date}T083001.jsonl.gz'
    download_object(container='unpaywall', filename=files[0], out=file_path)
    # 4. Load this file into a mongo collection
    snapshot_to_mongo(f=file_path, global_metadata=False, delete_input=False)
    os.system(f'rm -rf {file_path}')
    return

def snapshot_to_mongo(f: str, global_metadata: bool = False, delete_input: bool = False) -> None:
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['unpaywall']
    output_json = f'{f}_mongo.jsonl'
    collection_name = f.replace(MOUNTED_VOLUME, '').replace('/', '').replace('unpaywall_snapshot_', '')[0:10].replace('-', '')
    snapshot_date = collection_name
    logger.debug(f'collection_name: {collection_name}')
    logger.debug(f'output_json: {output_json}')
    if global_metadata:
        collection_name = 'global'
    start = datetime.datetime.now()
    logger.debug(f'jq file {f} start at {start}')
    jq_oa = f'zcat {f} | '
    if global_metadata:
        jq_oa += "jq -r -c '{doi, genre, is_paratext, journal_issns, journal_issn_l, journal_name, published_date, " \
                 "publisher, title, year, z_authors}'"
    else:
        jq_oa += "jq -r -c '{doi, is_oa, oa_locations, journal_is_oa, journal_is_in_doaj, oa_locations_embargoed, oa_status}'"
    logger.debug(jq_oa)
    os.system(f'{jq_oa} > {output_json}')
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'jq done in {delta}')

    ## mongo start
    start = datetime.datetime.now()
    drop_collection(collection_name)
    mongoimport = f"mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/unpaywall --file {output_json}" \
                  f" --collection {collection_name}"
    logger.debug(f'Mongoimport {f} start at {start}')
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('doi')
    mycol.create_index('year')
    mycol.create_index('is_oa')
    mycol.create_index('publisher')
    mycol.create_index('journal_issn_l')
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Mongoimport done in {delta}')
    ## mongo done

    ## elastic start
    create_full_index = False
    if collection_name == 'global' and create_full_index:
        start = datetime.datetime.now()
        es_url_without_http = ES_URL.replace('https://','').replace('http://','')
        es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
        es_index = f'publications-{snapshot_date}'
        reset_index(index=es_index)
        elasticimport = f"elasticdump --input={output_json} --output={es_host}{es_index} --type=data --limit 10000 " + "--transform='doc._source=Object.assign({},doc)'"
        logger.debug(f'{elasticimport}')
        logger.debug('starting import in elastic')
        os.system(elasticimport)
        end = datetime.datetime.now()
        delta = end - start
        logger.debug(f'Elasticimport done in {delta}')
    ## elastic done

    logger.debug(f'deleting {output_json}')
    os.remove(output_json)
    if delete_input:
        logger.debug(f'Deleting {f}')
        os.remove(f)
    myclient.close()


def download_snapshot(asof: str = None, upload_to_object_storage: bool = True) -> str:
    try:
        url_old = f'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_{asof}.jsonl.gz'
        return download_file(url_old, upload_to_object_storage)
    except:
        return download_file(url_snapshot, upload_to_object_storage)


def download_daily(date: str) -> str:
    daily_files = requests.get(f'https://api.unpaywall.org/feed/changefiles?api_key={UPW_API_KEY}&interval=day') \
        .json()['list']
    daily_url = [e for e in daily_files if e.get('date') == date and e.get('filetype') == 'jsonl'][0]['url']
    return download_file(daily_url)
