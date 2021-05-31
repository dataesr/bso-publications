import datetime
import os

import pymongo
import requests

from bso.server.main.unpaywall_mongo import drop_collection
from bso.server.main.utils import download_file

UPW_API_KEY = os.getenv('UPW_API_KEY')
PV_MOUNT = '/upw_data/'

small_url = f'http://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2021-01-08T080001' \
            f'.jsonl.gz?api_key={UPW_API_KEY}'
medium_url = f'http://api.unpaywall.org/feed/changefile/changed_dois_with_versions_2020-12-15T080001_to_2020-12' \
             f'-24T080001.jsonl.gz?api_key={UPW_API_KEY}'
url_snapshot = f'http://api.unpaywall.org/feed/snapshot?api_key={UPW_API_KEY}'
url = url_snapshot


def snapshot_to_mongo(f: str, global_metadata: bool = False) -> None:
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['unpaywall']
    output_json = f'{f}_mongo.jsonl'
    collection_name = f.replace(PV_MOUNT, '').replace('unpaywall_snapshot_', '')[0:10].replace('-', '')
    if global_metadata:
        collection_name = 'global'
    start = datetime.datetime.now()
    print(f'jq file {f} start at {start}', flush=True)
    jq_oa = f'zcat {f} | '
    if global_metadata:
        jq_oa += "jq -r -c '{doi, genre, is_paratext, journal_issns, journal_issn_l, journal_name, published_date, " \
                 "publisher, title, year, z_authors}'"
    else:
        jq_oa += "jq -r -c '{doi, is_oa, oa_locations, journal_is_oa, journal_is_in_doaj, oa_locations_embargoed}'"
    print(jq_oa, flush=True)
    os.system(f'{jq_oa} > {output_json}')
    end = datetime.datetime.now()
    delta = end - start
    print(f'jq done in {delta}', flush=True)
    drop_collection(collection_name)
    start = datetime.datetime.now()
    mongoimport = f"mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/unpaywall --file {output_json}" \
                  f" --collection {collection_name}"
    print(f"mongoimport {f} start at {start}", flush=True)
    print(f"{mongoimport}", flush=True)
    os.system(mongoimport) 
    end = datetime.datetime.now()
    delta = end - start
    print(f"mongoimport done in {delta}", flush=True)
    print(f"checking indexes on collection {collection_name}", flush=True)
    mycol = mydb[collection_name]
    mycol.create_index('doi')
    mycol.create_index('year')
    mycol.create_index('is_oa')
    mycol.create_index('publisher')
    print(f"deleting {output_json}", flush=True)
    delete_file = f"rm -rf {output_json}"
    os.system(delete_file)
    print(f"deleting {f}", flush=True)
    delete_file = f"rm -rf {f}"
    os.system(delete_file)


def download_snapshot(asof: str = None, upload_to_object_storage: bool = True) -> str:
    try:
        url_old = f'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_{asof}.jsonl.gz'
        return download_file(url_old, upload_to_object_storage)
    except:
        return download_file(url_snapshot, upload_to_object_storage)


def download_daily(dt: str) -> str:
    daily_files = requests.get(f'https://api.unpaywall.org/feed/changefiles?api_key={UPW_API_KEY}&interval=day')\
        .json()['list']
    daily_url = [e for e in daily_files if e.get('date') == dt and e.get('filetype') == 'jsonl'][0]['url']
    return download_file(daily_url)
