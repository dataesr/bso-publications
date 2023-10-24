import pandas as pd
import os
import requests
import pymongo
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object
from bso.server.main.bso_utils import get_ror_from_local
from bso.server.main.utils import to_jsonl
from bso.server.main.extract_transform import load_scanr_publications
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo, load_collection_from_object_storage

logger = get_logger(__name__)

def compute_extra(args):
    #myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    #mydb = myclient['unpaywall']
    #collection_name = 'global'
    #mycol = mydb[collection_name]
    #logger.debug('creating index')
    #mycol.create_index('journal_issn_l')
    #logger.debug('done')
    #myclient.close()

    asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    #filename = download_snapshot(asof).split('/')[-1]
    #logger.debug(f'Filename after download is {filename}')
    filename='unpaywall_snapshot_2023-10-22T083001.jsonl.gz'
    path = f'/upw_data/{filename}'
    jq_oa = f'zcat {path} | '
    jq_oa += "jq -r -c '{journal_issn_l}' | sort -u > /upw_data/issn_l"
    logger.debug(jq_oa)
    os.system(jq_oa)
    #logger.debug(f'Deleting file {path}')
    #os.remove(path)



    #load_scanr_publications('/upw_data/scanr/publications_denormalized.jsonl', 'scanr-publications-20230912')
    #load_scanr_publications('/upw_data/scanr/persons.jsonl', 'scanr-persons-20230912')

def compute_extra2(args):
    to_download = False
    to_compute = False
    to_upload = True
    enriched_output_file = '/upw_data/bso-publications-20230728.jsonl'
    new_file = f'/upw_data/bso-publications-20230728-with-rors.jsonl'
    if to_download:
        download_object(container='bso_dump', filename=f"{enriched_output_file.split('/')[-1]}.gz", out=f'{enriched_output_file}.gz')
    if to_compute:
        locals_data = requests.get('https://raw.githubusercontent.com/dataesr/bso-ui/main/src/config/locals.json').json()
        df = pd.read_json(f'{enriched_output_file}.gz', lines=True, chunksize=50000)
        os.system(f'rm -rf {new_file}')
        ix = 0
        for c in df:
            print(f'reading chunk {ix}', flush=True)
            current_data = c.to_dict(orient='records')
            for d in current_data:
                bso_local_affiliations = d.get('bso_local_affiliations', [])
                current_rors = []
                if isinstance(bso_local_affiliations, list):
                    for aff in bso_local_affiliations:
                        current_ror = get_ror_from_local(aff, locals_data)
                        if current_ror and current_ror not in current_rors:
                            current_rors.append(current_ror)
                d['rors'] = current_rors
            to_jsonl(current_data, new_file)
            ix += 1
        print('gzipping', flush=True)
        os.system(f'gzip {new_file}')
    if to_upload:
        upload_object(container='bso_dump', filename=f'{new_file}.gz')
