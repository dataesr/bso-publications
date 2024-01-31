import pandas as pd
import os
import requests
import pymongo
from bso.server.main.logger import get_logger
from bso.server.main.utils_swift import download_object, upload_object, delete_object, delete_objects, init_cmd
from bso.server.main.bso_utils import get_ror_from_local
from bso.server.main.utils import to_jsonl
from bso.server.main.utils_upw import chunks
from bso.server.main.extract_transform import load_scanr_publications
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo, load_collection_from_object_storage
from bso.server.main.scanr import clean_sudoc_extra, get_person_ids
from bso.server.main.etl import create_split_and_csv_files, collect_splitted_files
from bso.server.main.elastic import refresh_index
import hashlib
import json

logger = get_logger(__name__)

def aggregate_pubmed_data(pubmed_year, min_year = None):
    all_pubmed = []
    all_pubmed_paths = []
    for directory in os.listdir(f'/upw_data/medline/parsed/{pubmed_year}/fr/'):
        for current_file in os.listdir(f'/upw_data/medline/parsed/{pubmed_year}/fr/{directory}'):
            current_file_path = f'/upw_data/medline/parsed/{pubmed_year}/fr/{directory}/{current_file}'
            all_pubmed_paths.append(current_file_path)
    all_pubmed_paths.sort(reverse=True)
    known_pmids = set()
    all_data = []
    for c_ix, current_file_path in enumerate(all_pubmed_paths):
        if c_ix % 100==0:
            logger.debug(f'{c_ix} / {len(all_pubmed_paths)}')
        current_data = pd.read_json(current_file_path).to_dict(orient='records')
        for d in current_data:
            if d.get('pmid') not in known_pmids:
                known_pmids.add(d['pmid'])
                if isinstance(min_year, int) and isinstance(d.get('publication_year'), int) and d['publication_year']>=min_year:
                    all_data.append(d)
                elif min_year is None:
                    all_data.append(d)
    logger.debug(f'{len(all_data)} publications from PubMed')
    output_path = f'/upw_data/medline/aggregated/{pubmed_year}/fr/all'
    if min_year:
        output_path = f'/upw_data/medline/aggregated_recent/{pubmed_year}/fr/all'
    os.system(f'mkdir -p {output_path}')
    chunk_ix=0
    for data_chunked in chunks(all_data, 25000):
        output_pubmed_chunk = f'{output_path}/all_pubmed_{chunk_ix}.json'
        logger.debug(output_pubmed_chunk)
        json.dump(data_chunked, open(output_pubmed_chunk, 'w'))
        chunk_ix += 1


def aggregate_parsed_data(prefix):
    all_parsed_paths = []
    for directory in os.listdir(f'/upw_data/{prefix}_fr'):
        for current_file in os.listdir(f'/upw_data/{prefix}_fr/{directory}'):
            current_file_path = f'/upw_data/{prefix}_fr/{directory}/{current_file}'
            all_parsed_paths.append(current_file_path)
    all_data = []
    for c_ix, current_file_path in enumerate(all_parsed_paths):
        if c_ix % 100==0:
            logger.debug(f'{c_ix} / {len(all_parsed_paths)}')
        current_data = pd.read_json(current_file_path).to_dict(orient='records')
        for d in current_data:
            all_data.append(d)
    print(f'{len(all_data)} publications from {prefix}')
    output_path = f'/upw_data/all_{prefix}_fr/aggregated'
    assert(' ' not in output_path)
    os.system(f'rm -rf {output_path} && mkdir -p {output_path}')
    chunk_ix=0
    for data_chunked in chunks(all_data, 25000):
        output_chunk = f'{output_path}/all_{prefix}_fr_{chunk_ix}.json'
        logger.debug(output_chunk)
        json.dump(data_chunked, open(output_chunk, 'w'))
        chunk_ix += 1

def get_hash(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()


def get_filename(doi):
    init = doi.split('/')[0]
    notice_id = f'doi{doi}'
    id_hash = get_hash(notice_id)
    filename = f'{init}/{id_hash}.json.gz'
    return filename

def tmp(args):
    dois_to_clean = []
    df = pd.read_json('https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest_split_2021_enriched.jsonl.gz',
            chunksize=5000, lines=True, orient='records')
    for c in df:
        publications = c.to_dict(orient='records')
        for p in publications:
            if isinstance(p.get('bso_country'), list) and isinstance(p.get('bso_country_corrected'), list):
                if 'fr' in p['bso_country'] and ('fr' not in p['bso_country_corrected']) and 'html' in p['sources']:
                    for x in p['all_ids']:
                        if x[0:3]=='doi':
                            dois_to_clean.append(x[3:])
    print(len(dois_to_clean))

def compute_extra(args):

    index_name = args.get('index_name')
    output_dir = '/upw_data/bso-split'
    refresh_index(index_name)
    collect_splitted_files(index_name, output_dir )
    #split_idx = args.get('split_idx')
    #last_oa_details = '2023Q4'
    #bso_local_filenames = []
    #for filename in os.listdir(f'/upw_data/bso_local'):
    #    bso_local_filenames.append(filename)

    #create_split_and_csv_files('/upw_data/bso-split', index_name, split_idx, last_oa_details, bso_local_filenames)    

    #cmd0 = f'{init_cmd} delete --prefix parsed sudoc'
    #os.system(cmd0)

    #cmd1 = f'cd /upw_data/sudoc && {init_cmd} upload sudoc json_parsed --skip-identical'
    #os.system(cmd1)


    #index_name = args.get('index')
    #logger.debug(f'reading {index_name}')
    #df = pd.read_json(f'/upw_data/{index_name}.jsonl', lines=True, chunksize=1000)
    #ix = 0
    #for c in df:
    #    publications = c.to_dict(orient='records')
    #    publications = [p for p in publications if 'sudoc' in p['id']]
    #    logger.debug(f'{len(publications)} to clean {ix} for {index_name}')
    #    publications = get_person_ids(publications)
    #    sudocs_to_rm = []
    #    for p in publications:
    #        sudoc_to_rm = clean_sudoc_extra(p)
    #        if sudoc_to_rm:
    #            sudocs_to_rm.append(sudoc_to_rm)
    #            os.system(f'rm -rf /upw_data/sudoc/{sudoc_to_rm}')
    #    #delete_objects('sudoc', sudocs_to_rm)
    #    ix += 1

    #myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    #mydb = myclient['unpaywall']
    #collection_name = 'global'
    #mycol = mydb[collection_name]
    #logger.debug('creating index')
    #mycol.create_index('journal_issn_l')
    #logger.debug('done')
    #myclient.close()

    #asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    #filename = download_snapshot(asof).split('/')[-1]
    #logger.debug(f'Filename after download is {filename}')
    #filename='unpaywall_snapshot_2023-10-22T083001.jsonl.gz'
    #path = f'/upw_data/{filename}'
    #jq_oa = f'zcat {path} | '
    #jq_oa += "jq -r -c '{journal_issn_l}' | sort -u > /upw_data/issn_l"
    #logger.debug(jq_oa)
    #os.system(jq_oa)
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
