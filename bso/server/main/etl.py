import datetime
import ast
import gzip
import json
import jsonlines
import multiprocess as mp
import os
import pandas as pd
import pymongo
import pysftp
import requests

import dateutil.parser
from retry import retry

from os.path import exists
from urllib import parse
from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, reset_index_scanr, get_doi_not_in_index, update_local_affiliations, refresh_index
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.enrich_parallel import enrich_parallel
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix, upload_object, init_cmd, clean_container_by_prefix
from bso.server.main.utils_upw import chunks, get_millesime
from bso.server.main.utils import download_file, get_dois_from_input, dump_to_object_storage, is_valid, clean_doi, get_hash, to_json, to_jsonl, FRENCH_ALPHA2, clean_json, get_code_etab_nnt
from bso.server.main.strings import dedup_sort, normalize
from bso.server.main.scanr import to_scanr, to_scanr_patents, fix_patents, get_person_ids
from bso.server.main.funding import normalize_grant
from bso.server.main.scanr import to_light
from bso.server.main.bso_utils import json_to_csv, remove_wrong_match, get_ror_from_local, remove_too_long, dict_to_csv
from bso.server.main.s3 import upload_s3
from bso.server.main.denormalize_affiliations import get_orga_data, get_projects_data

from bso.server.main.extract import extract_one_bso_local, extract_container, extract_orcid, extract_fixed_list, extract_manual, build_bso_local_dict, get_bso_local_filenames 
from bso.server.main.transform import transform_publications

logger = get_logger(__name__)
    
os.makedirs(MOUNTED_VOLUME, exist_ok=True)
            
def get_collection_name(index_name):
    if 'scanr' in index_name:
        collection_name = 'publications_before_enrichment_scanr'
    else:
        collection_name = 'publications_before_enrichment_bso'
    return collection_name

def split_file(input_dir, file_to_split, nb_lines, split_prefix, output_dir, split_suffix):
    os.system(f'cd {input_dir} && split -l {nb_lines} {file_to_split} {split_prefix}')
    os.system(f'mkdir -p {output_dir}')
    os.system(f'rm -rf {output_dir}/{split_prefix}*')
    idx_split = 0
    local_files = os.listdir(input_dir)
    local_files.sort()
    for f in local_files:
        if f.startswith(f"{split_prefix}"):
            os.system(f'mv {input_dir}/{f} {output_dir}/{split_prefix}{idx_split}{split_suffix}')
            idx_split += 1
    logger.debug(f'{input_dir}/{file_to_split} has been splitted into {idx_split} files of {nb_lines} lines from {output_dir}/{split_prefix}0{split_suffix} to {output_dir}/{split_prefix}{idx_split - 1}{split_suffix}')

def etl(args):

    index_name = args.get('index_name')
    split_idx = args.get('split_idx')
    observations = args.get('observations')
    reset_file = args.get('reset_file')
    extract = args.get('extract')
    transform = args.get('transform')
    transform_scanr = args.get('transform_scanr')
    load = args.get('load')
    affiliation_matching = args.get('affiliation_matching')
    entity_fishing = args.get('entity_fishing')
    skip_download = args.get('skip_download')
    chunksize = args.get('chunksize')
    datasources = args.get('datasources')
    hal_date = args.get('hal_date')
    theses_date = args.get('theses_date')
    reload_index_only = args.get('reload_index_only')
    save_to_mongo = args.get('save_to_mongo')
    finalize = args.get('finalize', False)

    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    extract_output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
    scanr_split_prefix = extract_output_file.replace('_extract.jsonl', '_split_').split('/')[-1]
   
    # getting correspondance for bso local metadata (ror in particular)
    locals_data = requests.get('https://raw.githubusercontent.com/dataesr/bso-ui/main/src/config/locals.json').json()
    logger.debug(f'{len(locals_data)} locals data from bso-ui loaded')
    bso_local_filenames = []
    bso_local_dict = {}
    hal_struct_id_dict = {}
    min_year = 2010
    if 'bso-' in index_name:
        min_year = 2013
        output_dir = '/upw_data/bso-split'
        nb_lines_transform = 500000
    if 'scanr' in index_name:
        output_dir = '/upw_data/scanr-split'
        nb_lines_transform = 1000000

    
    # extract
    if extract:
        bso_local_dict, bso_local_dict_aff, bso_local_filenames, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict = build_bso_local_dict()
        collection_name = get_collection_name(index_name)

        drop_collection('scanr', 'publications_before_enrichment')
        drop_collection('scanr', collection_name)
        
        if 'local' in datasources:
            for filename in bso_local_filenames:
                extract_one_bso_local(filename, bso_local_dict, collection_name, locals_data=locals_data)
        if 'bso3' in datasources:
            extract_container('bso3_publications_dump', bso_local_dict, skip_download=False, download_prefix='final_for_bso_2024', one_by_one=True, filter_fr=True, min_year=None, collection_name=collection_name, locals_data=locals_data) #always fr
        #if 'pubmed' in datasources:
        #    extract_pubmed(bso_local_dict, collection_name)
        #medline depends on the year snapshot
        for d in datasources:
            if 'medline/' in d:
                medline_year = d.split('/')[1].strip()
                logger.debug(f'medline year = {medline_year}')
                skip_download_medline = True
                medline_path = f'aggregated_recent/{medline_year}/fr'
                if 'scanr' in index_name:
                    medline_path = f'aggregated/{medline_year}/fr'
                extract_container('medline', bso_local_dict, skip_download_medline, download_prefix=medline_path, one_by_one=True, filter_fr=False, min_year=min_year, collection_name=collection_name, locals_data=locals_data) #always fr
        #if 'medline' in datasources:
        #    extract_container('medline', bso_local_dict, skip_download, download_prefix='parsed/fr', one_by_one=True, filter_fr=False, min_year=min_year, collection_name=collection_name) #always fr
        if 'parsed_fr' in datasources:
            skip_download_parsed = True
            extract_container('all_parsed_fr', bso_local_dict, skip_download_parsed, download_prefix=None, one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'crossref_fr' in datasources:
            skip_download_crossref = True
            extract_container('all_crossref_fr', bso_local_dict, skip_download_crossref, download_prefix=None, one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'orcid' in datasources:
            extract_orcid(bso_local_dict=bso_local_dict, collection_name=collection_name, locals_data=locals_data)
        if 'theses' in datasources:
            extract_container('theses', bso_local_dict, False, download_prefix=f'{theses_date}/parsed', one_by_one=True, filter_fr=False, min_year=None, collection_name=collection_name, nnt_etab_dict=nnt_etab_dict, locals_data=locals_data) #always fr
        if 'hal' in datasources:
            hal_date.sort(reverse=True)
            extract_container('hal', bso_local_dict, False, download_prefix=f'{hal_date[0]}/parsed', one_by_one=True, filter_fr=True, min_year=min_year, collection_name=collection_name, nnt_etab_dict=nnt_etab_dict, hal_struct_id_dict=hal_struct_id_dict, hal_coll_code_dict=hal_coll_code_dict, locals_data=locals_data) # filter_fr add bso_country fr for french publi
        if 'sudoc' in datasources:
            skip_download_sudoc = True
            extract_container('sudoc', bso_local_dict, skip_download_sudoc, download_prefix=f'json_parsed', one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'fixed' in datasources:
            extract_fixed_list(extra_file='dois_fr', bso_local_dict=bso_local_dict, bso_country='fr', collection_name=collection_name, locals_data=locals_data) # always fr
            extract_fixed_list(extra_file='tmp_dois_fr', bso_local_dict=bso_local_dict, bso_country='fr', collection_name=collection_name, locals_data=locals_data)
        if 'manual' in datasources:
            extract_manual(bso_local_dict=bso_local_dict, collection_name=collection_name, locals_data=locals_data)

        # export to jsonl
        dump_cmd = f'mongoexport --forceTableScan --uri mongodb://mongo:27017/scanr --collection {collection_name} --out {extract_output_file}'
        os.system(dump_cmd)
        
        # split file in several smaller files
        split_file(input_dir = '/upw_data', file_to_split = extract_output_file, nb_lines = nb_lines_transform, split_prefix = scanr_split_prefix, output_dir=output_dir, split_suffix = '_extract.jsonl')

        reset_index(index=index_name)
        if 'scanr' in index_name:
            drop_collection('scanr', 'publi_meta')
    
    # enrichment
    before_transform_file = f'{output_dir}/{index_name}_split_{split_idx}_extract.jsonl'
    enriched_output_file = f'{output_dir}/{index_name}_split_{split_idx}.jsonl'
    logger.debug(f'enriched_output_file: {enriched_output_file}')
    enriched_output_file_csv = enriched_output_file.replace('.jsonl', '.csv')
    last_oa_details = ''
    for obs in observations:
        current_millesime = get_millesime(obs)
        if 'Q4' in current_millesime:
            last_oa_details = current_millesime
    logger.debug(f'using {last_oa_details} for oa_detail date in csv export')


    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    if transform:
        if '_0' in enriched_output_file:
            reset_index(index=index_name)
        logger.debug(f'reading {before_transform_file} for transform and saving results into {enriched_output_file}')
        df_chunks = pd.read_json(before_transform_file, lines=True, chunksize = chunksize)
        os.system(f'rm -rf {enriched_output_file}')
 
        ix = -1
        for c in df_chunks:
            ix += 1
            logger.debug(f'chunk {ix}')
            publications = c.to_dict(orient='records')
            transformed_publications = transform_publications(publications, index_name, observations, affiliation_matching, entity_fishing, enriched_output_file, 'a', hal_date)
        
        if 'bso' in index_name:
            assert('scanr' not in index_name)
            elasticimport = f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 100 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
            os.system(elasticimport)
            bso_local_filenames = get_bso_local_filenames()
            create_split_and_csv_files(output_dir, index_name, split_idx, last_oa_details, bso_local_filenames)
    
    if transform_scanr:
        assert('bso' not in index_name)
        assert('scanr' in index_name)
        df_orga = get_orga_data()
        df_project = get_projects_data()
        #scanr_output_file = enriched_output_file.replace('.jsonl', '_export_scanr.json')
        #os.system(f'rm -rf {scanr_output_file}')
        scanr_output_file_denormalized =  f'{output_dir}/{index_name}_split_{split_idx}_export_scanr_denormalized.jsonl'
        os.system(f'rm -rf {scanr_output_file_denormalized}')
        df_chunks = pd.read_json(enriched_output_file, lines=True, chunksize = chunksize)
        for c in df_chunks:
            publications = c.to_dict(orient='records')
            publications = get_person_ids(publications)
            publications_scanr = to_scanr(publications = publications, df_orga=df_orga, df_project=df_project, denormalize = False)
            #to_jsonl(publications_scanr, scanr_output_file) #to del
            #denormalized
            publications_scanr_denormalized = to_scanr(publications = publications, df_orga=df_orga, df_project=df_project, denormalize = True)
            to_jsonl(publications_scanr_denormalized, scanr_output_file_denormalized)
            # elements to be re-used in the person file
            relevant_infos = []
            for p in publications_scanr:
                new_elt = {'id': p['id']}
                for f in ['authors', 'domains', 'keywords', 'year', 'affiliations', 'title', 'source', 'projects']:
                    if p.get(f):
                        new_elt[f] = p[f]
                relevant_infos.append(new_elt)
            save_to_mongo_publi(relevant_infos)

        elasticimport = f"elasticdump --input={scanr_output_file_denormalized} --output={es_host}{index_name} --type=data --limit 100 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
        os.system(elasticimport)

def finalize(args):
    index_name = args.get('index_name')
    if 'bso-' in index_name:
        output_dir = '/upw_data/bso-split'
    if 'scanr' in index_name:
        output_dir = '/upw_data/scanr-split'
    refresh_index(index_name)
    collect_splitted_files(index_name, output_dir)
    if 'scanr' in index_name:
        save_to_mongo_publi_indexes()


def drop_collection(db, collection_name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient[db]
    mycoll = mydb[collection_name]
    mycoll.drop()
    myclient.close()

def save_to_mongo_publi(relevant_infos):
    #myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    #mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}publi-current.jsonl'
    pd.DataFrame(relevant_infos).to_json(output_json, lines=True, orient='records')
    collection_name = 'publi_meta'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    #logger.debug(f'Checking indexes on collection {collection_name}')
    #logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    #myclient.close()

def save_to_mongo_publi_indexes():
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publi_meta'
    logger.debug('indices on publi_meta')
    mycol = mydb[collection_name]
    mycol.create_index('id')
    mycol.create_index('authors.person')
    mycol.create_index('affiliations.id')
    mycol.create_index('projects.id')
    myclient.close()


def create_split_and_csv_files(output_dir, index_name, split_idx, last_oa_details, local_bso_filenames):
    enriched_output_file = f'{output_dir}/{index_name}_split_{split_idx}.jsonl'
    logger.debug(f'create_split_and_csv_files from {enriched_output_file}')
    year_min = 2013
    year_max = 2025

    # init (rm files for years)
    current_len_filename={}
    global_filename = enriched_output_file.replace('.jsonl', '')
    os.system(f'rm -rf {global_filename}.csv')
    current_len_filename[global_filename] = 0
    for year in range(year_min, year_max + 1):
        local_filename = enriched_output_file.replace('.jsonl', f'_SPLITYEAR{year}SPLITYEAR')
        current_len_filename[local_filename] = 0
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')

    # init (rm files for local affiliations) - all files with lower affiliation id
    for local_affiliation in local_bso_filenames:
        local_affiliation = local_affiliation.split('.')[0].lower()
        local_filename = enriched_output_file.replace('.jsonl', f'_SPLITLOCALAFF{local_affiliation}SPLITLOCALAFF')
        current_len_filename[local_filename] = 0
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')
    local_bso_lower = set([k.split('.')[0].lower() for k in local_bso_filenames])
    

    # loop through the whole dataset
    df = pd.read_json(enriched_output_file, lines=True, chunksize=20000)
    ix = 0
    for c in df:
        logger.debug(f'dumping bso {index_name} {split_idx} jsonl chunk {ix}')
        publications = [{k:v for k, v in x.items() if v == v } for x in c.to_dict(orient='records')]
        for p in publications:
            current_file = global_filename
            write_header_global = False
            if ((split_idx == 0) and (current_len_filename[current_file] == 0)):
                write_header_global = True
            dict_to_csv(p, last_oa_details, f'{current_file}.csv', write_header=write_header_global)
            current_len_filename[current_file] += 1
            for local_affiliation in p.get('bso_local_affiliations', []):
                local_affiliation = local_affiliation.lower()
                current_file = enriched_output_file.replace('.jsonl', f'_SPLITLOCALAFF{local_affiliation}SPLITLOCALAFF')
                if current_file in current_len_filename:
                    to_jsonl([p], f'{current_file}.jsonl', 'a')
                    write_header_aff = False
                    if ((split_idx == 0) and (current_len_filename[current_file] == 0)):
                        write_header_aff = True
                    dict_to_csv(p, last_oa_details, f'{current_file}.csv', write_header=write_header_aff)
                    current_len_filename[current_file] += 1
            if isinstance(p.get('year'), int) and year_min <= p['year'] <= year_max:
                year = p['year']
                current_file = enriched_output_file.replace('.jsonl', f'_SPLITYEAR{year}SPLITYEAR')
                to_jsonl([p], f'{current_file}.jsonl', 'a')
                write_header_year = False
                if ((split_idx == 0) and (current_len_filename[current_file] == 0)):
                    write_header_year = True
                dict_to_csv(p, last_oa_details, f'{current_file}.csv', write_header=write_header_year)
                current_len_filename[current_file] += 1
        ix += 1
   
def collect_splitted_files(index_name, output_dir):
    filemap={}
    for f in os.listdir(f'{output_dir}'):
        if index_name not in f:
            continue
        if '_extract.jsonl' in f:
            continue
        current_extension = f.split('.')[-1]
        current_suffix = ''
        if 'SPLITYEAR' in f:
            if 'bso' in index_name:
                custom_year = f.split('SPLITYEAR')[1]
                current_suffix = f'_split_{custom_year}_enriched'
            else:
                continue
        if 'SPLITLOCALAFF' in f:
            if 'bso' in index_name:
                custom_aff = f.split('SPLITLOCALAFF')[1]
                current_suffix = f'_{custom_aff}_enriched'
            else:
                continue
        if 'bso' in index_name:
            target = f'/upw_data/bso-publications-latest{current_suffix}.{current_extension}'
        else:
            target = f'/upw_data/{index_name}{current_suffix}.{current_extension}'
        if target not in filemap:
            filemap[target] = []
        filemap[target].append(f'{output_dir}/{f}')
    if 'bso' in index_name:
        filemap[f'/upw_data/{index_name}.jsonl'] = filemap[f'/upw_data/bso-publications-latest.jsonl']
        filemap[f'/upw_data/{index_name}.csv'] = filemap[f'/upw_data/bso-publications-latest.csv']

    target_ix = 0
    for target in filemap:
        logger.debug(f'concat {filemap[target]} into {target}')
        logger.debug(f'{target_ix} / {len(filemap)}')
        assert(' ' not in target)
        assert('.csv' in target or '.jsonl' in target)
        os.system(f'rm -rf {target}')
        files_to_concat = filemap[target]
        files_to_concat.sort()
        for f in files_to_concat:
            os.system(f'cat {f} >> {target}')
        if 'bso' in index_name:
            zip_upload(target)
        if 'scanr' in index_name and target == f'{output_dir}/{index_name}_split_{split_idx}_export_scanr_denormalized.jsonl':
            os.system(f'mv {target} /upw_data/scanr/publications_denormalized.jsonl && cd /upw_data/scanr/ && rm -rf publications_denormalized.jsonl.gz && gzip -k publications_denormalized.jsonl')
            upload_s3(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/publications_denormalized.jsonl.gz', destination='production/publications_denormalized.jsonl.gz')
        target_ix += 1
        

def zip_upload(a_file, delete=True):
    os.system(f'gzip {a_file}')
    upload_object(container='bso_dump', filename=f'{a_file}.gz')
    if delete:
        os.system(f'rm -rf {a_file}.gz')
