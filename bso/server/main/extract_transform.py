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

from dateutil import parser
from retry import retry

from os.path import exists
from urllib import parse
from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, reset_index_scanr, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.enrich_parallel import enrich_parallel
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos, get_dois_meta
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix, upload_object, init_cmd, clean_container_by_prefix
from bso.server.main.utils_upw import chunks, get_millesime
from bso.server.main.utils import download_file, get_dois_from_input, dump_to_object_storage, is_valid, clean_doi, get_hash, to_json, to_jsonl, FRENCH_ALPHA2, clean_json, get_code_etab_nnt
from bso.server.main.strings import dedup_sort, normalize
from bso.server.main.scanr import to_scanr, to_scanr_patents, fix_patents, get_person_ids
from bso.server.main.funding import normalize_grant
from bso.server.main.scanr import to_light
from bso.server.main.bso_utils import json_to_csv, remove_wrong_match, get_ror_from_local
from bso.server.main.s3 import upload_s3
from bso.server.main.denormalize_affiliations import get_orga_data, get_projects_data

logger = get_logger(__name__)
    
os.makedirs(MOUNTED_VOLUME, exist_ok=True)

def upload_sword(index_name):
    logger.debug('start sword upload')
    os.system('mkdir -p  /upw_data/scanr')
    os.system('mkdir -p  /upw_data/logs')
    try:
        os.system(f'mv /upw_data/{index_name}_export_scanr.json /upw_data/scanr/publications.json')
    except:
        logger.debug(f'erreur dans mv /upw_data/{index_name}_export_scanr.json /upw_data/scanr/publications.json')
    host = os.getenv('SWORD_PREPROD_HOST')
    username = os.getenv('SWORD_PREPROD_USERNAME')
    password = os.getenv('SWORD_PREPROD_PASSWORD')
    port = int(os.getenv('SWORD_PREPROD_PORT'))
    FTP_PATH = 'upload'
    # TOREMOVE if sword OK
    host = os.getenv('SWORD_PROD_HOST')
    username = os.getenv('SWORD_PROD_USERNAME')
    password = os.getenv('SWORD_PROD_PASSWORD')
    port = int(os.getenv('SWORD_PROD_PORT'))
    FTP_PATH = 'upload/preprod'
    # cat publications.json | sed -e "s/,$//" | sed -e "s/^\[//" | sed -e "s/\]$//i"
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts, log='/upw_data/logs/logs.log') as sftp:
        try:
            sftp.chdir(FTP_PATH)  # Test if remote_path exists
        except IOError:
            sftp.mkdir(FTP_PATH)  # Create remote_path
            sftp.chdir(FTP_PATH)
    with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts, log='/upw_data/logs/logs.log') as sftp:
        with sftp.cd(FTP_PATH):             # temporarily chdir to public
            sftp.put('/upw_data/scanr/publications.json')  # upload file to public/ on remote
            sftp.put('/upw_data/scanr/persons.json')  # upload file to public/ on remote
    logger.debug('end sword upload')

            
def remove_extra_fields(res): 
    # Not exposing some fields in index
    for f in ['references', 'abstract', 'incipit', 'abbreviations', 'academic_editor', 'accepted_date', 'acknowledgments', 'amonline_date', 'article_type', 'author_version_available', 'citation', 'conference_date', 'conference_location', 'conference_title', 'copyright', 'corrected and typeset_date', 'data_availability', 'databank', 'download_citation', 'editor', 'editorial decision_date', 'first_published_date', 'first_published_online_date', 'footnotes', 'images', 'issn_electronic', 'issn_print', 'modified_date', 'online_date', 'permissions', 'presentation', 'provenance', 'publication_types', 'received_date', 'revised_date', 'revision received_date', 'revision requested_date', 'revisions_received_date', 'submitted_date', 'z_authors', 'title_first_author', 'title_first_author_raw', 'publication_date', 'publication_year']:
        if f in res:
            del res[f]
    return res


def remove_fields_bso(res): 
    # not exposing some fields in index
    for f in list(res):
        if 'authors_' in f:
            del res[f]
        if f =='authors' and isinstance(res['authors'], list):
            if len(res['authors']) > 50:
                del res[f]
            else:
                for aut in res['authors']:
                    if isinstance(aut, dict):
                        for g in aut:
                            if 'affiliations_' in g:
                                del aut[g]
                            # if g == 'affiliation':
                            #    del aut[g]
        if 'affiliations_' in f and (f not in ['bso_local_affiliations', 'french_affiliations_types']) :
            del res[f]
        #if f == 'affiliations' and isinstance(res['affiliations'], list):
        #    for aff in res['affiliations']:
        #        for k in ['name', 'datasource']:
        #            if k in aff:
        #                del aff[k]
    return remove_extra_fields(res)


def transform_publications(publications, index_name, observations, affiliation_matching, entity_fishing, enriched_output_file, write_mode, hal_date):
    # list and remove the NaN
    publications = [{k:v for k, v in x.items() if v == v and k not in ['_id'] } for x in publications]
    # correct detected countries from previous affiliation-matcher
    publications = [remove_wrong_match(p) for p in publications]
    # publis_chunks = list(chunks(publications, 20000))
    enriched_publications = enrich(publications=publications, observations=observations, affiliation_matching=affiliation_matching,
        entity_fishing=entity_fishing, datasource=None, last_observation_date_only=False, hal_date=hal_date, index_name=index_name)
    if 'bso-publications' in index_name:
        enriched_publications = [p for p in enriched_publications if p['oa_details']]
        to_jsonl([remove_fields_bso(p) for p in enriched_publications], enriched_output_file, write_mode)
    elif 'scanr' in index_name:
        to_jsonl(enriched_publications, enriched_output_file, write_mode)
    else: # study APC
        enriched_publications = [p for p in enriched_publications if isinstance(p['doi'], str) and p['oa_details']]
        to_jsonl([remove_extra_fields(p) for p in enriched_publications], enriched_output_file, write_mode)

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

def extract_all(index_name, observations, reset_file, extract, transform, load, affiliation_matching, entity_fishing, skip_download, chunksize, datasources, hal_date, theses_date, start_chunk, reload_index_only):
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
    scanr_split_prefix = output_file.replace('_extract.jsonl', '_split_').split('/')[-1]
   
    # getting correspondance for bso local metadata (ror in particular)
    locals_data = requests.get('https://raw.githubusercontent.com/dataesr/bso-ui/main/src/config/locals.json').json()
    logger.debug(f'{len(locals_data)} locals data from bso-ui loaded')
    bso_local_filenames = []
    bso_local_dict = {}
    hal_struct_id_dict = {}
    min_year = 2010
    if 'bso-' in index_name:
        min_year = 2013

    # extract
    if extract:
        collection_name = get_collection_name(index_name)

        drop_collection('scanr', 'publications_before_enrichment')
        drop_collection('scanr', collection_name)
        bso_local_dict, bso_local_dict_aff, bso_local_filenames, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict = build_bso_local_dict()
        
        if 'local' in datasources:
            for filename in bso_local_filenames:
                extract_one_bso_local(filename, bso_local_dict, collection_name, locals_data=locals_data)
        if 'bso3' in datasources:
            extract_container('bso3_publications_dump', bso_local_dict, skip_download=False, download_prefix='final_for_bso', one_by_one=True, filter_fr=True, min_year=None, collection_name=collection_name, locals_data=locals_data) #always fr
        #if 'pubmed' in datasources:
        #    extract_pubmed(bso_local_dict, collection_name)
        #medline depends on the year snapshot
        for d in datasources:
            if 'medline/' in d:
                medline_year = d.split('/')[1].strip()
                logger.debug(f'medline year = {medline_year}')
                extract_container('medline', bso_local_dict, skip_download, download_prefix=f'parsed/{medline_year}/fr', one_by_one=True, filter_fr=False, min_year=min_year, collection_name=collection_name, locals_data=locals_data) #always fr
        #if 'medline' in datasources:
        #    extract_container('medline', bso_local_dict, skip_download, download_prefix='parsed/fr', one_by_one=True, filter_fr=False, min_year=min_year, collection_name=collection_name) #always fr
        if 'parsed_fr' in datasources:
            extract_container('parsed_fr', bso_local_dict, skip_download, download_prefix=None, one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'crossref_fr' in datasources:
            extract_container('crossref_fr', bso_local_dict, skip_download, download_prefix=None, one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'orcid' in datasources:
            extract_orcid(bso_local_dict=bso_local_dict, collection_name=collection_name, locals_data=locals_data)
        if 'theses' in datasources:
            extract_container('theses', bso_local_dict, False, download_prefix=f'{theses_date}/parsed', one_by_one=True, filter_fr=False, min_year=None, collection_name=collection_name, nnt_etab_dict=nnt_etab_dict, locals_data=locals_data) #always fr
        if 'hal' in datasources:
            hal_date.sort(reverse=True)
            extract_container('hal', bso_local_dict, False, download_prefix=f'{hal_date[0]}/parsed', one_by_one=True, filter_fr=True, min_year=min_year, collection_name=collection_name, nnt_etab_dict=nnt_etab_dict, hal_struct_id_dict=hal_struct_id_dict, hal_coll_code_dict=hal_coll_code_dict, locals_data=locals_data) # filter_fr add bso_country fr for french publi
        if 'sudoc' in datasources:
            extract_container('sudoc', bso_local_dict, skip_download, download_prefix=f'parsed', one_by_one=False, filter_fr=False, min_year=None, collection_name=collection_name, locals_data=locals_data) # always fr
        if 'fixed' in datasources:
            extract_fixed_list(extra_file='dois_fr', bso_local_dict=bso_local_dict, bso_country='fr', collection_name=collection_name, locals_data=locals_data) # always fr
            extract_fixed_list(extra_file='tmp_dois_fr', bso_local_dict=bso_local_dict, bso_country='fr', collection_name=collection_name, locals_data=locals_data)
        if 'manual' in datasources:
            extract_manual(bso_local_dict=bso_local_dict, collection_name=collection_name, locals_data=locals_data)

        # export to jsonl
        dump_cmd = f'mongoexport --forceTableScan --uri mongodb://mongo:27017/scanr --collection {collection_name} --out {output_file}'
        os.system(dump_cmd)
        
        # split file in several smaller files
        if 'scanr' in index_name:
            split_file(input_dir = '/upw_data', file_to_split = output_file, nb_lines = 1000000, split_prefix = scanr_split_prefix, output_dir='/upw_data/scanr-split', split_suffix = '_extract.jsonl')

    # enrichment
    enriched_output_file = output_file.replace('_extract.jsonl', '.jsonl')
    logger.debug(f'enriched_output_file: {enriched_output_file}')
    #enriched_output_file_full = output_file.replace('_extract.jsonl', '_full.jsonl')
    enriched_output_file_csv = enriched_output_file.replace('.jsonl', '.csv')
    last_oa_details = ''
    for obs in observations:
        current_millesime = get_millesime(obs)
        if 'Q4' in current_millesime:
            last_oa_details = current_millesime
    logger.debug(f'using {last_oa_details} for oa_detail date in csv export')


    if transform:
        df_chunks = pd.read_json(output_file, lines=True, chunksize = chunksize)
        os.system(f'rm -rf {enriched_output_file}')
        #os.system(f'rm -rf {enriched_output_file_full}')
       
        ix = -1
        for c in df_chunks:
            ix += 1
            if ix < start_chunk:
                continue
            logger.debug(f'chunk {ix}')
            parallel = False

            publications = c.to_dict(orient='records')
            
            if not parallel:
                transform_publications(publications, index_name, observations, affiliation_matching, entity_fishing, enriched_output_file, 'a', hal_date)
    
            else:
                publis_chunks = list(chunks(lst=publications, n=1700))
                jobs = []
                outputs = []
                for jx, c in enumerate(publis_chunks):
                    current_output = f'{enriched_output_file}_{ix}_{jx}'
                    logger.debug(current_output)
                    p = mp.Process(target=transform_publications, args=(c, index_name, observations, affiliation_matching, entity_fishing, current_output, 'w', hal_date))
                    outputs.append(current_output)
                    p.start()
                    jobs.append(p)
                for p in jobs:
                    p.join()
                for k, o in enumerate(outputs):
                    logger.debug(f'dumping {o} into {enriched_output_file}')
                    os.system(f'cat {o} >> {enriched_output_file}')
                    os.system(f'rm -rf {o}')
    if load and 'bso-publications' in index_name:
        if not exists(enriched_output_file):
            download_object(container='bso_dump', filename=f"{enriched_output_file.split('/')[-1]}.gz", out=f'{enriched_output_file}.gz')
            os.system(f'gunzip {enriched_output_file}.gz')
        # elastic
        es_url_without_http = ES_URL.replace('https://','').replace('http://','')
        es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
        logger.debug('loading bso-publications index')
        reset_index(index=index_name)
        elasticimport = f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 1000 " + "--transform='doc._source=Object.assign({},doc)'"
        # logger.debug(f'{elasticimport}')
        logger.debug('starting import in elastic')
        os.system(elasticimport)

        if reload_index_only:
            return
        
        # csv
        enriched_output_file_csv = json_to_csv(enriched_output_file, last_oa_details, split_year=False)

        if 'local' in datasources and len(bso_local_filenames) == 0:
            bso_local_dict, bso_local_dict_aff, bso_local_filenames, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict = build_bso_local_dict()
        dump_bso_local(index_name, bso_local_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details)

        # upload to OS
        os.system(f'cp {enriched_output_file} {MOUNTED_VOLUME}bso-publications-latest.jsonl')
        os.system(f'mv {enriched_output_file_csv} {MOUNTED_VOLUME}bso-publications-latest.csv')
        #clean_container_by_prefix('bso_dump', 'bso-publications-split')
        # end split upload
        zip_upload(enriched_output_file, delete = False)
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.jsonl')
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.csv')
    
    if reload_index_only and 'scanr' in index_name:
        load_scanr_publications('/upw_data/scanr/publications_denormalized.jsonl', 'scanr-publications-dev-'+index_name.split('-')[-1])

    if load and ('scanr' in index_name) and (reload_index_only is False):
        df_orga = get_orga_data()
        df_project = get_projects_data()
        df_chunks = pd.read_json(enriched_output_file, lines=True, chunksize = chunksize)
        scanr_output_file = enriched_output_file.replace('.jsonl', '_export_scanr.json')
        scanr_output_file_denormalized = enriched_output_file.replace('.jsonl', '_export_scanr_denormalized.json')
        #internal_output_file = enriched_output_file.replace('.jsonl', '_export_internal.jsonl')
        os.system(f'rm -rf {scanr_output_file}')
        os.system(f'rm -rf {scanr_output_file_denormalized}')
        #os.system(f'rm -rf {internal_output_file}')
        
        ix = 0

        download_object(container='patstat', filename=f'fam_final_json.jsonl', out=f'{MOUNTED_VOLUME}/fam_final_json.jsonl')
        df_patents = pd.read_json(f'{MOUNTED_VOLUME}/fam_final_json.jsonl', lines=True, chunksize=10000)
        for c in df_patents:
            patents = c.to_dict(orient='records')
            patents = fix_patents(patents) 
            #patents_scanr = to_scanr_patents(patents=patents, df_orga=df_orga, denormalize = False)
            to_jsonl(patents, scanr_output_file)
            
            # no patent in new home made file
            #patents_scanr_denormalized = to_scanr_patents(patents=patents, df_orga=df_orga, denormalize = True)
            #to_jsonl(patents_scanr_denormalized, scanr_output_file_denormalized)
            ix += 1
            logger.debug(f'scanr extract patent, {ix}')

        drop_collection('scanr', 'publi_meta')
    
        for c in df_chunks:
            publications = c.to_dict(orient='records')
            publications = get_person_ids(publications)
            publications_scanr = to_scanr(publications = publications, df_orga=df_orga, df_project=df_project, denormalize = False)
            to_jsonl(publications_scanr, scanr_output_file)
            
            #denormalized
            publications_scanr_denormalized = to_scanr(publications = publications, df_orga=df_orga, df_project=df_project, denormalize = True)
            to_jsonl(publications_scanr_denormalized, scanr_output_file_denormalized)
           
            # elements to be re-used in the person file
            relevant_infos = []
            for p in publications_scanr:
                new_elt = {'id': p['id']}
                for f in ['authors', 'domains', 'keywords', 'year', 'affiliations', 'title']:
                    if p.get(f):
                        new_elt[f] = p[f]
                relevant_infos.append(new_elt)
            save_to_mongo_publi(relevant_infos)
            #publications_cleaned = []
            #for elt in publications:
            #    if isinstance(elt.get('classifications'), list):
            #        for d in elt['classifications']:
            #            if 'code' in d:
            #                d['code'] = str(d['code'])
            #    for f in ['is_paratext', 'has_grant', 'has_apc', 'references']:
            #        if f in elt:
            #            del elt[f]
            #    elt = {f: elt[f] for f in elt if elt[f]==elt[f] }
            #    publications_cleaned.append(elt)
            ix += 1
            logger.debug(f'scanr extract publi, {ix}')
        os.system(f'mv {scanr_output_file} /upw_data/scanr/publications.jsonl && cd /upw_data/scanr/ && rm -rf publications.jsonl.gz && gzip -k publications.jsonl')
        upload_s3(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/publications.jsonl.gz', destination='production/publications.jsonl.gz')
        load_scanr_publications(scanr_output_file_denormalized, 'scanr-publications-dev-'+index_name.split('-')[-1])
        os.system(f'mv {scanr_output_file_denormalized} /upw_data/scanr/publications_denormalized.jsonl && cd /upw_data/scanr/ && rm -rf publications_denormalized.jsonl.gz && gzip -k publications_denormalized.jsonl')
        upload_s3(container='scanr-data', source = f'{MOUNTED_VOLUME}scanr/publications_denormalized.jsonl.gz', destination='production/publications_denormalized.jsonl.gz')

def load_scanr_publications(scanr_output_file_denormalized, index_name):
    denormalized_file=scanr_output_file_denormalized
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading scanr-publications index')
    reset_index_scanr(index=index_name)
    elasticimport = f"elasticdump --input={denormalized_file} --output={es_host}{index_name} --type=data --limit 500 " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)

def drop_collection(db, collection_name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient[db]
    mycoll = mydb[collection_name]
    mycoll.drop()
    myclient.close()

def save_to_mongo_publi(relevant_infos):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}publi-current.jsonl'
    pd.DataFrame(relevant_infos).to_json(output_json, lines=True, orient='records')
    collection_name = 'publi_meta'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    #logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('id')
    mycol.create_index('authors.person')
    mycol.create_index('affiliations')
    #logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    myclient.close()

def dump_bso_local(index_name, local_bso_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details):
    year_min = 2013
    year_max = 2025

    # init (rm files for years)

    for year in range(year_min, year_max + 1):
        local_filename = f'{MOUNTED_VOLUME}{index_name}_split_{year}_enriched'
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')

    # init (rm files for local affiliations)
    for local_affiliation in local_bso_filenames:
        local_affiliation = local_affiliation.split('.')[0]
        local_filename = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched'
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')
    local_bso_lower = set([k.split('.')[0].lower() for k in local_bso_filenames])
    

    # loop through the whole dataset
    df = pd.read_json(enriched_output_file, lines=True, chunksize=20000)
    ix = 0
    for c in df:
        logger.debug(f'dumping local bso jsonl chunk {ix}')
        publications = [{k:v for k, v in x.items() if v == v } for x in c.to_dict(orient='records')]
        for p in publications:
            for local_affiliation in p.get('bso_local_affiliations', []):
                if local_affiliation.lower() in local_bso_lower:
                    to_jsonl([p], f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched.jsonl', 'a')
            for year in range(year_min, year_max + 1):
                if p.get('year') == year:
                    to_jsonl([p], f'{MOUNTED_VOLUME}{index_name}_split_{year}_enriched.jsonl', 'a')
        ix += 1
    
    # create csv files for years
    for year in range(year_min, year_max + 1):
        logger.debug(f'csv files creation for {year}')
        local_filename_json = f'{MOUNTED_VOLUME}{index_name}_split_{year}_enriched.jsonl'
        try:
            local_filename_csv = json_to_csv(local_filename_json, last_oa_details)
            os.system(f'mv {local_filename_csv} {MOUNTED_VOLUME}bso-publications-latest_split_{year}_enriched.csv')
            zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_split_{year}_enriched.csv')
            os.system(f'mv {local_filename_json} {MOUNTED_VOLUME}bso-publications-latest_split_{year}_enriched.jsonl')
            zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_split_{year}_enriched.jsonl')
        except:
            logger.debug(f'ERROR in file creation for {local_filename_json}')
    
    # create csv files for affiliations
    for local_affiliation in local_bso_filenames:
        logger.debug(f'csv files creation for {local_affiliation}')
        local_affiliation = local_affiliation.split('.')[0]
        local_filename_json = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched.jsonl'
        try:
            local_filename_csv = json_to_csv(local_filename_json, last_oa_details)
            os.system(f'mv {local_filename_csv} {MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.csv')
            zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.csv')
            os.system(f'mv {local_filename_json} {MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.jsonl')
            zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.jsonl')
        except:
            logger.debug(f'ERROR in file creation for {local_filename_json}')

def zip_upload(a_file, delete=True):
    os.system(f'gzip {a_file}')
    upload_object(container='bso_dump', filename=f'{a_file}.gz')
    if delete:
        os.system(f'rm -rf {a_file}.gz')

@retry(delay=200, tries=3)
def to_mongo(input_list, collection_name):
    input_filtered = []
    known_ids = set([])
    for p in input_list:
        if p.get('id') is None:
            continue
        if p['id'] in known_ids:
            #logger.debug(f"{p['id']} was in duplicate, inserted only once")
            continue
        input_filtered.append(p)
        known_ids.add(p['id'])
    if len(input_filtered) == 0:
        return
    #logger.debug(f'importing {len(input_filtered)} publications')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}{collection_name}.jsonl'
    #pd.DataFrame(input_list).to_json(output_json, lines=True, orient='records')
    to_jsonl(input_filtered, output_json, 'w')
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    #logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    #logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    for f in ['id', 'doi', 'nnt_id', 'hal_id', 'pmid', 'sudoc_id', 'natural_id', 'all_ids']:
        mycol.create_index(f)
    #logger.debug(f'Deleting {output_json}')
    os.remove(output_json)
    myclient.close()

@retry(delay=200, tries=3)
def get_from_mongo(identifier_type, identifiers, collection_name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb[collection_name]
    res = []
    cursor = mycoll.find({ identifier_type : { '$in': identifiers } }, no_cursor_timeout=True).batch_size(40)
    for r in cursor:
        del r['_id']
        res.append(r)
    cursor.close()
    myclient.close()
    return res

@retry(delay=200, tries=3)
def delete_from_mongo(identifiers, collection_name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    mycoll = mydb[collection_name]
    logger.debug(f'removing {len(identifiers)} publis for {identifiers[0:10]} ...')
    mycoll.delete_many({ 'id' : { '$in': identifiers } })
    myclient.close()



def get_natural_id(res):
    title_info = ""
    if isinstance(res.get('title'), str) and len(res['title']) > 3:
        title_info += normalize(res.get('title'), 1).strip()
    else:
        return None
    first_author = ""
    if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
        if isinstance(res['authors'][0].get('first_name'), str) and isinstance(res['authors'][0].get('last_name'), str) :
            first_name_info = normalize(res['authors'][0].get('first_name'))
            last_name_info = normalize(res['authors'][0].get('last_name'), 1)
            if first_name_info and last_name_info:
                first_author =  f';{first_name_info[0]};{last_name_info}'
        if len(first_author)==0 and res['authors'][0].get('full_name') and len(normalize(res['authors'][0].get('full_name'), 1)) > 1:
            first_author = ';'+normalize(res['authors'][0].get('full_name'), 1)
    title_first_author = f'{title_info};{first_author}'
    res['title_first_author_raw'] = title_first_author
    res['title_first_author'] = get_hash(title_first_author)
    if len(title_info)> 10 and len(str(res.get('title')).split(' '))>4 and len(first_author)>3:
        return res['title_first_author']
    return None

def get_common_id(p):
    for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id']:
        if isinstance(p.get(f), str):
            id_type = f.replace('_id', '')
            return {'id': f'{id_type}{p[f]}', 'id_type': id_type}


def merge_publications(current_publi, new_publi, locals_data):
    change = False
    new_datasource = new_publi['datasource']
    # source
    new_sources = new_publi.get('sources', [])
    if not isinstance(new_sources, list):
        new_sources = []
    current_sources = current_publi.get('sources', [])
    if not isinstance(current_sources, list):
        current_sources = []
    for s in new_sources:
        if s not in current_sources:
            current_sources.append(s)
            change = True
    if current_sources:
        current_publi['sources'] = current_sources
    # bso3
    for f in ['has_availability_statement', 'softcite_details', 'datastet_details', 'bso3_downloaded', 'bso3_analyzed_grobid', 'bso3_analyzed_softcite', 'bso3_analyzed_datastet']:
        if f in new_publi:
            current_publi[f] = new_publi[f]
            if ('details' not in f) and (current_publi[f]):
                current_publi[f] = int(current_publi[f])
            change = True
    # hal
    for f in ['hal_collection_code']:
        if f in new_publi:
            existing_list = current_publi.get(f)
            if not isinstance(existing_list, list):
                existing_list = []
            current_publi[f] = list(set(existing_list + new_publi[f]))
            change = True
    # domains
    current_domains = current_publi.get('domains', [])
    for e in new_publi.get('domains', []):
        if e not in current_domains:
            current_domains.append(e)
            change = True
    if current_domains:
        current_publi['domains'] = current_domains
    # external ids
    current_external = current_publi.get('external_ids', [])
    for e in new_publi.get('external_ids', []):
        if e not in current_external:
            current_external.append(e)
            change = True
    if current_external:
        current_publi['external_ids'] = current_external
    # oa_details
    current_oa_details = current_publi.get('oa_details', {})
    new_oa_details = new_publi.get('oa_details', {})
    for obs_date in new_oa_details:
        if obs_date not in current_oa_details:
            current_oa_details[obs_date] = new_oa_details[obs_date]
            change = True
        else:
            if current_oa_details[obs_date]['is_oa'] is False and new_oa_details[obs_date]['is_oa'] is True:
                current_oa_details[obs_date] = new_oa_details[obs_date]
                change = True
            elif current_oa_details[obs_date]['is_oa'] is True and new_oa_details[obs_date]['is_oa'] is True:
                current_oa_details[obs_date]['repositories'] += new_oa_details[obs_date]['repositories']
                current_oa_details[obs_date]['repositories'] = dedup_sort(current_oa_details[obs_date]['repositories'])
                current_oa_details[obs_date]['oa_locations'] += new_oa_details[obs_date]['oa_locations']
                change = True
    # abstract, keywords, classifications
    # hal_classif to use for bso_classif
    for field in ['abstract', 'keywords', 'classifications', 'acknowledgments', 'references', 'hal_classification']:
    #for field in ['abstract', 'keywords', 'classifications', 'acknowledgments', 'references']:
        current_field = current_publi.get(field, [])
        if not isinstance(current_field, list):
            current_field = []
        new_field = new_publi.get(field, [])
        if not isinstance(new_field, list):
            new_field = []
        for k in new_field:
            if k not in current_field:
                current_field.append(k)
                change = True
        if current_field:
            current_publi[field] = current_field
    # merge grants
    if 'grants' in current_publi and not isinstance(current_publi['grants'], list):
        del current_publi['grants']
    if 'grants' in new_publi and not isinstance(new_publi['grants'], list):
        del new_publi['grants']
    grants = new_publi.get('grants', [])
    if isinstance(grants, list) and grants:
        for grant in new_publi['grants']:
            if 'grants' not in current_publi:
                current_publi['grants'] = []
            if grant not in current_publi['grants']:
                #logger.debug(f"merging grant {grant} into {current_publi['id']}")
                current_publi['grants'].append(grant)
                current_publi['has_grant'] = True
                change = True
    # merge bso country
    assert(isinstance(current_publi['bso_country'], list))
    assert(isinstance(new_publi.get('bso_country', []), list))
    for bso_country in new_publi.get('bso_country', []):
        if bso_country not in current_publi['bso_country']:
            current_publi['bso_country'].append(bso_country)
            change = True
    # bso local affiliations
    current_bso_local_aff = current_publi.get('bso_local_affiliations', [])
    current_local_rors = current_publi.get('rors', [])
    for aff in new_publi.get('bso_local_affiliations', []):
        if aff not in current_bso_local_aff:
            current_bso_local_aff.append(aff)
        current_ror = get_ror_from_local(aff, locals_data)
        if current_ror and current_ror not in current_local_rors:
            current_local_rors.append(current_ror)
    if current_bso_local_aff:
        current_publi['bso_local_affiliations'] = current_bso_local_aff
        change = True
    if current_local_rors:
        current_publi['rors'] = current_local_rors
        change = True

    # merge authors, affiliations and ids
    for f in new_publi:
        if 'authors' in f:
            current_publi[f+'_'+new_datasource] = new_publi[f]
            change = True
        if 'affiliations' in f and f != 'bso_local_affiliations':
            current_publi[f+'_'+new_datasource] = new_publi[f]
            change = True
        if f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id'] and f not in current_publi:
            current_publi[f] = new_publi[f]
            change = True
        for f in new_publi['all_ids']:
            if f not in current_publi['all_ids']:
                current_publi['all_ids'].append(f)
                change = True
    return current_publi, change


def tag_affiliations(p, datasource):
    affiliations = p.get('affiliations')
    if isinstance(affiliations, list):
        for aff in affiliations:
            if 'name_in_document' in aff:
                aff['name'] = aff['name_in_document']
            aff['datasource'] = datasource
    authors = p.get('authors')
    if isinstance(authors, list):
        for aut in authors:
            aut['datasource'] = datasource
            affiliations = aut.get('affiliations')
            if isinstance(affiliations, list):
                for aff in affiliations:
                    if 'name_in_document' in aff:
                        aff['name'] = aff['name_in_document']
                #aff['datasource'] = datasource
    return p



def update_publications_infos(new_publications, bso_local_dict, datasource, collection_name, locals_data):
    existing_publis_all_ids_to_main_id = {}
    existing_publis_dict = {}
    to_add, to_delete = [], []
    ids_to_check = []
    dois_to_enrich_metadata = [p['doi'] for p in new_publications if is_valid(p.get('doi'), 'doi') and ('title' not in p or 'authors' not in p)]
    missing_metadata = get_dois_meta(dois_to_enrich_metadata)
    for p in new_publications:
        p['datasource'] = datasource
        if p.get('doi') in missing_metadata:
                # logger.debug(f"getting metadata from crossref for doi {p['doi']}")
                p.update(missing_metadata[p['doi']])
        p = tag_affiliations(p, datasource)
        p['all_ids'] = []
        if p.get('doi'):
            p['doi'] = clean_doi(p['doi'])
            if p['doi'] is None:
                del p['doi']
        for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id']:
            if f in p:
                if not is_valid(p[f], f):
                    logger.debug(f'invalid {f} detected: {p[f]}')
                    del p[f]
            if p.get(f):
                if not isinstance(p[f], str):
                    p[f] = str(int(p[f]))
                p[f] = p[f].lower().strip()
        natural_id = get_natural_id(p)
        p['natural_id'] = natural_id
        p_id = get_common_id(p)
        if p_id:
            p['id'] = p_id['id']
            p['id_type'] = p_id['id_type']
        else:
            logger.debug(f'No ID for publi {p}')
            continue
        if p.get('pmid'):
            p['pmid'] = str(int(p['pmid']))
        for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id', 'natural_id']:
            if isinstance(p.get(f), str):
                p['all_ids'].append(f.replace('_id', '')+p[f])
            ids_to_check += p['all_ids']
        if isinstance(p.get('grants'), list):
            new_grants = []
            for g in p['grants']:
                new_grant = normalize_grant(g)
                if new_grant:
                    g.update(new_grant)
                    new_grants.append(g)
            p['grants'] = new_grants
        existing_affiliations = p.get('affiliations', [])
        for f in p:
            if 'authors' in f and isinstance(p[f], list):
                for aut in p[f]:
                    if 'affiliation' in aut:
                        new_affiliations = aut['affiliation']
                        for new_affiliation in new_affiliations:
                            if new_affiliation not in existing_affiliations:
                                existing_affiliations.append(new_affiliation)
                        aut['affiliations'] = new_affiliations
                        del aut['affiliation']
        if existing_affiliations:
            p['affiliations'] = existing_affiliations
    # on récupère les data des publis déjà en base
    ids_to_check = list(set(ids_to_check))
    existing_publis = get_from_mongo('all_ids', ids_to_check, collection_name)
    for p in existing_publis:
        for identifier in p.get('all_ids'):
            existing_publis_all_ids_to_main_id[identifier] = p['id']
            existing_publis_dict[p['id']] = p
    for p in new_publications:
        # on cherche si la publication est déjà en base pour lui ajouter des infos complémentaires
        existing_publi = None
        for f in p['all_ids']:
            if f in existing_publis_all_ids_to_main_id:
                current_id = existing_publis_all_ids_to_main_id[f]
                existing_publi = existing_publis_dict[current_id]
                existing_publi, change = merge_publications(existing_publi, p, locals_data)
                if change:
                    to_add.append(existing_publi)
                    to_delete.append(current_id)
                break
        if existing_publi is None:
            to_add.append(p)
    for p in to_add:
        if p.get('id') is None:
            continue
        current_id = p['id']
        for f in ['doi', 'nnt_id', 'hal_id']:
            f_short = f.replace('_id', '')
            if p.get(f) and is_valid(p[f], f):
                if p['id_type'] == f_short:
                    break
                elif p['id_type'] != f_short:
                    p['id'] = f"{f_short}{p[f]}"
                    p['id_type'] = f_short
                    if current_id not in to_delete:
                        to_delete.append(current_id)
                    #logger.debug(f'replacing {current_id} with {f_short}{p[f]}')
                    break
        for publi_id in p.get('all_ids', []):
            if publi_id and publi_id in bso_local_dict:
                if 'bso_local_affiliations' not in p:
                    p['bso_local_affiliations'] = []
                for e in bso_local_dict[publi_id]['affiliations']:
                    if e not in p['bso_local_affiliations']:
                        p['bso_local_affiliations'].append(e)
            

                if 'bso_country' not in p:
                    p['bso_country'] = []
                for e in bso_local_dict[publi_id]['bso_country']:
                    if e not in p['bso_country']:
                        p['bso_country'].append(e)
            
                if 'grants' in p and not isinstance(p['grants'], list):
                    del p['grants']
                current_grants = p.get('grants', [])
                for grant in bso_local_dict[publi_id].get('grants', []):
                    if grant not in current_grants:
                        current_grants.append(grant)
                if current_grants:
                    p['grants'] = current_grants
        extract_light = True
        if extract_light:
            p = to_light(p)
        current_local_rors = []
        for aff in p.get('bso_local_affiliations', []):
            current_ror = get_ror_from_local(aff, locals_data)
            if current_ror and current_ror not in current_local_rors:
                current_local_rors.append(current_ror)
        p['rors'] = current_local_rors
    if to_delete:
        delete_from_mongo(to_delete, collection_name)
    to_mongo(to_add, collection_name)
    nb_add = len(to_add)
    nb_del = len(to_delete)
    nb_new = nb_add - nb_del
    logger.debug(f'new : {nb_new} publis, updating {nb_del} publis')


def extract_pubmed(bso_local_dict, collection_name, locals_data) -> None:
    start_string = '2013-01-01'
    end_string = datetime.date.today().isoformat()
    start_date = parser.parse(start_string).date()
    end_date = parser.parse(end_string).date()
    nb_days = (end_date - start_date).days
    prefix_format = '%Y'
    prefixes = list(set([(start_date + datetime.timedelta(days=days)).strftime(prefix_format)
                             for days in range(nb_days)]))
    prefixes.sort()
    for prefix in prefixes:
        logger.debug(f'Getting parsed objects for {prefix} from object storage (pubmed)')
        publications = get_objects_by_prefix(container='pubmed', prefix=f'parsed/fr/{prefix}')
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        update_publications_infos(publications, bso_local_dict, 'pubmed', collection_name, locals_data)

# one_by_one True if no subdirectory
def extract_container(container, bso_local_dict, skip_download, download_prefix, one_by_one, filter_fr, min_year, collection_name, hal_struct_id_dict={}, hal_coll_code_dict={}, nnt_etab_dict={}, locals_data={}):
    local_path = download_container(container, skip_download, download_prefix)
    if one_by_one is False:
        for subdir in os.listdir(local_path):
            get_data(f'{local_path}/{subdir}', one_by_one, filter_fr, bso_local_dict, container, min_year, collection_name, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict, locals_data)
    else:
        get_data(local_path, one_by_one, filter_fr, bso_local_dict, container, min_year, collection_name, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict, locals_data)

def download_container(container, skip_download, download_prefix):
    if skip_download is False:
        cmd =  init_cmd + f' download {container} -D {MOUNTED_VOLUME}/{container} --skip-identical'
        if download_prefix:
            cmd += f" --prefix {download_prefix}"
        os.system(cmd)
    if download_prefix:
        return f'{MOUNTED_VOLUME}/{container}/{download_prefix}'
    return f'{MOUNTED_VOLUME}/{container}'

def get_data(local_path, batch, filter_fr, bso_local_dict, container, min_year, collection_name, hal_struct_id_dict={}, hal_coll_code_dict={}, nnt_etab_dict={}, locals_data={}):
    logger.debug(f'getting data from {local_path}')
    publications = []
    for root, dirs, files in os.walk(local_path, topdown=False):
        for name in files:
            jsonfilename = os.path.join(root, name)
            if batch:
                publications = []
                logger.debug(f'inserting data from file {jsonfilename}')
            if jsonfilename[-3:] == '.gz':
                with gzip.open(f'{jsonfilename}', 'r') as fin:
                    current_publications = json.loads(fin.read().decode('utf-8'))
            elif 'jsonl' in jsonfilename:
                current_publications = []
                with jsonlines.open(jsonfilename, 'r') as fin:
                    for publi in fin:
                        current_publications.append(publi)
            else:
                with open(f'{jsonfilename}', 'r') as fin:
                    current_publications = json.loads(fin.read())
            if isinstance(current_publications, dict):
                current_publications = [current_publications]
            assert(isinstance(current_publications, list))
            for publi in current_publications:
                if not isinstance(publi, dict):
                    logger.debug(f"publi not a dict : {publi}")
                    continue
                publi_id = None
                for k in ['id', 'doi', 'uid']:
                    if k in publi and publi[k]:
                        publi_id = publi[k]
                        break
                current_fields = list(publi.keys())
                for f in current_fields:
                    if len(str(publi[f])) > 100000:
                        logger.debug(f"deleting field {f} in publi {publi_id} from {jsonfilename} as too long !") 
                        del publi[f]
                # code etab NNT
                nnt_id = publi.get('nnt_id')
                if isinstance(nnt_id, str) and get_code_etab_nnt(nnt_id, nnt_etab_dict) in nnt_etab_dict:
                    # if nnt_id, make sure nnt_etab_dict if filled
                    assert('emal' in nnt_etab_dict)
                    current_local = publi.get('bso_local_affiliations', [])
                    new_local = nnt_etab_dict[get_code_etab_nnt(nnt_id, nnt_etab_dict)]
                    if new_local not in current_local:
                        current_local.append(new_local)
                        publi['bso_local_affiliations'] = current_local
                # code collection HAL
                if isinstance(publi.get('hal_collection_code'), list):
                    for coll_code in publi.get('hal_collection_code'):
                        current_local = publi.get('bso_local_affiliations', [])
                        # adding coll code into bso_local_affiliation
                        coll_code_lower = coll_code.lower()
                        current_local.append(coll_code_lower)
                        publi['bso_local_affiliations'] = list(set(current_local))
                        if coll_code_lower in hal_coll_code_dict:
                            new_local = hal_coll_code_dict[coll_code_lower]
                            if new_local not in current_local:
                                current_local.append(new_local)
                                publi['bso_local_affiliations'] = list(set(current_local))
                # code structId HAL
                affiliations = publi.get('affiliations')
                if isinstance(affiliations, list):
                    for aff in affiliations:
                        if isinstance(aff.get('name'), str):
                            if aff['name'].lower() == 'access provided by':
                                aff['name']='' # some publications are wrongly detected fr and parsed affiliation is 'Access provided by' ...
                        current_local = publi.get('bso_local_affiliations', [])
                        if aff.get('hal_docid'):
                            # adding hal_struct_id into bso_local_affiliation
                            current_local.append(str(int(float(aff.get('hal_docid')))))
                            publi['bso_local_affiliations'] = list(set(current_local))
                            if aff['hal_docid'] in hal_struct_id_dict:
                                new_local = hal_struct_id_dict[aff['hal_docid']]
                                if new_local not in current_local:
                                    current_local.append(new_local)
                                    publi['bso_local_affiliations'] = list(set(current_local))


                if filter_fr:
                    # si filter_fr, on ajoute bso_country fr seulement pour les fr
                    is_fr = False
                    countries = []
                    if isinstance(publi.get('affiliations'), list):
                        for a in publi.get('affiliations', []):
                            if isinstance(a, dict) and 'detected_countries' in a:
                                countries.append(a['detected_countries'])
                    countries_flat_list = list(set([item for sublist in countries for item in sublist]))
                    for ctry in countries_flat_list:
                        if ctry in FRENCH_ALPHA2:
                            is_fr = True
                            break
                    if is_fr:
                        publi['bso_country'] = ['fr']
                    else:
                        publi['bso_country'] = ['other']
                else:
                    # filter_fr == False
                    # sinon, fr par défaut
                    publi['bso_country'] = ['fr']
                
                if min_year and publi.get('genre') not in ['thesis']:
                    year = None
                    for f in ['year', 'publication_year', 'published_year']:
                        try:
                            year = int(publi.get(f))
                        except:
                            pass
                        if year:
                            break
                    if year is None or year < min_year:
                        continue
                    if publi.get('title') is None:
                        logger.debug(f'SKIP as no title for publi {publi}')
                        continue
                publications.append(publi)
            if batch:
                logger.debug(f'{len(publications)} publications')
                for chunk in chunks(publications, 5000):
                    update_publications_infos(chunk, bso_local_dict, container, collection_name, locals_data)
    if not batch:
        logger.debug(f'{len(publications)} publications')
        for chunk in chunks(publications, 5000):
            update_publications_infos(chunk, bso_local_dict, container, collection_name, locals_data)
    return publications

def extract_fixed_list(extra_file, bso_local_dict, bso_country, collection_name, locals_data):
    download_object(container='publications-related', filename=f'{extra_file}.json', out=f'{MOUNTED_VOLUME}/{extra_file}.json')
    if os.path.isfile(f'{MOUNTED_VOLUME}/{extra_file}.json'):
        fr_dois = json.load(open(f'{MOUNTED_VOLUME}/{extra_file}.json', 'r'))
        for chunk in chunks(fr_dois, 10000):
            update_publications_infos([{'doi': d, 'bso_country': [bso_country], 'sources': [extra_file]} for d in chunk], bso_local_dict, extra_file, collection_name, locals_data)

def extract_manual(bso_local_dict, collection_name, locals_data):
    url='https://docs.google.com/spreadsheet/ccc?key=1SuFzHK7OptlIYF8w42WG04WwQaNswb9qBspzW0DjTak&output=csv'
    df_all = pd.read_csv(url, chunksize=10000)
    for df in df_all:
        publications = {}
        for p in df.to_dict(orient='records'):
            e = clean_json(p)
            elt = {'bso_country': ['other'], 'sources': ['manual_input']}
            for f in ['doi', 'hal_id', 'nnt_id', 'sudoc_id']:
                if e.get(f):
                    elt[f] = e[f]
            publi_id = get_common_id(e)['id']
            if publi_id not in publications:
                publications[publi_id] = elt
                publications[publi_id]['authors'] = []
            elt = publications[publi_id]
            current_author = {}
            current_affiliations = []
            global_affiliations = elt.get('affiliations', [{'ids': []}])[0]['ids']
            if 'idref' in e.get('person_id'):
                current_author['idref'] = e['person_id'].replace('idref', '')
            for f in ['last_name', 'first_name', 'full_name']:
                if e.get(f):
                    current_author[f] = e[f]
            for f in ['rnsr', 'siren', 'siret', 'grid', 'ror']:
                if e.get(f):
                    for aff in [a.strip() for a in ast.literal_eval(e[f])]:
                        current_elt_to_add = {'id': aff, 'type': f}
                        current_affiliations.append(current_elt_to_add)
                        if current_elt_to_add not in global_affiliations:
                            global_affiliations.append(current_elt_to_add)
            if current_affiliations:
                current_author['affiliations'] = [{'ids': current_affiliations}]
            elt['authors'].append(current_author)
            elt['affiliations'] = [{'ids': global_affiliations}]
        update_publications_infos(list(publications.values()), bso_local_dict, 'manual_input', collection_name, locals_data)

def extract_orcid(bso_local_dict, collection_name, locals_data):
    df_all = pd.read_json('/upw_data/orcid_idref.jsonl', lines=True, orient='records', chunksize=10000)
    for df in df_all:
        publications = {}
        for p in df.to_dict(orient='records'):
            e = clean_json(p)
            elt = {'bso_country': ['other'], 'sources': ['orcid']}
            if 'doi' in e['publi_id'][0:3]:
                elt['doi'] = e['publi_id'][3:]
            elif 'hal' in e['publi_id'][0:3]:
                elt['hal_id'] = e['publi_id'][3:]
            else:
                continue
            publi_id = e['publi_id']
            if publi_id not in publications:
                publications[publi_id] = elt
                publications[publi_id]['authors'] = []
            elt = publications[publi_id]
            current_author = {}
            if 'idref' in e.get('person_id'):
                current_author['idref'] = e['person_id'].replace('idref', '')
            for f in ['last_name', 'first_name', 'full_name']:
                if e.get(f):
                    current_author[f] = e[f]
            elt['authors'].append(current_author)
        update_publications_infos(list(publications.values()), bso_local_dict, 'orcid', collection_name, locals_data)

def build_bso_local_dict():
    bso_local_dict = {}
    bso_local_dict_aff = {}
    hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict = {}, {}, {}
    bso_local_filenames = []
    os.system(f'mkdir -p {MOUNTED_VOLUME}/bso_local')
    cmd =  init_cmd + f' download bso-local -D {MOUNTED_VOLUME}/bso_local --skip-identical'
    os.system(cmd)
    for filename in os.listdir(f'{MOUNTED_VOLUME}/bso_local'):
        bso_local_filenames.append(filename)
        local_affiliations = '.'.join(filename.split('.')[:-1]).split('_')
        data_from_input = get_dois_from_input(filename=filename)
        current_ids = []
        if 'doi' in data_from_input:
            current_ids += data_from_input['doi']
        for id_type in ['hal_id', 'nnt_id']:
            input_ids = data_from_input.get(id_type, [])
            id_prefix = id_type.replace('_id', '')
            current_ids += [{'id': f'{id_prefix}{v}', id_type: v} for v in input_ids]
        #current_dois = data_from_input['doi']
        for s in data_from_input.get('hal_struct_id', []):
            assert(isinstance(s, str))
            assert('.0' not in s)
            hal_struct_id_dict[s] = local_affiliations[0]
        for s in data_from_input.get('hal_coll_code', []):
            assert(isinstance(s, str))
            assert('.0' not in s)
            hal_coll_code_dict[s] = local_affiliations[0]
        for s in data_from_input.get('nnt_etab', []):
            assert(isinstance(s, str))
            assert('.0' not in s)
            nnt_etab_dict[s] = local_affiliations[0]
        for elt in current_ids:
            elt_id = elt['id']
            if elt_id not in bso_local_dict:
                bso_local_dict[elt_id] = {'affiliations': [], 'grants': [], 'bso_country': []}
            for local_affiliation in local_affiliations:
                if local_affiliation not in bso_local_dict[elt_id]['affiliations']:
                    bso_local_dict[elt_id]['affiliations'].append(local_affiliation)
                if elt.get('grants'):
                    bso_local_dict[elt_id]['grants'] += elt['grants']
                if elt.get('bso_country'):
                    assert(isinstance(elt['bso_country'], list))
                    for bso_country in elt['bso_country']:
                        if bso_country not in bso_local_dict[elt_id]['bso_country']:
                            bso_local_dict[elt_id]['bso_country'].append(bso_country)
                if local_affiliation not in bso_local_dict_aff:
                    bso_local_dict_aff[local_affiliation] = []
                if elt_id not in bso_local_dict_aff[local_affiliation]:
                    bso_local_dict_aff[local_affiliation].append(elt_id)
    return bso_local_dict, bso_local_dict_aff, list(set(bso_local_filenames)), hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict

def extract_one_bso_local(bso_local_filename, bso_local_dict, collection_name, locals_data):
    local_affiliations = bso_local_filename.split('.')[0].split('_')
    current_dois = get_dois_from_input(filename=bso_local_filename)['doi']
    logger.debug(f'{len(current_dois)} publications in {bso_local_filename}')
    for chunk in chunks(current_dois, 10000):
        update_publications_infos(chunk, bso_local_dict, f'bso_local_{bso_local_filename}', collection_name, locals_data)

def tmp_apc_study():
    for y in range(2013, 2022):
        os.system(f'rm -rf /upw_data/study-apc_{y}.jsonl')
    df_all = pd.read_json('/upw_data/study-apc.jsonl', lines=True, chunksize=25000)
    ix = 0
    for df in df_all:
        for y in range(2013, 2022):
            x = df[df.year==y].to_dict(orient='records')
            to_jsonl(x, f'/upw_data/study-apc_{y}.jsonl')
        logger.debug(ix)
        ix += 1
    for y in range(2013, 2022):
        zip_upload(f'/upw_data/study-apc_{y}.jsonl')
