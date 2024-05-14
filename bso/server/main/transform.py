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
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos, get_dois_meta
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix, upload_object, init_cmd, clean_container_by_prefix
from bso.server.main.utils_upw import chunks, get_millesime
from bso.server.main.utils import download_file, get_dois_from_input, dump_to_object_storage, is_valid, clean_doi, get_hash, to_json, to_jsonl, FRENCH_ALPHA2, clean_json, get_code_etab_nnt
from bso.server.main.strings import dedup_sort, normalize
from bso.server.main.scanr import to_scanr, to_scanr_patents, fix_patents, get_person_ids
from bso.server.main.funders.funding import normalize_grant
from bso.server.main.scanr import to_light
from bso.server.main.bso_utils import json_to_csv, remove_wrong_match, get_ror_from_local, remove_too_long, dict_to_csv
from bso.server.main.s3 import upload_s3
from bso.server.main.denormalize_affiliations import get_orga_data, get_projects_data

from bso.server.main.extract import extract_one_bso_local, extract_container, extract_orcid, extract_fixed_list, extract_manual 

logger = get_logger(__name__)
    
os.makedirs(MOUNTED_VOLUME, exist_ok=True)
            
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
    # corrections 
    publications = [clean_softcite(p) for p in publications]
    # correct detected countries from previous affiliation-matcher
    publications = [remove_wrong_match(p) for p in publications]
    # publis_chunks = list(chunks(publications, 20000))
    enriched_publications = enrich(publications=publications, observations=observations, affiliation_matching=affiliation_matching,
        entity_fishing=entity_fishing, datasource=None, last_observation_date_only=False, hal_date=hal_date, index_name=index_name)
    if 'bso-publications' in index_name:
        enriched_publications = [remove_fields_bso(p) for p in enriched_publications if p['oa_details']]
    enriched_publications = [remove_too_long_affiliation(p) for p in enriched_publications]
    to_jsonl(enriched_publications, enriched_output_file, write_mode)

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

def save_to_mongo_publi(relevant_infos):
    output_json = f'{MOUNTED_VOLUME}publi-current.jsonl'
    pd.DataFrame(relevant_infos).to_json(output_json, lines=True, orient='records')
    collection_name = 'publi_meta'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    os.remove(output_json)


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


def dump_bso_local(index_name, local_bso_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details):
    assert(FALSE)
    # TODO remove code
    year_min = 2013
    year_max = 2025

    # init (rm files for years)
    current_len_filename={}
    current_len_filename['global'] = 0
    for year in range(year_min, year_max + 1):
        local_filename = f'{MOUNTED_VOLUME}{index_name}_split_{year}_enriched'
        current_len_filename[local_filename] = 0
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')

    # init (rm files for local affiliations)
    for local_affiliation in local_bso_filenames:
        local_affiliation = local_affiliation.split('.')[0]
        local_filename = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched'
        current_len_filename[local_filename] = 0
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
            current_file = 'global'
            dict_to_csv(p, last_oa_details, enriched_output_file_csv, write_header= (current_len_filename[current_file] == 0))
            current_len_filename[current_file] += 1
            for local_affiliation in p.get('bso_local_affiliations', []):
                if local_affiliation.lower() in local_bso_lower:
                    current_file = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched'
                    to_jsonl([p], f'{current_file}.jsonl', 'a')
                    dict_to_csv(p, last_oa_details, f'{current_file}.csv', write_header=(current_len_filename[current_file] == 0))
                    current_len_filename[current_file] += 1
            for year in range(year_min, year_max + 1):
                if p.get('year') == year:
                    current_file = f'{MOUNTED_VOLUME}{index_name}_split_{year}_enriched'
                    to_jsonl([p], f'{current_file}.jsonl', 'a')
                    dict_to_csv(p, last_oa_details, f'{current_file}.csv', write_header=(current_len_filename[current_file] == 0))
                    current_len_filename[current_file] += 1
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
            continue
        input_filtered.append(p)
        known_ids.add(p['id'])
    if len(input_filtered) == 0:
        return
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}{collection_name}.jsonl'
    to_jsonl(input_filtered, output_json, 'w')
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    os.system(mongoimport)
    mycol = mydb[collection_name]
    for f in ['id', 'doi', 'nnt_id', 'hal_id', 'pmid', 'sudoc_id', 'natural_id', 'all_ids']:
        mycol.create_index(f)
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
    return p


def remove_too_long_affiliation(publi):
    if not isinstance(publi.get('affiliations'), list):
        return publi
    for a in publi['affiliations']:
        if isinstance(a.get('name'), str) and len(a['name']) > 2000:
            logger.debug(f"shorten affiliation for {publi['id']} from {len(a['name'])} to 2000")
            a['name'] = a['name'][0:2000]
    return publi


def clean_softcite(publi):
    for d in ['softcite_details', 'datastet_details']:
        if isinstance(publi.get(d), dict) and isinstance(publi[d].get('raw_mentions'), list):
            for r in publi[d]['raw_mentions']:
                for s in ['references', 'url', 'language', 'publisher']:
                    if isinstance(r.get(s), dict):
                        for g in ['boundingBoxes', 'offsetEnd', 'offsetStart', 'refKey']:
                            if g in r[s]:
                                del r[s][g]
                    elif isinstance(r.get(s), list):
                        for e in r[s]:
                            for g in ['boundingBoxes', 'offsetEnd', 'offsetStart', 'refKey']:
                                if g in e:
                                    del e[g]

    return publi
