import datetime
import json
import os
import pandas as pd
import requests

from dateutil import parser

from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import download_file, get_dois_from_input
from bso.server.main.strings import normalize

logger = get_logger(__name__)
    
os.makedirs(MOUNTED_VOLUME, exist_ok=True)

def remove_fields_bso(res): 
    # not exposing some fields in index
    for f in ['authors', 'references', 'abstract', 'incipit']:
        if f in res:
            del res[f]
    if 'affiliations' in res and isinstance(res['affiliations'], list):
        for aff in res['affiliations']:
            if 'name' in aff:
                del aff['name']
    return res

def extract_all(output_file, observations):
    ids_in_index, natural_ids_in_index = set(), set()
    bso_local_dict, bso_local_filenames = build_bso_local_dict()
    os.system('rm -rf {output_file}')
    ids_in_index, natural_ids_in_index = extract_pubmed(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
    ids_in_index, natural_ids_in_index = extract_container(output_file, 'parsed_fr', ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
    ids_in_index, natural_ids_in_index = extract_container(output_file, 'crossref_fr', ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
    # ids_in_index, natural_ids_in_index = extract_theses(output_file, ids_in_index, natural_ids_in_index, snapshot_date, bso_local_dict)
    # ids_in_index, natural_ids_in_index = extract_hal(output_file, ids_in_index, natural_ids_in_index, snapshot_date, bso_local_dict, 'a')
    ids_in_index, natural_ids_in_index = extract_fixed_list(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
    for filename in bso_local_filenames:
        ids_in_index, natural_ids_in_index = extract_one_bso_local(output_file, filename, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')

    # enrichment
    affiliation_matching = False
    entity_fishing = False
    df_chunks = pd.load_json(output_file, lines=True, chunk_size = 10000)
    ix = 0
    enriched_output_file = output_file.replace('.jsonl', '_enriched.jsonl')
    os.system('rm -rf {enriched_output_file}')
    for c in df_chunks:
        enriched_publications = enrich(publications=c, observations=observations, affiliation_matching=affiliation_matching,
            entity_fishing=entity_fishing,
            last_observation_date_only=False)
        to_jsonl(enriched_publications, enriched_output_file, 'a')
        ix += 1

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            json.dump(entry, outfile)
            outfile.write('\n')

def get_natural_id(res):
    ## title - first author
    title_first_author = ""
    if res.get('title'):
        title_first_author += normalize(res.get('title'), 1).strip()
    if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
        if res['authors'][0].get('full_name'):
            title_first_author += ';'+normalize(res['authors'][0].get('full_name'), 1)
    if title_first_author:
        res['title_first_author'] = title_first_author
    return title_first_author 

def select_missing(new_publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, datasource, write_mode = 'a'):
    missing = []
    for p in new_publications:
        if not isinstance(p.get('title_first_author'), str):
            natural_id = get_natural_id(p)
        else:
            natural_id = p['title_first_author']
        unknown_publication = True
        known_ids = []
        for known_id in ['doi', 'nnt_id']: 
            if isinstance(p.get(known_id), str):
                current_id = p[known_id].lower().strip()
                known_ids.append(current_id)
                unknown_publication = unknown_publication and (current_id not in ids_in_index)
        if known_ids and unknown_publication:
            missing.append(p)
            ids_in_index.update(known_ids)
            natural_ids_in_index.update([natural_id])
        elif natural_id and natural_id not in natural_ids_in_index:
            missing.append(p)
            ids_in_index.update(known_ids)
            natural_ids_in_index.update([natural_id])
    for p in missing:
        if p.get('doi') and p['doi'] in bso_local_dict:
            p['bso_local_affiliations'] = bso_local_dict[p['doi']]
        p['datasource'] = datasource
    to_jsonl(missing, output_file, write_mode)
    logger.debug(f'{len(missing)} publications extracted')
    return ids_in_index, natural_ids_in_index

def extract_pubmed(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, write_mode) -> None:
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
        ids_in_index, natural_ids_in_index = select_missing(publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, 'pubmed', write_mode)
    return ids_in_index, natural_ids_in_index
   
def extract_container(output_file, container, ids_in_index, natural_ids_in_index, bso_local_dict, write_mode):
    for page in range(1, 1000000):
        logger.debug(f'Getting parsed objects for page {page} from object storage ({container})')
        publications = get_objects_by_page(container=container, page=page, full_objects=True, nb_objects=10000)
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        if len(publications) == 0:
            break
        ids_in_index, natural_ids_in_index = select_missing(publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, container, write_mode)
    return ids_in_index, natural_ids_in_index
   
def extract_fixed_list(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, write_mode):
    for extra_file in ['dois_fr', 'tmp_dois_fr']:
        download_object(container='publications-related', filename=f'{extra_file}.json', out=f'{MOUNTED_VOLUME}/{extra_file}.json')
        fr_dois = json.load(open(f'{MOUNTED_VOLUME}/{extra_file}.json', 'r'))
        fr_dois_set = set(fr_dois)
        ids_in_index, natural_ids_in_index = select_missing([{'doi': d} for d in fr_dois_set], ids_in_index, natural_ids_in_index, output_file, bso_local_dict, extra_file, write_mode)
    return ids_in_index, natural_ids_in_index

def extract_hal(output_file, ids_in_index, natural_ids_in_index, snapshot_date, bso_local_dict, write_mode):
    for ix in range(1,10):
        publications = get_objects_by_prefix(container = 'hal', prefix=f'{snapshot_date}/parsed/hal_parsed_all_years_{ix}')
        ids_in_index, natural_ids_in_index = select_missing(publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, f'hal_{snapshot_date}', write_mode)
    return ids_in_index, natural_ids_in_index

def build_bso_local_dict():
    bso_local_dict = {}
    bso_local_filenames = []
    for page in range(1, 1000000):
        filenames = get_objects_by_page(container = 'bso-local', page=page, full_objects=False)
        bso_local_filenames += filenames
        if len(filenames) == 0:
            break
        for filename in filenames:
            local_affiliations = filename.split('.')[0].split('_')
            current_dois = get_dois_from_input(container='bso-local', filename=filename)
            for d in current_dois:
                if d not in bso_local_dict:
                    bso_local_dict[d] = []
                for local_affiliation in local_affiliations:
                    if local_affiliation not in bso_local_dict[d]:
                        bso_local_dict[d].append(local_affiliation)
    return bso_local_dict, list(set(bso_local_filenames))

def extract_one_bso_local(output_file, bso_local_filename, ids_in_index, natural_ids_in_index, bso_local_dict, write_mode):
    local_affiliations = bso_localfilename.split('.')[0].split('_')
    current_dois = get_dois_from_input(container='bso-local', filename=filename)
    current_dois_set = set(current_dois)
    logger.debug(f'{len(current_dois)} publications in {filename}')
    return select_missing([{'doi': d} for d in current_dois_set], ids_in_index, natural_ids_in_index, output_file, bso_local_dict, f'bso_local_{bso_local_filename}', write_mode)

    # alias update is done manually !
    # update_alias(alias=alias, old_index='bso-publications-*', new_index=index)
