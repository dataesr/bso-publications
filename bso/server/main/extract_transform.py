import datetime
import json
import os
import pandas as pd
import requests

from dateutil import parser

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix, upload_object
from bso.server.main.utils_upw import chunks, get_millesime
from bso.server.main.utils import download_file, get_dois_from_input
from bso.server.main.strings import normalize

logger = get_logger(__name__)
    
os.makedirs(MOUNTED_VOLUME, exist_ok=True)

def json_to_csv(json_file, last_oa_details):
    output_csv_file = json_file.replace('.jsonl', '.csv')
    cmd_header = f"echo 'doi,year,title,journal_issns,journal_issn_l,journal_name,publisher,publisher_dissemination," \
                 f"hal_id,pmid,bso_classification,bsso_classification,domains,lang,genre,amount_apc_EUR," \
                 f"detected_countries,bso_local_affiliations,is_oa,journal_is_in_doaj,journal_is_oa,observation_date," \
                 f"oa_host_type,oa_colors,licence_publisher,licence_repositories,repositories' > {output_csv_file}"
    logger.debug(cmd_header)
    os.system(cmd_header)
    cmd_jq = f"cat {json_file} | jq -rc '[.doi,.year,.title,.journal_issns,.journal_issn_l,.journal_name," \
             f".publisher,.publisher_dissemination,.hal_id,.pmid,.bso_classification,((.bsso_classification.field)" \
             f"?|join(\";\"))//null,((.domains)?|join(\";\"))//null,.lang,.genre,.amount_apc_EUR," \
             f"((.detected_countries)?|join(\";\"))//null,((.bso_local_affiliations)?|join(\";\"))//null," \
             f"[.oa_details[]|select(.observation_date==\"{last_oa_details}\")|.is_oa,.journal_is_in_doaj," \
             f".journal_is_oa,.observation_date,([.oa_host_type]|flatten)[0],((.oa_colors)?|join(\";\"))//null," \
             f"((.licence_publisher)?|join(\";\"))//null,((.licence_repositories)?|join(\";\"))//null," \
             f"((.repositories)?|join(\";\"))//null]]|flatten|@csv' >> {output_csv_file}"
    logger.debug(cmd_jq)
    os.system(cmd_jq)
    return output_csv_file

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

def extract_all(index_name, observations, reset_file, extract, affiliation_matching, entity_fishing):
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
    
    ids_in_index, natural_ids_in_index = set(), set()
    bso_local_dict, bso_local_filenames = build_bso_local_dict()

    # reset
    if reset_file and extract:
        os.system(f'rm -rf {output_file}')

    # extract
    if extract:
        ids_in_index, natural_ids_in_index = extract_pubmed(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
        ids_in_index, natural_ids_in_index = extract_container(output_file, 'parsed_fr', ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
        ids_in_index, natural_ids_in_index = extract_container(output_file, 'crossref_fr', ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
        # ids_in_index, natural_ids_in_index = extract_theses(output_file, ids_in_index, natural_ids_in_index, snapshot_date, bso_local_dict)
        # ids_in_index, natural_ids_in_index = extract_hal(output_file, ids_in_index, natural_ids_in_index, snapshot_date, bso_local_dict, 'a')
        ids_in_index, natural_ids_in_index = extract_fixed_list(output_file, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')
        for filename in bso_local_filenames:
            ids_in_index, natural_ids_in_index = extract_one_bso_local(output_file, filename, ids_in_index, natural_ids_in_index, bso_local_dict, 'a')

    # enrichment
    df_chunks = pd.load_json(output_file, lines=True, chunk_size = 10000)
    ix = 0
    enriched_output_file = output_file.replace('_extract.jsonl', '.jsonl')
    os.system(f'rm -rf {enriched_output_file}')
    for c in df_chunks:
        enriched_publications = enrich(publications=c, observations=observations, affiliation_matching=affiliation_matching,
            entity_fishing=entity_fishing,
            last_observation_date_only=False)
        to_jsonl(enriched_publications, enriched_output_file, 'a')
        ix += 1

    # load
    # csv
    last_oa_details = get_millesime(max(observations))
    enriched_output_file_csv = json_to_csv(enriched_output_file, last_oa_details)

    # elastic
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    reset_index(index=index_name)
    elasticimport = f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 10000 " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)

    for local_affiliation in local_bso_filenames:
        local_filename = f' {index_name}_{local_affiliation}_enriched'
        logger.debug(f'bso-local files creation for {local_affiliation}')
        cmd_local_json = f'cat {enriched_output_file} | fgrep {local_affiliation} > {local_filename}.jsonl'
        cmd_local_csv_header = f'head -n 1 {enriched_output_file_csv} > {local_filename}.csv'
        cmd_local_csv = f'cat {enriched_output_file_csv} | fgrep {local_affiliation} >> {local_filename}.csv' 
        os.system(cmd_local_json)
        os.system(cmd_local_csv_header)
        os.system(cmd_local_csv)
        upload_object(container=container, filename=f'{local_filename}.jsonl')
        upload_object(container=container, filename=f'{local_filename}.csv')
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')


    # dump
    os.system(f'gzip {enriched_output_file}')
    os.system(f'gzip {enriched_output_file_csv}')
    upload_object(container='bso_dump', filename=f'{enriched_output_file}.gz')
    upload_object(container='bso_dump', filename=f'{enriched_output_file_csv}.gz')
    os.system(f'rm -rf {enriched_output_file}')
    os.system(f'rm -rf {enriched_output_file_csv}')


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
        if os.path.isfile(f'{MOUNTED_VOLUME}/{extra_file}.json'):
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
    local_affiliations = bso_local_filename.split('.')[0].split('_')
    current_dois = get_dois_from_input(container='bso-local', filename=bso_local_filename)
    current_dois_set = set(current_dois)
    logger.debug(f'{len(current_dois)} publications in {filename}')
    return select_missing([{'doi': d} for d in current_dois_set], ids_in_index, natural_ids_in_index, output_file, bso_local_dict, f'bso_local_{bso_local_filename}', write_mode)

    # alias update is done manually !
    # update_alias(alias=alias, old_index='bso-publications-*', new_index=index)
