import datetime
import json
import os
import pandas as pd
import requests
import gzip
import hashlib
from dateutil import parser

from urllib import parse
from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL, MOUNTED_VOLUME
from bso.server.main.elastic import load_in_es, reset_index, get_doi_not_in_index, update_local_affiliations
from bso.server.main.inventory import update_inventory
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.enrich_parallel import enrich_parallel
from bso.server.main.unpaywall_mongo import get_not_crawled, get_unpaywall_infos
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import download_object, get_objects_by_page, get_objects_by_prefix, upload_object, init_cmd
from bso.server.main.utils_upw import chunks, get_millesime
from bso.server.main.utils import download_file, get_dois_from_input, dump_to_object_storage
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
    for f in ['authors', 'references', 'abstract', 'incipit', 'abbreviations', 'academic_editor', 'accepted_date', 'acknowledgments', 'amonline_date', 'article_type', 'author_version_available', 'citation', 'conference_date', 'conference_location', 'conference_title', 'copyright', 'corrected and typeset_date', 'data_availability', 'databank', 'download_citation', 'editor', 'editorial decision_date', 'first_published_date', 'first_published_online_date', 'footnotes', 'images', 'issn_electronic', 'issn_print', 'modified_date', 'online_date', 'permissions', 'presentation', 'provenance', 'publication_types', 'received_date', 'revised_date', 'revision received_date', 'revision requested_date', 'revisions_received_date', 'submitted_date', 'z_authors']:
        if f in res:
            del res[f]
    if 'affiliations' in res and isinstance(res['affiliations'], list):
        for aff in res['affiliations']:
            if 'name' in aff:
                del aff['name']
    return res

def extract_bso_local(index_name, observations):
    bso_local_dict, bso_local_dict_aff, bso_local_filenames = build_bso_local_dict()

    # first recreates indices for doi and natural ids
    ids_in_index, natural_ids_in_index = set(), set()
    enriched_output_file = f'{MOUNTED_VOLUME}{index_name}.jsonl'
    cmd_jq = f"cat {enriched_output_file} | jq -rc '[.doi,.title_first_author]|@csv' > {MOUNTED_VOLUME}{index_name}_indices.csv"
    os.system(cmd_jq)
    df = pd.read_csv(f'{MOUNTED_VOLUME}{index_name}_indices.csv', header=None, names=['doi', 'title_first_author'], chunksize=100000)
    for c in df:
        ids_in_index.update(set(c['doi'].tolist()))
        natural_ids_in_index.update(set(c['title_first_author'].tolist()))
    logger.debug(f'{len(ids_in_index)} ids_in_index and {len(natural_ids_in_index)} natural_ids_in_index')

    # adding new publications publications, if any
    for filename in bso_local_filenames:
        ids_in_index, natural_ids_in_index = extract_one_bso_local(f'{MOUNTED_VOLUME}{index_name}_extract.jsonl', filename, ids_in_index, natural_ids_in_index, bso_local_dict, True)

    #patching existing 
    for local_aff in bso_local_dict_aff:
        for chunk in chunks(bso_local_dict_aff[local_aff], 500):
            update_local_affiliations(index=index_name,current_dois=chunk, local_affiliations=[local_aff])

    last_oa_details = get_millesime(max(observations))
   
    # dumping index
    es_url_without_http = ES_URL.replace('https://', '').replace('http://', '')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    container = 'bso_dump'
    output_json_file = f'{MOUNTED_VOLUME}{index_name}.jsonl'
    cmd_elasticdump = f'elasticdump --input={es_host}{es_index} --output={output_json_file} ' \
                      f'--type=data --sourceOnly=true --limit 10000'
    logger.debug(cmd_elasticdump)
    os.system(cmd_elasticdump)
    logger.debug('Elasticdump is done')

    #creating csv
    enriched_output_file_csv = json_to_csv(enriched_output_file, last_oa_details)

    # files for bso local
    dump_bso_local(index_name, local_bso_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details)
    
    # upload to OS
    os.system(f'cp {output_json_file} {MOUNTED_VOLUME}bso-publications-latest.jsonl')
    os.system(f'mv {output_json_csv} {MOUNTED_VOLUME}bso-publications-latest.csv')
    zip_upload(enriched_output_file)
    zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.jsonl')
    zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.csv')

def extract_all(index_name, observations, reset_file, extract, transform, load, affiliation_matching, entity_fishing, skip_download, chunksize):
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
    
    ids_in_index, natural_ids_in_index = set(), set()
    bso_local_dict, bso_local_dict_aff, bso_local_filenames = build_bso_local_dict()

    # reset
    if reset_file and extract:
        os.system(f'rm -rf {output_file}')

    # extract
    if extract:
        ids_in_index, natural_ids_in_index = extract_pubmed(output_file+'_pubmed', ids_in_index, natural_ids_in_index, bso_local_dict)
        ids_in_index, natural_ids_in_index = extract_container(output_file+'_parsed_fr', 'parsed_fr', ids_in_index, natural_ids_in_index, bso_local_dict, skip_download, download_prefix=None, filter_fr=False)
        ids_in_index, natural_ids_in_index = extract_container(output_file+'_crossref_fr', 'crossref_fr', ids_in_index, natural_ids_in_index, bso_local_dict, skip_download, download_prefix=None, filter_fr=False)
        if 'scanr' in index_name:
            ids_in_index, natural_ids_in_index = extract_container(output_file+'_theses', 'theses', ids_in_index, natural_ids_in_index, bso_local_dict, False, download_prefix='20211208/parsed', filter_fr=False)
            ids_in_index, natural_ids_in_index = extract_container(output_file+'_hal_fr', 'hal', ids_in_index, natural_ids_in_index, bso_local_dict, False, download_prefix='20211208/parsed', filter_fr=True)
        ids_in_index, natural_ids_in_index = extract_fixed_list(output_file+'_dois_fr', ids_in_index, natural_ids_in_index, bso_local_dict)
        for filename in bso_local_filenames:
            ids_in_index, natural_ids_in_index = extract_one_bso_local(output_file+'_bso_local_'+filename, filename, ids_in_index, natural_ids_in_index, bso_local_dict, False)

        logger.debug('copying pubmed')
        os.system(f'cat {output_file}_pubmed > {output_file}')
        logger.debug('copying parsed_fr')
        os.system(f'cat {output_file}_parsed_fr >> {output_file}')
        logger.debug('copying crossref_fr')
        os.system(f'cat {output_file}_crossref_fr >> {output_file}')
        logger.debug('copying hal_fr')
        os.system(f'cat {output_file}_hal_fr >> {output_file}')
        logger.debug('copying dois_fr')
        os.system(f'cat {output_file}_dois_fr >> {output_file}')
        for filename in bso_local_filenames:
            logger.debug(f'copying {filename}')
            os.system(f'cat {output_file}_bso_local_{filename} | sort | uniq >> {output_file}')

    del ids_in_index
    del natural_ids_in_index
    # enrichment
    # TO do check: 10000=>40 min
    enriched_output_file = output_file.replace('_extract.jsonl', '.jsonl')
    enriched_output_file_full = output_file.replace('_extract.jsonl', '_full.jsonl')
    enriched_output_file_csv = enriched_output_file.replace('.jsonl', '.csv')
    last_oa_details = get_millesime(max(observations))

    if transform:
        df_chunks = pd.read_json(output_file, lines=True, chunksize = chunksize)
        ix = 0
        os.system(f'rm -rf {enriched_output_file}')
        os.system(f'rm -rf {enriched_output_file_full}')
        for c in df_chunks:
            logger.debug(f'chunk {ix}')
            # list and remove the NaN
            publications = [{k:v for k, v in x.items() if v == v } for x in c.to_dict(orient='records')]
            # publis_chunks = list(chunks(publications, 20000))
            enriched_publications = enrich(publications=publications, observations=observations, affiliation_matching=affiliation_matching,
                entity_fishing=entity_fishing, datasource=None, last_observation_date_only=False)
            if 'bso-publications' in index_name:
                enriched_publications = [p for p in enriched_publications if isinstance(p['doi'], str) and p['oa_details']]
                to_jsonl([remove_fields_bso(p) for p in enriched_publications], enriched_output_file, 'a')
            else:
                to_jsonl(enriched_publications, enriched_output_file_full, 'a')
            ix += 1
        
        # csv
        if 'bso-publications' in index_name:
            enriched_output_file_csv = json_to_csv(enriched_output_file, last_oa_details)


    if load:
        # elastic
        es_url_without_http = ES_URL.replace('https://','').replace('http://','')
        es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
        
        logger.debug('loading bso-publications index')
        reset_index(index=index_name)
        elasticimport = f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 10000 " + "--transform='doc._source=Object.assign({},doc)'"
        logger.debug(f'{elasticimport}')
        logger.debug('starting import in elastic')
        os.system(elasticimport)
        
        logger.debug('loading FULL publications index')
        reset_index(index='bso-publications-full')
        elasticimport = f"elasticdump --input={enriched_output_file_full} --output={es_host}bso-publications-full --type=data --limit 1000 " + "--transform='doc._source=Object.assign({},doc)'"
        logger.debug(f'{elasticimport}')
        logger.debug('starting import in elastic')
        os.system(elasticimport)

        dump_bso_local(index_name, bso_local_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details)

        zip_upload(enriched_output_file)
        zip_upload(enriched_output_file_csv)

def dump_bso_local(index_name, local_bso_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details):
    # first remove existing files
    for local_affiliation in local_bso_filenames:
        local_affiliation = local_affiliation.split('.')[0]
        local_filename = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched'
        os.system(f'rm -rf {local_filename}.jsonl')
        os.system(f'rm -rf {local_filename}.csv')
    
    df = pd.read_json(enriched_output_file, lines=True, chunksize=20000)
    ix = 0
    for c in df:
        logger.debug(f'dumping local bso jsonl chunk {ix}')
        publications = [{k:v for k, v in x.items() if v == v } for x in c.to_dict(orient='records')]
        for p in publications:
            for local_affiliation in p.get('bso_local_affiliations', []):
                to_jsonl([p], f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched.jsonl', 'a')
        ix += 1
    
    for local_affiliation in local_bso_filenames:
        local_affiliation = local_affiliation.split('.')[0]
        local_filename_json = f'{MOUNTED_VOLUME}{index_name}_{local_affiliation}_enriched.jsonl'
        local_filename_csv = json_to_csv(local_filename_json, last_oa_details)
        os.system(f'mv {local_filename_json} {MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.jsonl')
        os.system(f'mv {local_filename_csv} {MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.csv')
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.jsonl')
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest_{local_affiliation}_enriched.csv')

def zip_upload(a_file):
    os.system(f'gzip {a_file}')
    upload_object(container='bso_dump', filename=f'{a_file}.gz')
    os.system(f'rm -rf {a_file}.gz')

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            json.dump(entry, outfile)
            outfile.write('\n')

def get_hash(txt):
    return hashlib.md5(text.encode()).hexdigest()

def get_natural_id(res):
    title_first_author = ""
    if res.get('title_first_author'):
        title_first_author = res['title_first_author']
    else:
        if res.get('title'):
            title_first_author += normalize(res.get('title'), 1).strip()
        if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
            if res['authors'][0].get('full_name'):
                title_first_author += ';'+normalize(res['authors'][0].get('full_name'), 1)
    res['title_first_author'] = get_hash(title_first_author)
    return res['title_first_author'] 

def select_missing(new_publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, datasource, load_in_elastic):
    missing = []
    for p in new_publications:
        natural_id = get_natural_id(p)
        unknown_publication = True
        known_ids = []
        has_an_id = False
        for known_id in ['doi', 'nnt_id']: 
            if isinstance(p.get(known_id), str):
                current_id = p[known_id].lower().strip()
                has_an_id = True
                known_ids.append(current_id)
                unknown_publication = unknown_publication and (current_id not in ids_in_index)
        
        if has_an_id:
            pass
        elif has_an_id is False and natural_id not in natural_ids_in_index:
            unknown_publication = True
        else:
            unknown_publication = False

        if unknown_publication:
            missing.append(p)
            ids_in_index.update(known_ids)
            natural_ids_in_index.update([natural_id])
    for p in missing:
        if p.get('doi') and p['doi'] in bso_local_dict:
            p['bso_local_affiliations'] = bso_local_dict[p['doi']]
        p['datasource'] = datasource
    to_jsonl(missing, output_file, 'a')
    if load_in_elastic:
        output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
        loaded = load_in_es(data=missing, index=output_file.replace(MOUNTED_VOLUME, '').replace('_extract.jsonl', ''))
    logger.debug(f'{len(missing)} publications extracted')
    logger.debug(f'ids_in_index: {len(ids_in_index)}, natural_ids_in_index: {len(natural_ids_in_index)}')
    return ids_in_index, natural_ids_in_index

def extract_pubmed(output_file, ids_in_index, natural_ids_in_index, bso_local_dict) -> None:
    os.system(f'rm -rf {output_file}')
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
        ids_in_index, natural_ids_in_index = select_missing(publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, 'pubmed', False)
    return ids_in_index, natural_ids_in_index
   
def extract_container(output_file, container, ids_in_index, natural_ids_in_index, bso_local_dict, skip_download, download_prefix, filter_fr):
    os.system(f'rm -rf {output_file}')
    if skip_download is False:
        cmd =  init_cmd + f' download {container} -D {MOUNTED_VOLUME}/{container} --skip-identical'
        if download_prefix:
            cmd += f" --prefix {download_prefix}"
        os.system(cmd)
    local_path = f'{MOUNTED_VOLUME}/{container}'
    if download_prefix:
        path_prefix = '/'.join(download_prefix.split('/')[0:-1])
        local_path = f'{local_path}/{path_prefix}'
    for prefix in os.listdir(local_path):
        logger.debug(f'prefix {local_path}/{prefix}')
        publications = []
        json_files = os.listdir(f'{local_path}/{prefix}')
        for jsonfilename in json_files:
            with gzip.open(f'{local_path}/{prefix}/{jsonfilename}', 'r') as fin:
                current_publications = json.loads(fin.read().decode('utf-8'))
                for publi in current_publications:
                    if filter_fr:
                        countries = [a.get('detected_countries') for a in publi.get('affiliations', []) if 'detected_countries' in a]
                        countries_flat_list = list(set([item for sublist in countries for item in sublist]))
                        if 'fr' in countries_flat_list:
                            publications.append(publi)
                    else:
                        publications.append(publi)
        ids_in_index, natural_ids_in_index = select_missing(publications, ids_in_index, natural_ids_in_index, output_file, bso_local_dict, container, False)
    return ids_in_index, natural_ids_in_index
   
def extract_fixed_list(output_file, ids_in_index, natural_ids_in_index, bso_local_dict):
    os.system(f'rm -rf {output_file}')
    for extra_file in ['dois_fr', 'tmp_dois_fr']:
        download_object(container='publications-related', filename=f'{extra_file}.json', out=f'{MOUNTED_VOLUME}/{extra_file}.json')
        if os.path.isfile(f'{MOUNTED_VOLUME}/{extra_file}.json'):
            fr_dois = json.load(open(f'{MOUNTED_VOLUME}/{extra_file}.json', 'r'))
            fr_dois_set = set(fr_dois)
            ids_in_index, natural_ids_in_index = select_missing([{'doi': d} for d in fr_dois_set], ids_in_index, natural_ids_in_index, output_file, bso_local_dict, extra_file, False)
    return ids_in_index, natural_ids_in_index

def build_bso_local_dict():
    bso_local_dict = {}
    bso_local_dict_aff = {}
    bso_local_filenames = []
    os.system(f'mkdir -p {MOUNTED_VOLUME}/bso_local')
    cmd =  init_cmd + f' download bso-local -D {MOUNTED_VOLUME}/bso_local --skip-identical'
    os.system(cmd)
    for filename in os.listdir(f'{MOUNTED_VOLUME}/bso_local'):
        bso_local_filenames.append(filename)
        local_affiliations = filename.split('.')[0].split('_')
        current_dois = get_dois_from_input(filename=filename)
        for d in current_dois:
            if d not in bso_local_dict:
                bso_local_dict[d] = []
            for local_affiliation in local_affiliations:
                if local_affiliation not in bso_local_dict[d]:
                    bso_local_dict[d].append(local_affiliation)
                if local_affiliation not in bso_local_dict_aff:
                    bso_local_dict_aff[local_affiliation] = []
                if d not in bso_local_dict_aff[local_affiliation]:
                    bso_local_dict_aff[local_affiliation].append(d)
    return bso_local_dict, bso_local_dict_aff, list(set(bso_local_filenames))

def extract_one_bso_local(output_file, bso_local_filename, ids_in_index, natural_ids_in_index, bso_local_dict, load_in_elastic):
    os.system(f'rm -rf {output_file}')
    local_affiliations = bso_local_filename.split('.')[0].split('_')
    current_dois = get_dois_from_input(filename=bso_local_filename)
    current_dois_set = set(current_dois)
    logger.debug(f'{len(current_dois)} publications in {bso_local_filename}')
    return select_missing([{'doi': d} for d in current_dois_set], ids_in_index, natural_ids_in_index, output_file, bso_local_dict, f'bso_local_{bso_local_filename}', load_in_elastic)

    # alias update is done manually !
    # update_alias(alias=alias, old_index='bso-publications-*', new_index=index)
