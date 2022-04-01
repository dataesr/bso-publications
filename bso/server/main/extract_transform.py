import datetime
import json
import pymongo
import os
import pandas as pd
import requests
import gzip
from dateutil import parser
import multiprocess as mp
from retry import retry

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
from bso.server.main.utils import download_file, get_dois_from_input, dump_to_object_storage, is_valid, clean_doi, get_hash
from bso.server.main.strings import normalize
from bso.server.main.scanr import to_scanr, get_person_ids

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

def remove_extra_fields(res): 
    # not exposing some fields in index
    for f in ['references', 'abstract', 'incipit', 'abbreviations', 'academic_editor', 'accepted_date', 'acknowledgments', 'amonline_date', 'article_type', 'author_version_available', 'citation', 'conference_date', 'conference_location', 'conference_title', 'copyright', 'corrected and typeset_date', 'data_availability', 'databank', 'download_citation', 'editor', 'editorial decision_date', 'first_published_date', 'first_published_online_date', 'footnotes', 'images', 'issn_electronic', 'issn_print', 'modified_date', 'online_date', 'permissions', 'presentation', 'provenance', 'publication_types', 'received_date', 'revised_date', 'revision received_date', 'revision requested_date', 'revisions_received_date', 'submitted_date', 'z_authors', 'title_first_author', 'title_first_author_raw']:
        if f in res:
            del res[f]
    return res

def remove_fields_bso(res): 
    # not exposing some fields in index
    for f in list(res):
        if 'authors' in f:
            del res[f]
        if 'affiliations_' in f and (f not in ['bso_local_affiliations', 'french_affiliations_types']) :
            del res[f]
        if f == 'affiliations' and isinstance(res['affiliations'], list):
            for aff in res['affiliations']:
                for k in ['name', 'datasource']:
                    if k in aff:
                        del aff[k]
    return remove_extra_fields(res)

def transform_publications(publications, index_name, observations, affiliation_matching, entity_fishing, enriched_output_file, write_mode):
    # list and remove the NaN
    publications = [{k:v for k, v in x.items() if v == v and k not in ['_id'] } for x in publications]
    # publis_chunks = list(chunks(publications, 20000))
    enriched_publications = enrich(publications=publications, observations=observations, affiliation_matching=affiliation_matching,
        entity_fishing=entity_fishing, datasource=None, last_observation_date_only=False, index_name=index_name)
    if 'bso-publications' in index_name:
        enriched_publications = [p for p in enriched_publications if isinstance(p['doi'], str) and p['oa_details']]
        to_jsonl([remove_fields_bso(p) for p in enriched_publications], enriched_output_file, write_mode)
    elif 'scanr' in index_name:
        to_jsonl(enriched_publications, enriched_output_file, write_mode)
    else: # study APC
        enriched_publications = [p for p in enriched_publications if isinstance(p['doi'], str) and p['oa_details']]
        to_jsonl([remove_extra_fields(p) for p in enriched_publications], enriched_output_file, write_mode)

def extract_all(index_name, observations, reset_file, extract, transform, load, affiliation_matching, entity_fishing, skip_download, chunksize):
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    output_file = f'{MOUNTED_VOLUME}{index_name}_extract.jsonl'
    
    bso_local_dict, bso_local_dict_aff, bso_local_filenames = build_bso_local_dict()

    # extract
    if extract:
        drop_collection('scanr', 'publications_before_enrichment')

        extract_pubmed(bso_local_dict)
        #extract_container('medline', bso_local_dict, False, download_prefix='parsed/fr', one_by_one=True, filter_fr=False)

        extract_container('parsed_fr', bso_local_dict, skip_download, download_prefix=None, one_by_one=False, filter_fr=False)
        extract_container('crossref_fr', bso_local_dict, skip_download, download_prefix=None, one_by_one=False, filter_fr=False)
        if 'scanr' in index_name:
            extract_container('theses', bso_local_dict, False, download_prefix='20211208/parsed', one_by_one=True, filter_fr=False)
            extract_container('hal',    bso_local_dict, False, download_prefix='20220325/parsed', one_by_one=True, filter_fr=True)
            extract_container('sudoc',  bso_local_dict, False, download_prefix=f'parsed', one_by_one=False, filter_fr=False)
        extract_fixed_list(bso_local_dict)
        for filename in bso_local_filenames:
            extract_one_bso_local(filename, bso_local_dict)

        # export to jsonl
        dump_cmd = f'mongoexport --forceTableScan --uri mongodb://mongo:27017/scanr --collection {collection_name} --out {output_file}'
        os.system(dump_cmd)

    # enrichment
    enriched_output_file = output_file.replace('_extract.jsonl', '.jsonl')
    #enriched_output_file_full = output_file.replace('_extract.jsonl', '_full.jsonl')
    enriched_output_file_csv = enriched_output_file.replace('.jsonl', '.csv')
    last_oa_details = get_millesime(max(observations))

    if transform:
        df_chunks = pd.read_json(output_file, lines=True, chunksize = chunksize)
        os.system(f'rm -rf {enriched_output_file}')
        #os.system(f'rm -rf {enriched_output_file_full}')
       
        ix = -1
        for c in df_chunks:
            parallel = False

            publications = c.to_dict(orient='records')
            ix += 1
            
            if not parallel:
                logger.debug(f'chunk {ix}')
                transform_publications(publications, index_name, observations, affiliation_matching, entity_fishing, enriched_output_file, 'a')
    
            else:
                publis_chunks = list(chunks(lst=publications, n=1700))
                jobs = []
                outputs = []
                for jx, c in enumerate(publis_chunks):
                    current_output = f'{enriched_output_file}_{ix}_{jx}'
                    logger.debug(current_output)
                    p = mp.Process(target=transform_publications, args=(c, index_name, observations, affiliation_matching, entity_fishing, current_output, 'w'))
                    outputs.append(current_output)
                    p.start()
                    jobs.append(p)
                for p in jobs:
                    p.join()
                for k, o in enumerate(outputs):
                    logger.debug(f'dumping {o} into {enriched_output_file}')
                    os.system(f'cat {o} >> {enriched_output_file}')
                    os.system(f'rm -rf {o}')
        
        # csv
        if 'bso-publications' in index_name:
            enriched_output_file_csv = json_to_csv(enriched_output_file, last_oa_details)


    if load and 'bso-publications' in index_name:
        # elastic
        es_url_without_http = ES_URL.replace('https://','').replace('http://','')
        es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
        
        logger.debug('loading bso-publications index')
        reset_index(index=index_name)
        elasticimport = f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 5000 " + "--transform='doc._source=Object.assign({},doc)'"
        logger.debug(f'{elasticimport}')
        logger.debug('starting import in elastic')
        os.system(elasticimport)

        dump_bso_local(index_name, bso_local_filenames, enriched_output_file, enriched_output_file_csv, last_oa_details)

        # upload to OS
        os.system(f'cp {enriched_output_file} {MOUNTED_VOLUME}bso-publications-latest.jsonl')
        os.system(f'mv {enriched_output_file_csv} {MOUNTED_VOLUME}bso-publications-latest.csv')
        zip_upload(enriched_output_file)
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.jsonl')
        zip_upload(f'{MOUNTED_VOLUME}bso-publications-latest.csv')

    if 'scanr' in index_name:
        df_chunks = pd.read_json(enriched_output_file, lines=True, chunksize = chunksize)
        scanr_output_file = enriched_output_file.replace('.jsonl', '_export_scanr.json')
        internal_output_file = enriched_output_file.replace('.jsonl', '_export_internal.jsonl')
        os.system(f'rm -rf {scanr_output_file}')
        os.system(f'rm -rf {internal_output_file}')

        drop_collection('scanr', 'publi_meta')

        ix = 0
        for c in df_chunks:
            publications = c.to_dict(orient='records')
            publications = get_person_ids(publications)
            publications_scanr = to_scanr(publications)
            to_json(to_scanr(publications), scanr_output_file, ix)
            relevant_infos = []
            for p in publications_scanr:
                new_elt = {'id': p['id']}
                for f in ['authors', 'domains', 'keywords', 'year']:
                    if p.get(f):
                        new_elt[f] = p[f]
            save_to_mongo_publi(relevant_infos)
            to_jsonl(publications, internal_output_file, 'a')
            ix += 1
            logger.debug(f'scanr extract, {ix}')
        with open(scanr_output_file, 'a') as outfile:
            outfile.write(']')

def drop_collection(db, collection_name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient[db]
    mycoll = mydb[collection_name]
    mycoll.drop()

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
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('id')
    mycol.create_index('authors.id')
    logger.debug(f'Deleting {output_json}')
    os.remove(output_json)

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

@retry(delay=200, tries=3)
def to_mongo(input_list):
    logger.debug(f'importing {len(input_list)} publications')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    output_json = f'{MOUNTED_VOLUME}publications-current.jsonl'
    #pd.DataFrame(input_list).to_json(output_json, lines=True, orient='records')
    to_jsonl(input_list, output_json, 'w')
    collection_name = 'publications_before_enrichment'
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {output_json}' \
                  f' --collection {collection_name}'
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    for f in ['id', 'doi', 'nnt_id', 'hal_id', 'pmid', 'sudoc_id', 'natural_id', 'all_ids']:
        mycol.create_index(f)
    logger.debug(f'Deleting {output_json}')
    os.remove(output_json)

@retry(delay=200, tries=3)
def get_from_mongo(identifier_type, identifiers):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publications_before_enrichment'
    mycoll = mydb[collection_name]
    res = []
    cursor = mycoll.find({ identifier_type : { '$in': identifiers } }, no_cursor_timeout=True).batch_size(40)
    for r in cursor:
        del r['_id']
        res.append(r)
    cursor.close()
    return res

@retry(delay=200, tries=3)
def delete_from_mongo(identifiers):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publications_before_enrichment'
    mycoll = mydb[collection_name]
    logger.debug(f'removing {len(identifiers)} publis for {identifiers[0:10]} ...')
    mycoll.delete_many({ 'id' : { '$in': identifiers } })

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            json.dump(entry, outfile)
            outfile.write('\n')

def to_json(input_list, output_file, ix):
    if ix == 0:
        mode = 'w'
    else:
        mode = 'a'
    with open(output_file, mode) as outfile:
        if ix == 0:
            outfile.write('[')
        for jx, entry in enumerate(input_list):
            if ix + jx != 0:
                outfile.write(',\n')
            json.dump(entry, outfile)


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
    res['title_first_author_raw'] = title_first_author
    res['title_first_author'] = get_hash(title_first_author)
    return res['title_first_author'] 

def get_common_id(p):
    for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id']:
        if isinstance(p.get(f), str):
            return f.replace('_id', '')+p[f]


def merge_publications(current_publi, new_publi):
    change = False
    new_datasource = new_publi['datasource']
    for f in new_publi:
        if 'authors' in f:
            current_publi[f+'_'+new_datasource] = new_publi[f]
            change = True
        if 'affiliations' in f:
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



def update_publications_infos(new_publications, bso_local_dict, datasource):
    existing_publis_all_ids_to_main_id = {}
    existing_publis_dict = {}
    to_add, to_delete = [], []
    ids_to_check = []
    for p in new_publications:
        p['datasource'] = datasource
        p = tag_affiliations(p, datasource)
        p['all_ids'] = []
        if p.get('doi'):
            p['doi'] = clean_doi(p['doi'])
        for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id']:
            if p.get(f):
                if not is_valid(p[f], f):
                    logger.debug(f'invalid {f} detected: {p[f]}')
                    del p[f]
            if p.get(f):
                if not isinstance(p[f], str):
                    p[f] = str(int(p[f]))
                p[f] = p[f].lower().strip()
        natural_id = get_natural_id(p)
        p_id = get_common_id(p)
        if p_id:
            p['id'] = p_id
        else:
            logger.debug(f'No ID for publi {p}')
            continue
        if p.get('pmid'):
            p['pmid'] = str(int(p['pmid']))
        for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id', 'natural_id']:
            if isinstance(p.get(f), str):
                p['all_ids'].append(f.replace('_id', '')+p[f])
            ids_to_check += p['all_ids']
    # on récupère les data des publis déjà en base
    ids_to_check = list(set(ids_to_check))
    existing_publis = get_from_mongo('all_ids', ids_to_check)
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
                existing_publi, change = merge_publications(existing_publi, p)
                if change:
                    to_add.append(existing_publi)
                    to_delete.append(current_id)
                break
        if existing_publi is None:
            to_add.append(p)
    for p in to_add:
        if p.get('doi') and p['doi'] in bso_local_dict:
            p['bso_local_affiliations'] = bso_local_dict[p['doi']]
    if to_delete:
        delete_from_mongo(to_delete)
    to_mongo(to_add)
    nb_add = len(to_add)
    nb_del = len(to_delete)
    nb_new = nb_add - nb_del
    logger.debug(f'new : {nb_new} publis, updating {nb_del} publis')

def extract_pubmed(bso_local_dict) -> None:
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
        update_publications_infos(publications, bso_local_dict, 'pubmed')

def extract_container(container, bso_local_dict, skip_download, download_prefix, one_by_one, filter_fr):
    local_path = download_container(container, skip_download, download_prefix)
    if one_by_one is False:
        for subdir in os.listdir(local_path):
            get_data(f'{local_path}/{subdir}', one_by_one, filter_fr, bso_local_dict, container)
    else:
        get_data(local_path, one_by_one, filter_fr, bso_local_dict, container)

def download_container(container, skip_download, download_prefix):
    if skip_download is False:
        cmd =  init_cmd + f' download {container} -D {MOUNTED_VOLUME}/{container} --skip-identical'
        if download_prefix:
            cmd += f" --prefix {download_prefix}"
        os.system(cmd)
    if download_prefix:
        return f'{MOUNTED_VOLUME}/{container}/{download_prefix}'
    return f'{MOUNTED_VOLUME}/{container}'

def get_data(local_path, batch, filter_fr, bso_local_dict, container):
    logger.debug(f'getting data from {local_path}')
    publications = []
    for jsonfilename in os.listdir(local_path):
        if batch:
            publications = []
        if jsonfilename[-3:] == '.gz':
            with gzip.open(f'{local_path}/{jsonfilename}', 'r') as fin:
                current_publications = json.loads(fin.read().decode('utf-8'))
        else:
            with open(f'{local_path}/{jsonfilename}', 'r') as fin:
                current_publications = json.loads(fin.read())
        if isinstance(current_publications, dict):
            current_publications = [current_publications]
        assert(isinstance(current_publications, list))
        for publi in current_publications:
            if not isinstance(publi, dict):
                logger.debug(f"publi not a dict : {publi}")
                continue
            if filter_fr:
                countries = [a.get('detected_countries') for a in publi.get('affiliations', []) if 'detected_countries' in a]
                countries_flat_list = list(set([item for sublist in countries for item in sublist]))
                if 'fr' in countries_flat_list:
                    publications.append(publi)
            else:
                publications.append(publi)
        if batch:
            logger.debug(f'{len(publications)} publications')
            update_publications_infos(publications, bso_local_dict, container)
    if not batch:
        logger.debug(f'{len(publications)} publications')
        update_publications_infos(publications, bso_local_dict, container)
    return publications

def extract_fixed_list(bso_local_dict):
    for extra_file in ['dois_fr', 'tmp_dois_fr']:
        download_object(container='publications-related', filename=f'{extra_file}.json', out=f'{MOUNTED_VOLUME}/{extra_file}.json')
        if os.path.isfile(f'{MOUNTED_VOLUME}/{extra_file}.json'):
            fr_dois = json.load(open(f'{MOUNTED_VOLUME}/{extra_file}.json', 'r'))
            for chunk in chunks(fr_dois, 10000):
                update_publications_infos([{'doi': d} for d in chunk], bso_local_dict, extra_file)

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

def extract_one_bso_local(bso_local_filename, bso_local_dict):
    local_affiliations = bso_local_filename.split('.')[0].split('_')
    current_dois = get_dois_from_input(filename=bso_local_filename)
    logger.debug(f'{len(current_dois)} publications in {bso_local_filename}')
    for chunk in chunks(current_dois, 10000):
        update_publications_infos([{'doi': d} for d in chunk], bso_local_dict, f'bso_local_{bso_local_filename}')

    # alias update is done manually !
    # update_alias(alias=alias, old_index='bso-publications-*', new_index=index)

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
