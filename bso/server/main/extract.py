import ast
import datetime
import gzip
import json
import jsonlines
import os
import pandas as pd
import pymongo

import dateutil.parser
from retry import retry

from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_mongo import get_dois_meta
from bso.server.main.utils_swift import download_object, get_objects_by_prefix, init_cmd
from bso.server.main.utils_upw import chunks
from bso.server.main.utils import get_dois_from_input, is_valid, clean_doi, get_hash, to_jsonl, FRENCH_ALPHA2, clean_json, get_code_etab_nnt
from bso.server.main.strings import dedup_sort, normalize
from bso.server.main.funders.funding import normalize_grant
from bso.server.main.scanr import to_light
from bso.server.main.bso_utils import get_ror_from_local, remove_too_long

logger = get_logger(__name__)


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
    # title
    for f in ['title', 'title_first_author_raw', 'title_first_author', 'natural_id']:
        if current_publi.get(f) is None and isinstance(new_publi.get(f), str):
            current_publi[f] = new_publi[f]
    # bso3
    for f in ['has_availability_statement', 'softcite_details', 'datastet_details', 'bso3_downloaded', 'bso3_analyzed_grobid', 'bso3_analyzed_softcite', 'bso3_analyzed_datastet']:
        if f in new_publi:
            current_publi[f] = new_publi[f]
            if ('details' not in f) and (current_publi[f]):
                current_publi[f] = int(current_publi[f])
            change = True
    # hal
    for field in ["hal_collection_code", "has_doi_in_hal", "doi_in_hal"]:
        if field in new_publi:
            field_value = new_publi.get(field)
            if field == "hal_collection_code":
                if not isinstance(field_value, list):
                    field_value = []
                current_value = current_publi.get(field, [])
                if not isinstance(current_value, list):
                    current_value = []
                current_publi[field] = list(set(current_value + field_value))
            else:
                if field == "has_doi_in_hal" and not isinstance(field_value, int):
                    field_value = 0
                if field == "doi_in_hal" and not isinstance(field_value, str):
                    field_value = None
                current_publi[field] = field_value
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
            if current_oa_details[obs_date]["is_oa"] is False and new_oa_details[obs_date]["is_oa"] is True:
                current_oa_details[obs_date] = new_oa_details[obs_date]
                change = True
            elif current_oa_details[obs_date]["is_oa"] is True and new_oa_details[obs_date]["is_oa"] is True:
                if "repositories" not in current_oa_details[obs_date]:
                    current_oa_details[obs_date]["repositories"] = []
                if "repositories" in new_oa_details[obs_date]:
                    current_oa_details[obs_date]["repositories"] += new_oa_details[obs_date]["repositories"]
                    current_oa_details[obs_date]["repositories"] = dedup_sort(current_oa_details[obs_date]["repositories"])
                else:
                    logger.debug(f"no repositories for {new_publi} at {obs_date}")
                if "oa_locations" not in current_oa_details[obs_date]:
                    current_oa_details[obs_date]["oa_locations"] = []
                if "oa_locations" in new_oa_details[obs_date]:
                    current_oa_details[obs_date]["oa_locations"] += new_oa_details[obs_date]["oa_locations"]
                else:
                    logger.debug(f"no oa_locations for {new_publi} at {obs_date}")
                change = True
    # abstract, keywords, classifications
    # hal_classif to use for bso_classif
    for field in ['abstract', 'keywords', 'classifications', 'acknowledgments', 'references', 'hal_classification']:
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
    for f in new_publi.copy():
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
    return p


# TODO handle a "main" doi if crossref / datacite
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
                p.update(missing_metadata[p['doi']])
        p = tag_affiliations(p, datasource)
        p['all_ids'] = []
        if p.get('doi'):
            p['doi'] = clean_doi(p['doi'])
            if p['doi'] is None:
                del p['doi']
        for f in ['doi', 'pmid', 'nnt_id', 'hal_id', 'sudoc_id']:
            if f in p:
                if not isinstance(p[f], str):
                    p[f] = str(p[f])
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
                if isinstance(g, dict):
                    new_grant = normalize_grant(g)
                elif isinstance(g, str):
                    new_grant = normalize_grant({'grantid': g})
                else:
                    logger.debug(f'UNEXPECTED grant {g} not string neither dict')
                if new_grant:
                    for new_g in new_grant:
                        new_g['datasource'] = datasource
                    new_grants += new_grant
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
            if identifier not in existing_publis_all_ids_to_main_id:
                existing_publis_all_ids_to_main_id[identifier] = p['id']
                existing_publis_dict[p['id']] = p
    for p in new_publications:
        # on cherche si la publication est déjà en base pour lui ajouter des infos complémentaires
        existing_publi = None
        #for f in p['all_ids']:
        #    if f in existing_publis_all_ids_to_main_id:
        #        current_id = existing_publis_all_ids_to_main_id[f]
        #        existing_publi = existing_publis_dict[current_id]
        #        existing_publi, change = merge_publications(existing_publi, p, locals_data)
        #        if change:
        #            to_add.append(existing_publi)
        #            to_delete.append(current_id)
        #        break
        existing_publi_after_merge = None
        has_changed = False
        for f in p['all_ids']:
            if f in existing_publis_all_ids_to_main_id:
                current_id = existing_publis_all_ids_to_main_id[f]
                existing_publi = existing_publis_dict[current_id]
                if existing_publi_after_merge is None:
                    existing_publi_after_merge, change = merge_publications(existing_publi, p, locals_data)
                else:
                    existing_publi_after_merge, change = merge_publications(existing_publi, existing_publi_after_merge, locals_data)
                if change:
                    has_changed = True
                    to_delete.append(current_id)
        if has_changed:
            to_add.append(existing_publi_after_merge)
        if existing_publi_after_merge is None:
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
        to_delete = list(set(to_delete))
        delete_from_mongo(to_delete, collection_name)

    # make sure no duplicates in to_add
    to_add_known_ids = set()
    to_add_no_dups = []
    for k in to_add:
        if idk in k.get('all_ids'):
            if idk in to_add_known_ids:
                logger.debug(f'removed a duplicate entry for {idk}')
                continue
            to_add_known_ids.update(k['all_ids'])
            to_add_no_dups.append(k)

    to_mongo(to_add_no_dups, collection_name)
    nb_add = len(to_add_no_dups)
    nb_del = len(to_delete)
    nb_new = nb_add - nb_del
    logger.debug(f'new : {nb_new} publis, updating {nb_del} publis')


def extract_pubmed(bso_local_dict, collection_name, locals_data) -> None:
    start_string = '2013-01-01'
    end_string = datetime.date.today().isoformat()
    start_date = dateutil.parser.parse(start_string).date()
    end_date   = dateutil.parser.parse(end_string).date()
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
                # Create new fields to flag if there is a DOI in HAL, and which one
                if container == "hal":
                    doi_in_hal = publi.get("doi")
                    publi["doi_in_hal"] = doi_in_hal
                    publi["has_doi_in_hal"] = 1 if is_valid(doi_in_hal, "doi") else 0
                publi = remove_too_long(publi, publi_id, jsonfilename)
                # code etab NNT
                nnt_id = publi.get('nnt_id')
                if isinstance(nnt_id, str) and get_code_etab_nnt(nnt_id, nnt_etab_dict) in nnt_etab_dict:
                    # if nnt_id, make sure nnt_etab_dict if filled
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

                if 'year' not in publi and isinstance(publi.get('publication_date'), str):
                    if publi['publication_date'][0:2] in ['19', '20']:
                        publi['year'] = publi['publication_date'][0:4]
                
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
    manual_infos = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vRtJvpjh4ySiniYVzgUYpGQVQEuNY7ZOpqPbi3tcyRfKiBaLnAgYziQgecX_kvwnem3fr0M34hyCTFU/pub?gid=1281340758&single=true&output=csv')
    publications = {}
    for p in manual_infos.to_dict(orient='records'):
        e = clean_json(p)
        elt = {'bso_country': ['other'], 'sources': ['manual_input']}
        e['id'] = e['publi_id']
        if e['id'][0:3] == 'doi':
            elt['doi'] = e['id'][3:]
        if e['id'][0:3] == 'hal':
            elt['hal_id'] = e['id'][3:]
        if e['id'][0:3] == 'nnt':
            elt['nnt_id'] = e['id'][3:]
        if e['id'][0:5] == 'sudoc':
            elt['sudoc_id'] = e['id'][5:]
        publi_id = e['id']
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

def get_bso_local_filenames():
    bso_local_filenames = []
    for filename in os.listdir(f'{MOUNTED_VOLUME}/bso_local'):
        bso_local_filenames.append(filename)
    return  list(set(bso_local_filenames))

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
    bso_local_filenames = list(set(bso_local_filenames))
    bso_local_filenames.sort()
    return bso_local_dict, bso_local_dict_aff, bso_local_filenames, hal_struct_id_dict, hal_coll_code_dict, nnt_etab_dict

def extract_one_bso_local(bso_local_filename, bso_local_dict, collection_name, locals_data):
    local_affiliations = bso_local_filename.split('.')[0].split('_')
    current_dois = get_dois_from_input(filename=bso_local_filename)['doi']
    logger.debug(f'{len(current_dois)} publications in {bso_local_filename}')
    for chunk in chunks(current_dois, 10000):
        update_publications_infos(chunk, bso_local_dict, f'bso_local_{bso_local_filename}', collection_name, locals_data)
