import os
import math
import requests
import time
import timeout_decorator
import pymongo
import multiprocess as mp
import re
from bso.server.main.utils import get_hash
from bso.server.main.utils_upw import chunks

from bso.server.main.logger import get_logger


AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_list'


logger = get_logger(__name__)

def is_na(x):
    return not(not x)

def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            logger.error(f'{func.__name__} raises an error through decorator "exception_handler".')
            logger.error(exception)
            return None
    return inner_function

def get_affiliation_from_mongo(name, myclient):
    mydb = myclient['scanr']
    collection_name = 'affiliations'
    mycoll = mydb[collection_name]
    query_md5 = get_hash(name)
    res = mycoll.find_one({'id': query_md5})
    if res:
        return res['cache']
    return

def clean(p):
    if not isinstance(p.get('authors'), list):
        p['authors'] = []
    if not isinstance(p.get('affiliations'), list):
        p['affiliations'] = []
    for aut in p['authors']:
        if 'affiliations' in aut and not isinstance(aut.get('affiliations'), list):
            aut['affiliations'] = []
    return p

def is_ed(s):
    pattern = r"^ED\d{1,3}$"
    return bool(re.match(pattern, s))

def get_affiliations_computed(publications, recompute_all = False, compute_missing = True):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    affiliations = {}
    done, todo = [], []
    for p in publications:
        nb_aff_with_id = 0
        nb_aff = 0
        for aff in p.get('affiliations', []):
            aff_name = get_query_from_affiliation(aff)
            if not aff_name:
                continue
            if recompute_all is False:
                if aff_name not in affiliations:
                    res = get_affiliation_from_mongo(aff_name, myclient)
                    if isinstance(res, list):
                        affiliations[aff_name] = res 
                if aff_name in affiliations:
                    aff['ids'] = affiliations[aff_name]
                    nb_aff_with_id += 1
                nb_aff += 1
            if isinstance(aff.get('aliases'), list):
                for alias in aff['aliases']:
                    if is_ed(alias):
                        if aff_name not in affiliations:
                            affiliations[aff_name] = []
                        affiliations[aff_name].append({'id': alias, 'type': 'ed'})
                        if 'ids' not in aff:
                            aff['ids'] = []
                        aff['ids'].append({'id': alias, 'type': 'ed'})
        authors = p.get('authors')
        if isinstance(authors, list):
            for aut in authors:
                if isinstance(aut.get('affiliations'), list):
                    for aff in aut.get('affiliations', []):
                        aff_name = get_query_from_affiliation(aff)
                        if aff_name in affiliations:
                            aff['ids'] = affiliations[aff_name]
        if nb_aff_with_id == nb_aff and recompute_all is False:
            done.append(p)
        else:
            if compute_missing:
                # remove None affiliations / authors
                todo.append(clean(p))
            else:
                done.append(p)
    logger.debug(f'affiliation matching {len(todo)}/{len(publications)} todo, {len(done)}/{len(publications)} done')
    myclient.close()
    return done, todo

@timeout_decorator.timeout(50*60)
def get_matcher_results(affiliations: list, proc_num = 0, return_dict = {}) -> list:
    r = requests.post(matcher_endpoint_url, json={'affiliations': affiliations,
                                                  'queue': 'matcher_short'})
    task_id = r.json()['data']['task_id']
    logger.debug(f'New task {task_id} for matcher')
    for i in range(0, 100000):
        r_task = requests.get(f'{AFFILIATION_MATCHER_SERVICE}/tasks/{task_id}').json()
        try:
            status = r_task['data']['task_status']
        except:
            logger.error(f'Error in getting task {task_id} status : {r_task}')
            status = 'error'
        if status == 'finished':
            return_dict[proc_num] = r_task['data']['task_result']
            return return_dict[proc_num]
        elif status in ['started', 'queued']:
            time.sleep(2)
            continue
        else:
            logger.error(f'Error with task {task_id} : status {status}')
            logger.debug(f'{r_task}')
            return_dict[proc_num] = []
            return return_dict[proc_num]

@exception_handler
def get_matcher_parallel(affil_chunks):
    # prend une liste de liste d'affiliations
    logger.debug(f'start parallel with {len(affil_chunks)} sublists')
    
    manager = mp.Manager()
    return_dict = manager.dict()
    
    jobs = []
    for ix, c in enumerate(affil_chunks):
        if len(c) == 0:
            continue
        p = mp.Process(target=get_matcher_results, args=(c, ix, return_dict))
        p.start()
        jobs.append(p)
    for p in jobs:
        p.join()
    logger.debug(f'end parallel')
    flat_list = [item for sublist in return_dict.values() for item in sublist]
    return flat_list

def get_query_from_affiliation(affiliation):
    query_elts = []
    keys = list(affiliation.keys())
    keys.sort()
    for f in affiliation:
        if f.lower() in ['name', 'ror', 'grid', 'rnsr', 'country', 'city', 'aliases']:
            if isinstance(affiliation.get(f), str) and affiliation[f]:
                query_elts.append(affiliation[f])
            if isinstance(affiliation.get(f), list):
                for k in affiliation[f]:
                    if isinstance(k, str) and k:
                        query_elts.append(k)
    return ' '.join(query_elts)

def enrich_publications_with_affiliations_id(publications: list) -> dict:
    logger.debug(f'Matching affiliations for {len(publications)} publications')
    # Retrieve all affiliations
    all_affiliations = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        all_affiliations += [get_query_from_affiliation(affiliation) for affiliation in affiliations]
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliations', [])
            affiliations = [] if affiliations is None else affiliations
            all_affiliations += [get_query_from_affiliation(affiliation) for affiliation in affiliations]
    logger.debug(f'Found {len(all_affiliations)} affiliations in total.')
    # Deduplicate affiliations
    all_affiliations_list = list(filter(is_na, list(set(all_affiliations))))
    logger.debug(f'Found {len(all_affiliations_list)} different affiliations in total.')
    NB_PARALLEL_JOB = 10
    nb_publis_per_group = math.ceil(len(all_affiliations_list)/NB_PARALLEL_JOB)
    groups = list(chunks(lst=all_affiliations_list, n=nb_publis_per_group))
    logger.debug(f'{len(groups)} groups with {nb_publis_per_group} each')
    all_affiliations_matches = get_matcher_parallel(groups)
    all_affiliations_dict = {}
    for elt in all_affiliations_matches:
        query = elt['query']
        all_affiliations_dict[query] = elt['matches']
    # Map countries with affiliations
    for publication in publications:
        affiliationsIds_by_publication = []
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        for affiliation in affiliations:
            query = get_query_from_affiliation(affiliation)
            if query in all_affiliations_dict:
                results = all_affiliations_dict[query]
                if 'ids' not in affiliation:
                    affiliation['ids'] = []
                for result in results:
                    if result not in affiliation['ids']:
                        affiliation['ids'].append(result)
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliations', [])
            for affiliation in affiliations:
                query = get_query_from_affiliation(affiliation)
                if query in all_affiliations_dict:
                    results = all_affiliations_dict[query]
                    if 'ids' not in affiliation:
                        affiliation['ids'] = []
                    for result in results:
                        if result not in affiliation['ids']:
                            affiliation['ids'].append(result)
    return publications
