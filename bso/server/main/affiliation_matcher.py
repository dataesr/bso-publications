import os
import requests
import time
import pymongo
import multiprocess as mp
from bso.server.main.utils import get_hash

from bso.server.main.logger import get_logger


AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/enrich_with_affiliations_id'


logger = get_logger(__name__)


def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            logger.error(f'{func.__name__} raises an error through decorator "exception_handler".')
            logger.error(exception)
            return None
    return inner_function

def get_from_mongo(name):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'affiliations'
    mycoll = mydb[collection_name]
    name_md5 = get_hash(name)
    res = mycoll.find_one({'name_md5': name_md5})
    if res:
        return res['ids']
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

def get_affiliations_computed(publications, recompute_all = False):
    affiliations = {}
    done, todo = [], []
    for p in publications:
        nb_aff_with_id = 0
        nb_aff = 0
        for aff in p.get('affiliations'):
            aff_name = aff.get('name')
            if not aff_name:
                continue
            if recompute_all is False:
                if aff_name not in affiliations:
                    res = get_from_mongo(aff_name)
                    if res:
                        affiliations[aff_name] = res 
                if aff_name in affiliations:
                    aff['ids'] = affiliations[aff_name]
                    nb_aff_with_id += 1
                nb_aff += 1
        authors = p.get('authors')
        if isinstance(authors, list):
            for aut in authors:
                if isinstance(aut.get('affiliations'), list):
                    for aff in aut.get('affiliations'):
                        if aff['name'] in affiliations:
                            aff['ids'] = affiliations[aff['name']]
        if nb_aff_with_id == nb_aff and recompute_all is False:
            done.append(p)
        else:
            # remove None affiliations / authors
            todo.append(clean(p))
    logger.debug(f'affiliation matching {len(todo)}/{len(publications)} todo, {len(done)}/{len(publications)} done')
    return done, todo


@exception_handler
def get_matcher_results(publications: list, proc_num = 0, return_dict = {}) -> list:
    r = requests.post(matcher_endpoint_url, json={'publications': publications,
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
def get_matcher_parallel(publi_chunks):
    logger.debug(f'start parallel with {len(publi_chunks)} sublists')
    
    manager = mp.Manager()
    return_dict = manager.dict()
    
    jobs = []
    for ix, c in enumerate(publi_chunks):
        p = mp.Process(target=get_matcher_results, args=(c, ix, return_dict))
        p.start()
        jobs.append(p)
    for p in jobs:
        p.join()
    logger.debug(f'end parallel')
    flat_list = [item for sublist in return_dict.values() for item in sublist]
    return flat_list
