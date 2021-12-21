import os
import requests
import time
import multiprocess as mp
import redis
from rq import Connection, Queue
from flask import Blueprint, current_app, jsonify, render_template, request

from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich

logger = get_logger(__name__)
default_timeout = 43200000

def create_task(args: dict) -> list:
    publications = args.get('publications', [])
    observations = args.get('observations', [])
    affiliation_matching = args.get('affiliation_matching', False)
    entity_fishing = args.get('entity_fishing', False)
    datasource = args.get('datasource', 'user')
    last_observation_date_only = args.get('last_observation_date_only', False)
    return enrich(publications=publications, observations=observations, datasource=datasource, affiliation_matching=affiliation_matching,
            entity_fishing=entity_fishing,
            last_observation_date_only=last_observation_date_only)

def enrich_results(publications: list, observations, affiliation_matching, entity_fishing, proc_num = 0, return_dict = {}) -> list:
    args = {'publications': publications, 'observations': observations, 'affiliation_matching': affiliation_matching, 'entity_fishing': entity_fishing, 'datasource':None}
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task, args)
    
    task_id = task.get_id() 

    for i in range(0, 100000):
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(name='bso-publications')
            task = q.fetch_job(task_id)
        if task:
            status = task.get_status()
            if status == 'finished':
                return_dict[proc_num] = task.result
                return return_dict[proc_num]
            elif status in ['started', 'queued']:
                time.sleep(2)
                continue
            else:
                logger.error(f'Error with task {task_id} : status {status}')
                return_dict[proc_num] = []
                return return_dict[proc_num]
    return return_dict[proc_num]

def enrich_parallel(publications, observations, affiliation_matching, entity_fishing):
    logger.debug(f'start parallel with {len(publications)} sublists')
    
    manager = mp.Manager()
    return_dict = manager.dict()
    
    jobs = []
    for ix, c in enumerate(publications):
        p = mp.Process(target=enrich_results, args=(c, observations, affiliation_matching, entity_fishing, ix, return_dict))
        p.start()
        jobs.append(p)
    for p in jobs:
        p.join()
    logger.debug(f'end parallel')
    flat_list = [item for sublist in return_dict.values() for item in sublist]
    return flat_list
