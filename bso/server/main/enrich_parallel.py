import os
import requests
import time
import multiprocess as mp

from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich

logger = get_logger(__name__)

def enrich_results(publications: list, observations, affiliation_matching, entity_fishing, proc_num = 0, return_dict = {}) -> list:
    enriched_publications = enrich(publications=publications, observations=observations, affiliation_matching=affiliation_matching,
            entity_fishing=entity_fishing,
            datasource = None,
            last_observation_date_only=False)
    return_dict[proc_num] = enriched_publications
    return return_dict[proc_num]

def enrich_parallel(publi_chunks, observations, affiliation_matching, entity_fishing):
    logger.debug(f'start parallel with {len(publi_chunks)} sublists')
    
    manager = mp.Manager()
    return_dict = manager.dict()
    
    jobs = []
    for ix, c in enumerate(publi_chunks):
        p = mp.Process(target=enrich_results, args=(c, observations, affiliation_matching, entity_fishing, ix, return_dict))
        p.start()
        jobs.append(p)
    for p in jobs:
        p.join()
    logger.debug(f'end parallel')
    flat_list = [item for sublist in return_dict.values() for item in sublist]
    return flat_list
