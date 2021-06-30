import os
import requests
from concurrent.futures import ThreadPoolExecutor

from bso.server.main.logger import get_logger

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
NB_AFFILIATION_MATCHER = 5

logger = get_logger(__name__)

def get_country(affiliation):
    endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
    countries = requests.post(endpoint_url, json={'query': affiliation, 'type': 'country'}).json()['results']
    return countries

def is_na(x):
    return not(not x)

def filter_publications_by_country(publications: list, countries_to_keep: list = None) -> list:
    logger.debug(f'Filter {len(publications)} publication against {",".join(countries_to_keep)} countries.')
    if countries_to_keep is None:
        countries_to_keep = []
    field_name = 'detected_countries'
    # Retrieve all affiliations
    all_affiliations = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        all_affiliations += [affiliation.get('name') for affiliation in affiliations]
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliation', [])
            affiliations = [] if affiliations is None else affiliations
            all_affiliations += [affiliation.get('name') for affiliation in affiliations]
    logger.debug(f'Found {len(all_affiliations)} affiliations in total.')
    # Deduplicate affiliations
    all_affiliations_list = list(filter(is_na, list(set(all_affiliations))))
    logger.debug(f'Found {len(all_affiliations_list)} different affiliations in total.')
    # Transform list into dict
    all_affiliations_dict = {key: None for key in all_affiliations_list}
    # Retrieve countries for all publications
    #for affiliation in all_affiliations:
    #    countries = requests.post(endpoint_url, json={'query': affiliation, 'type': 'country'}).json()['results']
    #    all_affiliations[affiliation] = countries
    with ThreadPoolExecutor(max_workers=NB_AFFILIATION_MATCHER) as pool:
        countries_list = list(pool.map(get_country,all_affiliations_list))
    for ix, affiliation in enumerate(all_affiliations_list):
        all_affiliations_dict[affiliation] = countries_list[ix]
    logger.debug('All countries of all affiliations have been retrieved.')
    # Map countries with affiliations
    for publication in publications:
        countries_by_publication = []
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        for affiliation in affiliations:
            query = affiliation.get('name')
            if query in all_affiliations_dict:
                countries = all_affiliations_dict[query]
                affiliation[field_name] = countries
                countries_by_publication += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliation', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                if query in all_affiliations_dict:
                    countries = all_affiliations_dict[query]
                    affiliation[field_name] = countries
                    countries_by_publication += countries
        publication[field_name] = list(set(countries_by_publication))
    if not countries_to_keep:
        filtered_publications = publications
    else:
        countries_to_keep_set = set(countries_to_keep)
        filtered_publications = [publication for publication in publications
                                 if len(set(publication[field_name]).intersection(countries_to_keep_set)) > 0]
    logger.debug(f'After filtering by countries, {len(filtered_publications)} publications have been kept.')
    return filtered_publications
