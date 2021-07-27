import requests

from concurrent.futures import ThreadPoolExecutor

from bso.server.main.config import AFFILIATION_MATCHER_SERVICE
from bso.server.main.elastic import client, load_in_es
from bso.server.main.logger import get_logger
from bso.server.main.utils_upw import chunks

NB_AFFILIATION_MATCHER = 3

logger = get_logger(__name__)


def load_matcher_data() -> bool:
    """ Initialize matcher by loading data """
    try:
        load_res = requests.get(f'{AFFILIATION_MATCHER_SERVICE}/load', timeout=1000)
        logger.debug(load_res.json())
        return True
    except Exception as error:
        logger.error(f'Error while loading here : {AFFILIATION_MATCHER_SERVICE}/load')
        logger.error(error)
        return False


def check_matcher_data_is_loaded(response: requests.Response) -> bool:
    """ Check that the matcher data is loaded """
    try:
        assert ('results' in response.json())
        logger.debug('Matcher seems healthy')
        return True
    except Exception:
        logger.debug('Matcher does not seem loaded, let\'s load it')
        return load_matcher_data()


def check_matcher_health() -> bool:
    """ Check that the matcher is available """
    try:
        response = requests.post(f'{AFFILIATION_MATCHER_SERVICE}/match_api', json={'query': 'france',
                                                                                   'type': 'country'})
        return check_matcher_data_is_loaded(response)
    except Exception as error:
        logger.error(f'Error while searching here : {AFFILIATION_MATCHER_SERVICE}/match_api')
        logger.error(error)
        return False


def get_country(affiliation: str) -> dict:
    in_cache = False
    params = {
        'size': 1,
        'query': {
            'term': {
                'affiliation.keyword': affiliation
            }
        }
    }
    r = client.search(index='bso-cache-country', body=params)
    hits = r['hits']['hits']
    if len(hits) >= 1:
        in_cache = True
        countries = hits[0]['_source']['countries']
    else:
        strategies = [
                ['grid_city', 'grid_name', 'country_all_names'],
                ['grid_city', 'country_all_names'],
                ['grid_city', 'country_alpha3'],
                ['country_all_names'],
                ['country_subdivisions', 'country_alpha3']
        ]
        countries = requests.post(f'{AFFILIATION_MATCHER_SERVICE}/match_api',
                                  json={'query': affiliation, 'type': 'country', 'strategies': strategies}
                                  ).json()['results']
    return {'countries': countries, 'in_cache': in_cache}


def is_na(x) -> bool:
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
            affiliations = author.get('affiliations', [])
            affiliations = [] if affiliations is None else affiliations
            all_affiliations += [affiliation.get('name') for affiliation in affiliations]
    logger.debug(f'Found {len(all_affiliations)} affiliations in total.')
    # Deduplicate affiliations
    all_affiliations_list = list(filter(is_na, list(set(all_affiliations))))
    logger.debug(f'Found {len(all_affiliations_list)} different affiliations in total.')
    # Transform list into dict
    all_affiliations_dict = {}
    if check_matcher_health():
        for all_affiliations_list_chunk in chunks(all_affiliations_list, 1000):
            with ThreadPoolExecutor(max_workers=NB_AFFILIATION_MATCHER) as pool:
                countries_list = list(pool.map(get_country, all_affiliations_list_chunk))
            for ix, affiliation in enumerate(all_affiliations_list_chunk):
                all_affiliations_dict[affiliation] = countries_list[ix]
            logger.debug(f'{len(all_affiliations_dict)} / {len(all_affiliations_list)} treated in country_matcher')
            logger.debug(f'loading in cache')
            cache = []
            for ix, affiliation in enumerate(all_affiliations_list_chunk):
                if affiliation in all_affiliations_dict and not all_affiliations_dict[affiliation]['in_cache']:
                    cache.append({'affiliation': affiliation,
                                  'countries': all_affiliations_dict[affiliation]['countries']})
            load_in_es(data=cache, index='bso-cache-country')
        logger.debug('All countries of all affiliations have been retrieved.')
        # Map countries with affiliations
        for publication in publications:
            countries_by_publication = []
            affiliations = publication.get('affiliations', [])
            affiliations = [] if affiliations is None else affiliations
            for affiliation in affiliations:
                query = affiliation.get('name')
                if query in all_affiliations_dict:
                    countries = all_affiliations_dict[query]['countries']
                    affiliation[field_name] = countries
                    countries_by_publication += countries
            authors = publication.get('authors', [])
            for author in authors:
                affiliations = author.get('affiliations', [])
                for affiliation in affiliations:
                    query = affiliation.get('name')
                    if query in all_affiliations_dict:
                        countries = all_affiliations_dict[query]['countries']
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
    else:
        filtered_publications = publications
        logger.debug('Error while matching publications !')
    return filtered_publications
