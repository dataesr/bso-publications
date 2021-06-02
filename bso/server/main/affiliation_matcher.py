import os

import requests

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')


def filter_publications_by_country(publications: list, country: str = 'fr') -> list:
    for publication in publications:
        countries = requests.post(f'{AFFILIATION_MATCHER_SERVICE}/match_api', json={'query': publication,
                                                                                    'type': 'country'})
        publication['matched_countries'] = countries
    french_publications = [publication for publication in publications if country in publication['matched_countries']]
    return french_publications
