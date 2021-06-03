import os

import requests

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')


def filter_publications_by_country(publications: list, country: str = 'fr') -> list:
    field_name = 'detected_countries'
    endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
    all_countries = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        for affiliation in affiliations:
            query = affiliation.get('name')
            countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'})
            affiliation[field_name] = countries
            all_countries += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliations', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'})
                affiliation[field_name] = countries
                all_countries += countries
        publication[field_name] = list(set(countries))
    filtered_publications = [publication for publication in publications if country.lower() in publication[field_name]]
    return filtered_publications
