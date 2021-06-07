import os

import requests

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')


def filter_publications_by_country(publications: list, country: str = None) -> list:
    field_name = 'detected_countries'
    endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
    all_countries = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        for affiliation in affiliations:
            query = affiliation.get('name')
            countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'}).json()['logs']
            affiliation[field_name] = countries
            all_countries += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliation', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'}).json()['logs']
                affiliation[field_name] = countries
                all_countries += countries
        publication[field_name] = list(set(all_countries))
    if country is None:
        filtered_publications = publications
    else:
        filtered_publications = [publication for publication in publications if country.lower() in
                                 publication[field_name]]
    return filtered_publications
