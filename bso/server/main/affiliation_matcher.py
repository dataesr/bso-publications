import os
import requests

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')

def filter_publications_by_country(publications: list, countries_to_keep: list = []) -> list:
    field_name = 'detected_countries'
    endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
    all_countries = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        for affiliation in affiliations:
            query = affiliation.get('name')
            countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'}).json()['results']
            affiliation[field_name] = countries
            all_countries += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliation', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                countries = requests.post(endpoint_url, json={'query': query, 'type': 'country'}).json()['results']
                affiliation[field_name] = countries
                all_countries += countries
        publication[field_name] = list(set(all_countries))
    if not countries_to_keep:
        filtered_publications = publications
    else:
        countries_in_publi = set(publication[field_name])
        filtered_publications = [publication for publication in publications if len(countries_in_publi.intersection(countries_to_keep)) > 0 ]
    return filtered_publications
