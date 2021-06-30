import os
import requests

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')


def filter_publications_by_country(publications: list, countries_to_keep: list = None) -> list:
    if countries_to_keep is None:
        countries_to_keep = []
    field_name = 'detected_countries'
    endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
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
    # Deduplicate affiliations
    all_affiliations = filter(None, list(set(all_affiliations)))
    # Transform list into dict
    all_affiliations = {key: None for key in all_affiliations}
    # Retrieve countries for all publications
    for affiliation in all_affiliations:
        countries = requests.post(endpoint_url, json={'query': affiliation, 'type': 'country'}).json()['results']
        all_affiliations[affiliation] = countries
    # Map countries with affiliations
    for publication in publications:
        countries_by_publication = []
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        for affiliation in affiliations:
            query = affiliation.get('name')
            countries = all_affiliations[query]
            affiliation[field_name] = countries
            countries_by_publication += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliation', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                countries = all_affiliations[query]
                affiliation[field_name] = countries
                countries_by_publication += countries
        publication[field_name] = list(set(countries_by_publication))
    if not countries_to_keep:
        filtered_publications = publications
    else:
        countries_to_keep_set = set(countries_to_keep)
        filtered_publications = [publication for publication in publications
                                 if len(set(publication[field_name]).intersection(countries_to_keep_set)) > 0]
    return filtered_publications
