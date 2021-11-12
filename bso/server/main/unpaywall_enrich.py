import fasttext
import os
import pandas as pd

from dateutil import parser
from typing import Union

from bso.server.main.apc.apc_detect import detect_apc
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.affiliation_matcher import get_matcher_parallel
from bso.server.main.field_detect import detect_fields
from bso.server.main.logger import get_logger
from bso.server.main.predatory.predatory_detect import detect_predatory
from bso.server.main.publisher.publisher_detect import detect_publisher
from bso.server.main.strings import dedup_sort, normalize, remove_punction, get_words
from bso.server.main.unpaywall_mongo import get_doi_full
from bso.server.main.utils import download_file, FRENCH_ALPHA2
from bso.server.main.utils_upw import chunks, format_upw_millesime

logger = get_logger(__name__)
models = {}
project_id = os.getenv('OS_TENANT_ID')


def init_model_lang() -> None:
    logger.debug('Init model lang')
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    lid_model_name = f'{MOUNTED_VOLUME}lid.176.bin'
    if not os.path.exists(lid_model_name):
        download_file(f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/models/lid.176.bin',
                      upload_to_object_storage=False, destination=lid_model_name)
    lid_model = fasttext.load_model(lid_model_name)
    models['lid'] = lid_model


def identify_language(text: str) -> Union[str, None]:
    if 'lid' not in models:
        init_model_lang()
    if text is None or len(text) < 3:
        return None
    text = remove_punction(text.replace('\n', ' ').replace('\xa0', ' ')).strip()
    return (models['lid'].predict(text, 1)[0][0]).replace('__label__', '')


def normalize_genre(genre, publisher) -> str:
    if publisher in ['Cold Spring Harbor Laboratory', 'Research Square']:
        return 'preprint'
    if genre in ['journal-article', 'book-chapter']:
        return genre
    if 'proceedings' in genre:
        return 'proceedings'
    if genre in ['book', 'monograph']:
        return 'book'
    return 'other'


def get_affiliation_types(affiliation: str) -> dict:
    normalized_affiliation = normalize(affiliation)
    is_university = False
    if 'centre hospitalier univ' in normalized_affiliation:
        is_university = False
    else:
        for word in ['universite', 'université', 'university', 'univ']:
            if word in normalized_affiliation:
                is_university = True
    is_hospital = False
    for word in ['hospit', 'hopit', 'ch ', 'chu', 'chru', 'aphp', 'aphm']:
        if word in normalized_affiliation:
            is_hospital = True
    is_inserm = False
    for word in ['inserm', 'institut national de la santé']:
        if word in normalized_affiliation:
            is_inserm = True
    is_cnrs = False
    for word in ['cnrs', 'umr']:
        if word in normalized_affiliation:
            is_cnrs = True
    return {
        'is_cnrs': is_cnrs,
        'is_hospital': is_hospital,
        'is_inserm': is_inserm,
        'is_university': is_university
    }


def compute_affiliations_types(affiliations: list) -> list:
    result = []
    for affiliation in affiliations:
        res = get_affiliation_types(affiliation)
        if res.get('is_university'):
            result.append('university')
        if res.get('is_hospital'):
            result.append('hospital')
        if res.get('is_inserm'):
            result.append('inserm')
        if res.get('is_cnrs'):
            result.append('cnrs')
    result = dedup_sort(result)
    return result


def has_fr(countries: list) -> bool:
    if not countries or not isinstance(countries, list):
        return False
    for country in countries:
        if country.lower() in FRENCH_ALPHA2:
            return True
    return False


def format_upw(dois_infos: dict, extra_data: dict) -> list:
    final = []
    for doi in dois_infos:
        if 'global' not in dois_infos[doi]:
            res = {'doi': doi}
        else:
            res = dois_infos[doi]['global']
        if doi in extra_data:
            res.update(extra_data[doi])
        if 'z_authors' in res:
            # todo implement a merge if 'authors' is in res
            if 'authors' not in res:
                res['authors'] = res['z_authors']
            del res['z_authors']
        # Fields detection
        res = detect_fields(res)
        # APC
        info_apc = detect_apc(doi, res.get('journal_issns'), res.get('publisher'),
                              res.get('published_date', '2100-01-01'), dois_infos[doi])
        res.update(info_apc)
        # Language
        lang_mapping = {
                'english': 'en',
                'french': 'fr',
                'spanish': 'es',
                'german': 'de',
                'dutch': 'nl',
                'italian': 'it'
        }
        if isinstance(res.get('lang'), str) and res.get('lang').lower() in lang_mapping:
            res['lang'] = lang_mapping[res['lang'].lower()]
        elif 'lang' not in res or res['lang'] is None or len(res['lang']) != 2 or res['lang'] != res['lang'].lower():
            publi_title_abstract = ''
            words_title = get_words(res.get('title', ''))
            if isinstance(words_title, str):
                publi_title_abstract += words_title + ' '
            words_abstract = get_words(res.get('abstract', ''))
            if isinstance(words_abstract, str):
                publi_title_abstract += words_abstract
            if len(publi_title_abstract) > 5:
                res['lang'] = identify_language(publi_title_abstract.strip())
        # Predatory info
        pred_info = detect_predatory(res.get('publisher'), res.get('journal_name'))
        res.update(pred_info)
        # normalisation des editeurs
        published_year = None
        if isinstance(res.get('published_date'), str):
            published_year = res.get('published_date')[0:4]
        publisher_clean = detect_publisher(res.get('publisher'), published_year, doi) 
        res.update(publisher_clean)
        #if res.get('publisher_normalized') in ['Cold Spring Harbor Laboratory']:
        #    res['domains'] = ['health']
        # Genre
        if isinstance(res.get('genre'), str):
            res['genre_raw'] = res['genre']
            res['genre'] = normalize_genre(res['genre'], res.get('publisher_normalized'))
        # Affiliations
        affiliations = res.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        fr_affil = [a.get('name', '') for a in affiliations if has_fr(a.get('detected_countries'))]
        fr_affil_types = compute_affiliations_types(fr_affil)
        res['french_affiliations_types'] = fr_affil_types
        # Authors useful rank
        author_useful_rank_countries = []
        authors = res.get('authors', [])
        if not isinstance(authors, list):
            authors = []
        nb_authors = len(authors)
        for index, author in enumerate(authors):
            affiliations = author.get('affiliations', [])
            if not isinstance(affiliations, list):
                affiliations = []
            for affiliation in affiliations:
                if index == 0 or index == nb_authors - 1:
                    author_useful_rank_countries += affiliation.get('detected_countries', [])
        author_useful_rank_countries = list(set(author_useful_rank_countries))
        author_useful_rank_fr = has_fr(author_useful_rank_countries)
        res['author_useful_rank_fr'] = author_useful_rank_fr
        res['author_useful_rank_countries'] = author_useful_rank_countries
        # OA Details
        res['observation_dates'] = []
        res['oa_details'] = {}
        for asof in dois_infos[doi]:
            if asof == 'global':
                continue
            else:
                tmp = format_upw_millesime(dois_infos[doi][asof], asof, res['has_apc'])
                res['oa_details'].update(tmp)
                res['observation_dates'].append(list(tmp.keys())[0])  # getting the key that is the observation date
        for field in ['amount_apc_doaj', 'amount_apc_doaj_EUR', 'amount_apc_EUR', 'is_paratext', 'issn_print',
                      'has_coi', 'has_grant', 'pmid', 'publication_year', 'year']:
            if pd.isna(res.get(field)):
                res[field] = None
        for field in ['has_coi', 'has_grant', 'is_paratext']:
            if res.get(field, 0.0) == 0.0:
                res[field] = False
        # not exposing some fields in index
        for f in ['authors', 'references', 'abstract', 'incipit']:
            if f in res:
                del res[f]
        final.append(res)
    return final


def enrich(publications: list, observations: list, datasource: str, affiliation_matching: bool) -> list:
    publis_dict = {}

    
    # affiliation matcher
    publicationsWithAffiliations = []
    if affiliation_matching:
        NB_PARALLEL_JOBS = 20
        PUBLI_GROUP_SIZE = 100
        logger.debug(f'affiliation matching for {len(publications)} publications')
        publis_chunks = list(chunks(lst=publications, n=PUBLI_GROUP_SIZE))
        groups = list(chunks(lst=publis_chunks, n=NB_PARALLEL_JOBS))
        for chunk in groups:
            publicationsWithAffiliations += get_matcher_parallel(chunk)
        publications = publicationsWithAffiliations
    
    for p in publications:
        if datasource:
            p['datasource'] = datasource
        if 'doi' in p and isinstance(p['doi'], str):
            doi = p['doi'].lower()
            publis_dict[doi] = p
    all_updated = []
    logger.debug(f'Enriching {len(publications)} publications')
    for publi_chunk in chunks(lst=publications, n=5000):
        doi_chunk = [p.get('doi') for p in publi_chunk if p and isinstance(p.get('doi'), str) and '10' in p['doi']]
        

        data = get_doi_full(dois=doi_chunk, observations=observations)
        # Remove data with no oa details info (not indexed in unpaywall)
        new_updated = format_upw(dois_infos=data, extra_data=publis_dict)
        for d in new_updated:
            if len(d.get('oa_details', {})) == 0:
                continue
            # some post-filtering
            if d.get('publisher_group') in ['United Nations', 'World Trade Organization']:
                continue
            if d.get('genre') == 'other':
                continue
            all_updated.append(d)
        logger.debug(f'{len(publi_chunk)} / {len(publications)} enriched')
    for p in all_updated:
        field_to_del = []
        for field in p:
            if isinstance(p.get(field), str) and field.endswith('_date'):
                try:
                    p[field] = parser.parse(p[field]).isoformat()
                except:
                    logger.debug(f"error for field {field} : {p[field]} of type {type(p[field])}, deleting field")
                    field_to_del.append(field)
        for field in field_to_del:
            del p[field]
    return all_updated
