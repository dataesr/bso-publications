import os

import fasttext

from bso.server.main.apc.apc_detect import detect_apc
from bso.server.main.field_detect import detect_fields
from bso.server.main.logger import get_logger
from bso.server.main.predatory.predatory_detect import detect_predatory
from bso.server.main.strings import dedup_sort, normalize, remove_punction
from bso.server.main.unpaywall_mongo import get_doi_full
from bso.server.main.utils import download_file
from bso.server.main.utils_upw import chunks, format_upw_millesime

PV_MOUNT = '/src/models/'
logger = get_logger(__name__)
models = {}
os.system(f'mkdir -p {PV_MOUNT}')
project_id = os.getenv('OS_TENANT_ID')


def init_model_lang() -> None:
    logger.debug('Init model lang')
    lid_model_name = f'{PV_MOUNT}lid.176.bin'
    if not os.path.exists(lid_model_name):
        download_file(f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/models/lid.176.bin',
                      upload_to_object_storage=False, destination=lid_model_name)
    lid_model = fasttext.load_model(lid_model_name)
    models['lid'] = lid_model


def identify_language(text: str) -> str:
    if 'lid' not in models:
        init_model_lang()
    if len(text) < 3 or text is None:
        return None
    text = remove_punction(text.replace('\n', ' ').replace('\xa0', ' ')).strip()
    return (models['lid'].predict(text, 1)[0][0]).replace('__label__', '')


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


def has_fr(countries: list):
    if not countries or not isinstance(countries, list):
        return False
    for country in countries:
        if country.lower() == 'fr':
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
            if 'authors' in res:
                # todo implement a merge 
                del res['z_authors']
            else:
                res['authors'] = res['z_authors']
                del res['z_authors']
        # APC
        info_apc = detect_apc(doi, res.get('journal_issns'), res.get('published_date', '2020-01-01'))
        res.update(info_apc)
        # Language
        if 'language' not in res or len(res['language']) < 2:
            publi_title_abstract = ''
            if isinstance(res.get('title'), str):
                publi_title_abstract += res.get('title', '') + ' '
            if isinstance(res.get('abstract'), str):
                publi_title_abstract += res.get('abstract', '')
            if len(publi_title_abstract) > 5:
                res['language'] = identify_language(publi_title_abstract.strip())
        # Predatory info
        pred_info = detect_predatory(res.get('publisher'), res.get('journal_name'))
        res.update(pred_info)
        # Fields detection
        res = detect_fields(res)
        # Retraction info
        # retraction_info = detect_retraction(x.get('doi'), x.get('pmid'))
        # res.update(retraction_info)
        # Affiliations
        fr_affil = [a.get('name', '') for a in res.get('affiliations', []) if has_fr(a.get('countries'))]
        fr_affil_types = compute_affiliations_types(fr_affil)
        res['french_affiliations_types'] = fr_affil_types
        # Authors useful rank
        author_useful_rank_countries = []
        nb_authors = len(res.get('authors', []))
        for index, author in enumerate(res.get('authors', [])):
            for affiliation in author.get('affiliations', []):
                if index == 0 or index == nb_authors - 1:
                    author_useful_rank_countries += affiliation.get('matched_countries', [])
        author_useful_rank_countries = list(set(author_useful_rank_countries))
        author_useful_rank_fr = 'FR' in author_useful_rank_countries
        res['author_useful_rank_fr'] = author_useful_rank_fr
        res['author_useful_rank_countries'] = author_useful_rank_countries
        # OA Details
        res['oa_details'] = []
        for asof in dois_infos[doi]:
            if asof == 'global':
                continue
            else:
                tmp = format_upw_millesime(dois_infos[doi][asof], asof, res['has_apc'])
                res['oa_details'].update(tmp)
        final.append(res)
    return final


def enrich(publications: list) -> list:
    publis_dict = {}
    for p in publications:
        if 'doi' in p:
            doi = p['doi'].lower()
            publis_dict[doi] = p
    all_updated = []
    for publi_chunk in chunks(publications, 100):
        doi_chunk = [p.get('doi') for p in publi_chunk if ('doi' in p and '10' in p['doi'])]
        data = get_doi_full(doi_chunk)
        all_updated += format_upw(data, publis_dict)
    return all_updated
