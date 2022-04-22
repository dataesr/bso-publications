import fasttext
import os
import pandas as pd

from dateutil import parser
from typing import Union

from bso.server.main.apc.apc_detect import detect_apc
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.affiliation_matcher import get_matcher_parallel, get_affiliations_computed
from bso.server.main.field_detect import detect_fields
from bso.server.main.logger import get_logger
from bso.server.main.predatory.predatory_detect import detect_predatory
from bso.server.main.publisher.publisher_detect import detect_publisher
from bso.server.main.strings import dedup_sort, normalize, remove_punction, get_words
from bso.server.main.unpaywall_mongo import get_doi_full
from bso.server.main.utils import download_file, FRENCH_ALPHA2
from bso.server.main.utils_upw import chunks, format_upw_millesime
from bso.server.main.entity_fishing import get_entity_fishing

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
    for word in ['inserm', 'institut national de la sante']:
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


def format_upw(dois_infos: dict, extra_data: dict, entity_fishing: bool) -> list:
    # dois_infos contains unpaywall infos (oa_details + crossref meta) => only on crossref DOIs
    # extra_data contains info for all publi, even if no DOI crossref
    final = []
    identifier_idx = 0
    for identifier in extra_data:
        identifier_idx += 1
        if identifier_idx % 2500 == 0:
            logger.debug(f'format_upw {identifier_idx} / {len(extra_data)}')
        res = extra_data[identifier]
        doi = res.get('doi')
        if isinstance(doi, str) and (doi in dois_infos) and ('global' in dois_infos[doi]):
            res.update(dois_infos[doi]['global'])
        if 'z_authors' in res and isinstance(res['z_authors'], list):
            for ix, a in enumerate(res['z_authors']):
                full_name = ''
                last_name = a.get('family')
                first_name = a.get('given')
                if isinstance(first_name, str):
                    full_name = f'{first_name} '
                    a['first_name'] = first_name.strip()
                if isinstance(last_name, str):
                    full_name += last_name
                    a['last_name'] = last_name.strip()
                full_name = full_name.strip()
                if full_name:
                    a['full_name'] = full_name
                a['datasource'] = 'crossref'
                a['author_position'] = ix + 1
        # Fields detection
        #logger.debug('fields1')
        classification_types = ['bso']
        domains = res.get('domains', [])
        if not isinstance(domains, list):
            domains = []
        if 'health' in domains:
            classification_types.append('bsso')
        
        # TODO TO REMOVE
        #if entity_fishing:
        #    classification_types.append('sdg')
        res = detect_fields(res, classification_types)
        
        #logger.debug('fieldsEND')
        # APC
        published_date_for_apc = res.get('published_date')
        if not isinstance(published_date_for_apc, str):
            published_date_for_apc = '2100-01-01'
            #logger.debug(f"missing published date ({res.get('published_date')}) for doi {doi}, using a fallback in future for apc")
        if isinstance(doi, str) and (doi in dois_infos):
            info_apc = detect_apc(doi, res.get('journal_issns'), res.get('publisher'),
                              published_date_for_apc, dois_infos[doi])
            res.update(info_apc)
        #logger.debug('APC_END')
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
        elif (not(isinstance(res.get('lang'), str))) or (len(res['lang']) != 2) or (res['lang'] != res['lang'].lower()):
            publi_title_abstract = ''
            words_title = get_words(res.get('title'))
            if isinstance(words_title, str):
                publi_title_abstract += words_title + ' '
            words_abstract = get_words(res.get('abstract'))
            if isinstance(words_abstract, str):
                publi_title_abstract += words_abstract
            publi_title_abstract = publi_title_abstract.strip()
            if len(publi_title_abstract) > 5:
                res['lang'] = identify_language(publi_title_abstract.strip())
            else:
                pass
                #logger.debug(f'not enough info title / abstract for doi {doi} : {publi_title_abstract}')
        # Entity fishing
        if entity_fishing:
            ef_info = get_entity_fishing(res)
            if ef_info:
                res.update(ef_info)
        # Predatory info
        pred_info = detect_predatory(res.get('publisher'), res.get('journal_name'))
        res.update(pred_info)
        #logger.debug('PREDA_END')
        # Language
        # normalisation des editeurs
        published_year = None
        if isinstance(res.get('published_date'), str):
            published_year = res.get('published_date')[0:4]
        publisher_raw = res.get('publisher')
        if not publisher_raw:
            publisher_raw = 'unknown'
        publisher_clean = detect_publisher(publisher_raw, published_year, doi) 
        res.update(publisher_clean)
        #logger.debug('PUBLISHER_END')
        #if res.get('publisher_normalized') in ['Cold Spring Harbor Laboratory']:
        #    res['domains'] = ['health']
        # Genre
        if isinstance(res.get('genre'), str):
            res['genre_raw'] = res['genre']
            res['genre'] = normalize_genre(res['genre'], res.get('publisher_normalized'))
        # OA Details
        if 'oa_details' not in res and isinstance(doi, str) and doi in dois_infos:
            res['observation_dates'] = []
            res['oa_details'] = {}
            last_millesime = None
            for asof in dois_infos[doi]:
                if asof == 'global':
                    continue
                else:
                    tmp = format_upw_millesime(dois_infos[doi][asof], asof, res['has_apc'], res['publisher_dissemination'])
                    res['oa_details'].update(tmp)
                    res['observation_dates'].append(list(tmp.keys())[0])  # getting the key that is the observation date
                    if last_millesime:
                        last_millesime = max(last_millesime, asof)
                    else:
                        last_millesime = asof

            #logger.debug('MILLESIME_END')
            # get hal_id if present in one of the last oa locations
            if last_millesime:
                last_oa_loc = dois_infos[doi][last_millesime].get('oa_locations', [])
                #if 'hybrid' not in dois_infos[doi][last_millesime].get('oa_colors', []) and 'gold' not in dois_infos[doi][last_millesime].get('oa_colors', []):
                #    # si ni gold ni hybrid '
                #    res['amount_apc_EUR'] = 0
                #    if res['has_apc'] == True:
                #        res['has_apc'] = None
                if isinstance(last_oa_loc, list):
                    for loc in last_oa_loc:
                        if loc.get('repository_normalized') == 'HAL' or 'archives-ouvertes.fr' in loc.get('url'):
                            hal_id = None
                            if isinstance(loc.get('pmh_id'), str):
                                hal_id = loc['pmh_id'].split(':')[2].strip().lower()
                                if hal_id[-2] == 'v': # remove version
                                    hal_id = hal_id[:-2]
                            if hal_id is None and isinstance(loc.get('url_for_pdf'), str) and '/document' in loc['url_for_pdf'].lower():
                                try:
                                    url_split = loc['url_for_pdf'].lower().split('/')[-2]
                                    if '-' in url_split:
                                        hal_id = url_split
                                except:
                                    pass
                            if hal_id:
                                external_ids = []
                                external_ids.append({'id_type': 'hal_id', 'id_value': hal_id})
                                res['external_ids'] = external_ids
                                res['hal_id'] = hal_id
        if 'oa_details' not in res:
            pass
            #logger.debug(f'no oa details for publi {res["id"]}')

        #logger.debug('HAL_END')
        for field in ['amount_apc_doaj', 'amount_apc_doaj_EUR', 'amount_apc_EUR', 'is_paratext', 'issn_print',
                      'has_coi', 'has_grant', 'pmid', 'publication_year', 'year']:
            if pd.isna(res.get(field)):
                res[field] = None
        for field in ['has_coi', 'has_grant', 'is_paratext']:
            if res.get(field, 0.0) == 0.0:
                res[field] = False
            elif res.get(field, 1) == 1:
                res[field] = True
        final.append(res)
    logger.debug(f'format_upw DONE')
    return final

def treat_affiliations_authors(res):
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
    return res


def check(aut1, aut2):
    if not isinstance(aut1.get('full_name'), str):
        return False
    if not isinstance(aut2.get('full_name'), str):
        return False
    fullname1 = set(normalize(aut1.get('full_name'), min_length=3).split(' '))
    fullname2 = set(normalize(aut2.get('full_name'), min_length=3).split(' '))
    return len(fullname1.intersection(fullname2)) > 0


def merge_element(elt1, elt2):
    for f in elt2:
        if f not in elt1 and isinstance(elt2[f], str):
            elt1[f] = elt2[f]
        if isinstance(elt2[f], list):
            if f not in elt1:
                elt1[f] = []
            elt1[f] += elt2[f]
    return elt1

def merge_authors_affiliations(p):
    affiliations_name = {}

    # target authors = z_authors (from unpaywall) in priority
    target_authors = []
    target_name = ''
    for f in ['authors', 'z_authors']:
        if f in p and isinstance(p[f], list):
            target_authors = p[f]
            target_name = f

    for f in p:
        if ('affiliations' in f) and ('bso_local_affiliations' not in f) and (isinstance(p[f], list)):
            for aff in p[f]:
                name = aff['name']
                if name in affiliations_name:
                    affiliations_name[name] = merge_element(affiliations_name[name], aff)
                else:
                    affiliations_name[name] = aff

    for f in p:
         if ('authors' in f) and (isinstance(p[f], list)) and f != target_name:
            if isinstance(target_authors, list) and isinstance(p[f], list) and len(target_authors) == len(p[f]):
                for ix, aut in enumerate(p[f]):
                    if check(target_authors[ix], aut):
                        for k in aut:
                            if k not in target_authors[ix]:
                                target_authors[ix][k] = aut[k]
                            if aut.get('affiliations'):
                                current_aff = target_authors[ix].get('affiliations', [])
                                for aff in aut['affiliations']:
                                    if aff not in current_aff:
                                        current_aff.append(aff)

    if isinstance(target_authors, list) and target_authors and ('nnt' in p['id'] or len(target_authors) == 1):
        target_authors[0]['affiliations'] = list(affiliations_name.values())

    p['affiliations'] = list(affiliations_name.values())
    p['authors'] = target_authors

    return p


def enrich(publications: list, observations: list, datasource: str, affiliation_matching: bool, last_observation_date_only:bool, entity_fishing: bool, index_name='bso-publications') -> list:
    publis_dict = {}
    
    # dict of all the publis
    for p in publications:
        if 'id' not in p:
            logger.debug(f'MISSING id for publication {p} : publication skipped')
            continue
        publis_dict[p['id']] = p

    all_updated = []
    logger.debug(f'Enriching {len(publications)} publications')
    for publi_chunk in chunks(lst=publications, n=20000):
        logger.debug(f'{len(publi_chunk)} / {len(publications)} to enrich')
        
        # getting infos for the DOIs
        doi_chunk = [p.get('doi') for p in publi_chunk if p and isinstance(p.get('doi'), str)]
        data = get_doi_full(dois=doi_chunk, observations=observations, last_observation_date_only=last_observation_date_only)
        # data contains unpaywall infos (oa_details + crossref meta) => only on crossref DOIs
        
        # publis_dict contains info for all publi, even if no DOI crossref
        new_updated = format_upw(dois_infos=data, extra_data=publis_dict, entity_fishing=entity_fishing)
        for d in new_updated:
            # some post-filtering
            if d.get('publisher_group') in ['United Nations', 'World Trade Organization']:
                continue
            if d.get('genre') == 'other' and 'scanr' not in index_name:
                continue

            year = None
            try:
                year = int(d.get('year'))
            except:
                year = None
            if 'scanr' not in index_name and d.get('genre') not in ['thesis'] and year and year < 2013:
                continue
            if 'scanr' not in index_name and not isinstance(year, int):
                #logger.debug(f'year is not integer for publication { d.get("doi") }')
                continue

            # merge authors, z_authors etc 
            d = merge_authors_affiliations(d)

            d = treat_affiliations_authors(d) # useful_rank etc ...
            
            all_updated.append(d)

    # affiliation matcher
    tmp = True
    publicationsWithAffiliations = []
    if affiliation_matching:
        done, todo = get_affiliations_computed(all_updated, recompute_all = True)
        NB_PARALLEL_JOBS = 20
        PUBLI_GROUP_SIZE = 80
        # TODO TO REMOVE
        if tmp:
            logger.debug(f'affiliation matching for {len(todo)} publications')
            publis_chunks = list(chunks(lst=todo, n=PUBLI_GROUP_SIZE))
            groups = list(chunks(lst=publis_chunks, n=NB_PARALLEL_JOBS))
            for chunk in groups:
                publicationsWithAffiliations += get_matcher_parallel(chunk)
            todo = publicationsWithAffiliations
        all_updated = done + todo

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
