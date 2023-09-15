import fasttext
import os
import pandas as pd
import pymongo

from dateutil import parser
from typing import Union

from bso.server.main.apc.apc_detect import detect_apc
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.affiliation_matcher import enrich_publications_with_affiliations_id, get_affiliations_computed
from bso.server.main.fields.field_detect import detect_fields
from bso.server.main.hal_mongo import get_hal_history
from bso.server.main.logger import get_logger
from bso.server.main.predatory.predatory_detect import detect_predatory
from bso.server.main.publisher.publisher_detect import detect_publisher
from bso.server.main.retraction import detect_retraction 
from bso.server.main.strings import dedup_sort, normalize, normalize2, remove_punction, get_words
from bso.server.main.unpaywall_mongo import get_unpaywall_history
from bso.server.main.utils import download_file, FRENCH_ALPHA2
from bso.server.main.scanr import to_light
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
    logger.debug('Init model lang done')


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
    if genre in ['thesis']:
        return 'thesis'
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


def format_upw(dois_infos: dict, extra_data: dict, entity_fishing: bool, index_name: str, myclient) -> list:
    logger.debug('start format_upw')
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
            if 'external_ids' not in res:
                res['external_ids'] = []
            res['external_ids'].append({'id_type': 'crossref', 'id_value': doi})
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
        
        # Normalisation des editeurs
        published_year = None
        if isinstance(res.get('published_date'), str):
            published_year = res.get('published_date')[0:4]
        publisher_raw = res.get('publisher')
        if not publisher_raw:
            publisher_raw = 'unknown'
        publisher_clean = detect_publisher(publisher_raw, published_year, doi) 
        res.update(publisher_clean)

        # Retraction (must be BEFORE genre)
        if doi:
            retraction_infos = detect_retraction(doi)
            res.update(retraction_infos)
        
        # Genre (dépend de publisher_normalized)
        if isinstance(res.get('genre'), str):
            res['genre_raw'] = res['genre']
            res['genre'] = normalize_genre(res['genre'], res.get('publisher_normalized'))
        else:
            res['genre'] = 'other'
       
        domains = res.get('domains', [])
        if not isinstance(domains, list):
            domains = []
        classification_types = []
        if res['genre'] == 'thesis':
            classification_types.append('thesis')
        if 'bso' in index_name: 
            # Fields detection (dépend de genre)
            classification_types.append('bso')
            #classification_types.append('sdg')
            if 'health' in domains:
                classification_types.append('bsso')
        res = detect_fields(res, classification_types)
        
        
        info_apc = {'has_apc': None}
        if 'bso' in index_name:
            # APC
            published_date_for_apc = res.get('published_date')
            if not isinstance(published_date_for_apc, str):
                published_date_for_apc = '2100-01-01'
                #logger.debug(f"missing published date ({res.get('published_date')}) for doi {doi}, using a fallback in future for apc")
            if isinstance(doi, str) and (doi in dois_infos) and (res.get('is_paratext') == False):
                info_apc = detect_apc(doi, res.get('journal_issns'), res.get('publisher'),
                              published_date_for_apc, dois_infos[doi])
        res.update(info_apc)
        
        if 'bso' in index_name:
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
            ef_info = get_entity_fishing(res, myclient)
            if ef_info:
                res.update(ef_info)
        
        if 'bso' in index_name:
            # Predatory info
            pred_info = detect_predatory(res.get('publisher'), res.get('journal_name'))
            res.update(pred_info)
        
        # OA Details
        if isinstance(doi, str) and doi in dois_infos:
            res['observation_dates'] = []
            res['oa_details'] = {}
            last_millesime = None
            last_observation_date = None
            for asof in dois_infos[doi]:
                if asof == 'global':
                    continue
                else:
                    tmp = format_upw_millesime(dois_infos[doi][asof], asof, res['has_apc'], res['publisher_dissemination'], res['genre'])
                    res['oa_details'].update(tmp)
                    observation_date = list(tmp.keys())[0]
                    res['observation_dates'].append(observation_date)  # getting the key that is the observation date
                    if last_millesime:
                        last_millesime = max(last_millesime, asof)
                        last_observation_date = max(last_observation_date, observation_date) 
                    else:
                        last_millesime = asof
                        last_observation_date = observation_date
            #logger.debug('MILLESIME_END')
            # get hal_id if present in one of the last oa locations
            if last_millesime:
                last_oa_loc = dois_infos[doi][last_millesime].get('oa_locations', [])
                last_oa_details = res['oa_details'][last_observation_date]
                if 'hybrid' not in last_oa_details.get('oa_colors', []) and 'gold' not in last_oa_details.get('oa_colors', []):
                    # si ni gold ni hybrid '
                    res['amount_apc_EUR'] = 0
                    if res.get('has_apc') == True:
                        #logger.debug(f'{doi} should not have apc')
                        res['has_apc'] = None
                if isinstance(last_oa_loc, list):
                    for loc in last_oa_loc:
                        if loc.get('repository_normalized') == 'HAL' or 'archives-ouvertes.fr' in loc.get('url'):
                            hal_id = None
                            if isinstance(loc.get('pmh_id'), str):
                                loc_split = loc['pmh_id'].split(':')
                                if len(loc_split) > 2:
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
                                if f'hal{hal_id}' not in res['all_ids']:
                                    res['all_ids'].append(f'hal{hal_id}')
                                external_ids = res.get('external_ids', [])
                                if external_ids is None:
                                    external_ids = []
                                external_ids.append({'id_type': 'hal_id', 'id_value': hal_id})
                                res['external_ids'] = external_ids
                                if 'hal_id' not in res:
                                    res['hal_id'] = hal_id

        hal_id = res.get('hal_id')
        if isinstance(hal_id, str) and hal_id in dois_infos:
            # res['oa_details'] = {**dois_infos[hal_id], **res['oa_details']}
            current_oa_details = res.get('oa_details', {})
            hal_oa_details = dois_infos[hal_id]
            for observation_date in hal_oa_details:
                if observation_date not in current_oa_details:
                    current_oa_details[observation_date] = hal_oa_details[observation_date]
                else:
                    if current_oa_details[observation_date]['is_oa'] is False and hal_oa_details[observation_date]['is_oa'] is True:
                        current_oa_details[observation_date] = hal_oa_details[observation_date]
                    elif current_oa_details[observation_date]['is_oa'] is True and hal_oa_details[observation_date]['is_oa'] is True:
                        current_oa_details[observation_date]['repositories'] += hal_oa_details[observation_date]['repositories']
                        current_oa_details[observation_date]['repositories'] = dedup_sort(current_oa_details[observation_date]['repositories'])
                        current_oa_details[observation_date]['oa_locations'] += hal_oa_details[observation_date]['oa_locations']
            res['oa_details'] = current_oa_details

        if 'oa_details' not in res:
            pass
        else:
            for millesime in res.get('oa_details'):
                if isinstance(res['oa_details'][millesime].get('oa_colors'), str):
                    current_color = res['oa_details'][millesime].get('oa_colors')
                    res['oa_details'][millesime]['oa_colors'] = [current_color]
                if res['oa_details'][millesime].get('repositories'):
                    repos = dedup_sort(res['oa_details'][millesime].get('repositories'))
                    res['oa_details'][millesime]['repositories_concat'] = ";".join(repos)
                else:
                    res['oa_details'][millesime]['repositories_concat'] = 'closed'

                current_repo = res['oa_details'][millesime].get('repositories')
                if isinstance(current_repo, list) and 'HAL' in current_repo:
                    res['oa_details'][millesime]['is_oa_hal'] = True
                else:
                    res['oa_details'][millesime]['is_oa_hal'] = False

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

def get_author_key(a):
    author_key = None
    if normalize2(a.get('first_name'), remove_space=True) and normalize2(a.get('last_name'), remove_space=True):
        author_key = normalize2(a.get('first_name'), remove_space=True)[0]+normalize2(a.get('last_name'), remove_space=True)
    elif normalize2(a.get('full_name'), remove_space=True):
        author_key = normalize2(a.get('full_name'), remove_space=True)
    return author_key

def check_same_authors(aut1, aut2):
    aut_key_1 = get_author_key(aut1)
    aut_key_2 = get_author_key(aut2)
    if aut_key_1 is None or aut_key_2 is None:
        return False
    if aut_key_1 == aut_key_2:
        #logger.debug(f'1. assuming these two authors are the same : {aut1} and {aut2}')
        return True

    fullname1 = [w for w in normalize2(aut1.get('full_name'), min_length=3).split(' ') if w]
    fullname1.sort()
    fullname2 = [w for w in normalize2(aut2.get('full_name'), min_length=3).split(' ') if w]
    fullname2.sort()

    if len(fullname1) > 1 and len(fullname2) > 1:
        if ''.join(fullname1) == ''.join(fullname2):
            logger.debug(f'2. assuming these two authors are the same : {aut1} and {aut2}')
            return True

    return False


def merge_element(elt1, elt2):
    for f in elt2:
        if f not in elt1 and isinstance(elt2[f], str):
            elt1[f] = elt2[f]
        if isinstance(elt2[f], list):
            if f not in elt1:
                elt1[f] = []
            elt1[f] += elt2[f]
    return elt1

def merge_authors_affiliations(p, index_name):
    target_authors = []
    target_name = ''
    for f in ['authors', 'z_authors']:
        # if z_authors in object, this is the one that is kept
        if f in p and isinstance(p[f], list):
            target_authors = p[f]
            target_name = f

    affiliations = []
    for f in p:
        if ('affiliation' in f) and ('bso_local' not in f) and (isinstance(p[f], list)):
            for new_aff in p[f]:
                if new_aff not in affiliations:
                    affiliations.append(new_aff)

    # for bso no need to work on authors data
    if 'scanr' in index_name:
        for f in p:
            if ('authors' in f) and (isinstance(p[f], list)) and f != target_name:
                current_authors = p[f]
                # if list is too long, keep manual only for perf
                if len(target_authors) > 30 and 'manual' not in f:
                     continue
                if isinstance(target_authors, list) and isinstance(current_authors, list):# and len(target_authors) == len(p[f]):
                    # no check on same length, especially to get ids from manual input
                    for ix, aut in enumerate(current_authors):
                        for jx, aut_target in enumerate(target_authors):
                            if check_same_authors(aut_target, aut):
                                for k in aut:
                                    if k not in aut_target:
                                        aut_target[k] = aut[k]
                                    if aut.get('affiliations'):
                                        current_aff = aut_target.get('affiliations', [])
                                        for aff in aut['affiliations']:
                                            if aff not in current_aff:
                                                current_aff.append(aff)
                                                aut_target['affiliations'] = current_aff

    if isinstance(target_authors, list) and target_authors and ('nnt' in p['id'] or len(target_authors) == 1):
        # if thesis or single-author publication, first author has all the affiliations
        target_authors[0]['affiliations'] = affiliations
        target_authors[0]['corresponding'] = True

    p['affiliations'] = affiliations
    p['authors'] = target_authors

    return p


def enrich(publications: list, observations: list, datasource: str, affiliation_matching: bool, last_observation_date_only:bool, entity_fishing: bool, hal_date: list, index_name='bso-publications') -> list:
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
        
        # list doi
        doi_chunk = [p.get('doi') for p in publi_chunk if p and isinstance(p.get('doi'), str)]
        # get infos for the DOI, data_unpaywall contains unpaywall infos (oa_details + crossref meta) => only on crossref DOIs
        data_unpaywall = get_unpaywall_history(dois=doi_chunk, observations=observations, last_observation_date_only=last_observation_date_only)
        # list hal_id without doi
        hal_chunk = [p.get('hal_id') for p in publi_chunk if p and isinstance(p.get('hal_id'), str) and 'doi' not in p]
        # data_hal contains HAL infos (oa_details + crossref meta) => only on hal_ids
        hal_date.sort()
        #get HAL history for all dates but not the last one (already the from the extract step)
        data_hal = {}
        if len(hal_date)>1:
            data_hal = get_hal_history(hal_ids=hal_chunk, observations=hal_date[0:-1], last_observation_date_only=last_observation_date_only)
        data = {**data_hal, **data_unpaywall}

        # publis_dict contains info for all publi, even if no DOI crossref
        myclient = pymongo.MongoClient('mongodb://mongo:27017/')
        new_updated = format_upw(dois_infos=data, extra_data=publis_dict, entity_fishing=entity_fishing, index_name = index_name, myclient=myclient)
        myclient.close()

        for d in new_updated:
            # some post-filtering
            if d.get('publisher_group') in ['United Nations', 'World Trade Organization']:
                continue
            #if d.get('genre') == 'other' and 'scanr' not in index_name:
            #    continue

            year = None
            try:
                year = int(d.get('year'))
            except:
                year = None
            if 'bso-' in index_name:
                if not isinstance(year, int):
                    continue
                elif year < 2013 and d.get('genre') != 'thesis':
                    continue
            # merge authors, z_authors etc 
            d = merge_authors_affiliations(d, index_name)

            d = treat_affiliations_authors(d) # useful_rank etc ...
            d = to_light(d) 
            all_updated.append(d)

    # affiliation matcher
    compute_missing = True
    recompute_all = False
    publicationsWithAffiliations = []
    if affiliation_matching:
        done, todo = get_affiliations_computed(all_updated, recompute_all = recompute_all, compute_missing = compute_missing)
        logger.debug(f'affiliation matching for {len(todo)} publications')
        publicationsWithAffiliations += enrich_publications_with_affiliations_id(todo)
        todo = publicationsWithAffiliations
        if compute_missing is False:
            assert(len(todo) == 0)
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
