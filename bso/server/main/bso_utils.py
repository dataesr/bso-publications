import pandas as pd
from bso.server.main.utils import FRENCH_ALPHA2
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def remove_too_long(d, publi_id, jsonfilename):
    assert(isinstance(d, dict))
    current_fields = list(d.keys())
    for f in current_fields:
        if isinstance(d[f], str):
            if len(d[f]) > 5000:
                logger.debug(f"shortening str field {f} in publi {publi_id} from {jsonfilename} as too long !")
                d[f] = d[f][0:5000]+'...'
        elif isinstance(d[f], list):
            if len(d[f]) > 50:
                if 'authors' in f:
                    new_authors = d[f][0:10]
                    for ixa, a in enumerate(d[f]):
                        keep_this_author = False
                        if ixa < 10:
                            keep_this_author = True
                        for k in ['id_hal_s', 'id_hal_i', 'idref', 'orcid']:
                            if isinstance(a.get(k), str):
                                keep_this_author = True
                                break
                        if isinstance(a.get('affiliations'), list):
                            for aff in a['affiliations']:
                                if isinstance(aff.get('detected_countries'), list):
                                    if 'fr' in aff.get('detected_countries'):
                                        keep_this_author = True
                                        break
                        if keep_this_author:
                            new_authors.append(a)
                    d[f] = new_authors
                else:
                    logger.debug(f"shortening list field {f} in publi {publi_id} from {jsonfilename} as too long !")
                    d[f] = d[f][0:50]
        elif isinstance(d[f], dict):
            d[f]=remove_too_long(d[f].copy(), publi_id, jsonfilename)
    return d

def get_ror_from_local(aff, locals_data):
    if aff in locals_data:
        if 'ror' in locals_data[aff]:
            return locals_data[aff]['ror']
        else:
            pass
            #logger.debug(f'{aff} has no ror in locals from bso-ui')
    else:
        pass
        #logger.debug(f'{aff} not in locals data from bso-ui')


def json_to_csv(json_file, observation_date, split_year = False):
    logger.debug(f'start json to csv for {json_file} split_year={split_year}')
    df = pd.read_json(json_file, lines=True, chunksize = 25000)
    output_csv_file = json_file.replace('.jsonl', '.csv')
    ix = 0
    for c in df:
        if ix == 0:
            write_header = True
        else:
            write_header = False
        pandas_to_csv(c, observation_date, output_csv_file, write_header)
        ix += 1
    logger.debug('end json to csv')
    return output_csv_file


def dict_to_csv(elem, observation_date, filename, write_header=True):
    simple_fields = ["id", "doi", "pmid", "hal_id", "year", "title", "journal_issns", "journal_issn_l",
                     "journal_name", "publisher", "publisher_dissemination", "bso_classification", "lang",
                     "genre", "amount_apc_EUR", "apc_source", "has_crossref", "has_hal_id", "missing_doi_in_hal",
                     "has_doi_in_hal", "doi_in_hal"]
    array_fields = ["domains", "detected_countries", "bso_local_affiliations", "bso_country_corrected", "rors"]
    INSIDE_FIELD_SEP = "|"
    flatten_data = []
    new_elem = {"observation_date": observation_date}
    for f in simple_fields:
        if isinstance(elem.get(f), str):
            new_elem[f] = elem[f].replace('\u2028',' ').replace('\n', ' ').replace(';', ',').replace('|', ',').replace('  ', ' ')
        elif elem.get(f):
            new_elem[f] = elem[f]
        else:
            new_elem[f] = None

    for f in array_fields:
        if isinstance(elem.get(f), list):
            new_elem[f] = INSIDE_FIELD_SEP.join(elem[f])
        else:
            new_elem[f] = None

    funding_anr, funding_europe = [], []
    grants = elem.get('grants')
    if isinstance(grants, list):
        for g in grants:
            assert(isinstance(g, dict))
            if g.get('agency') == 'ANR' and g.get('grantid'):
                funding_anr.append(g.get('grantid'))
            if g.get('agency') == 'H2020' and g.get('grantid'):
                funding_europe.append(g.get('grantid'))
    new_elem['funding_anr'] = INSIDE_FIELD_SEP.join(list(set(funding_anr)))
    new_elem['funding_europe'] = INSIDE_FIELD_SEP.join(list(set(funding_europe)))

    if isinstance(elem.get('bsso_classification'), dict) and isinstance(elem['bsso_classification'].get('field'), list):
        new_elem['bsso_classification'] = "###".join(elem['bsso_classification'].get('field'))
    else:
        new_elem['bsso_classification'] = None

    if 'oa_details' in elem and observation_date in elem['oa_details']:
        new_elem['is_oa'] = elem['oa_details'][observation_date].get('is_oa')
        new_elem['is_oa_hal'] = elem['oa_details'][observation_date].get('is_oa_hal')
        new_elem['oa_host_type'] = elem['oa_details'][observation_date].get('oa_host_type')
        if new_elem['oa_host_type']:
            new_elem['oa_host_type'] = new_elem['oa_host_type'].replace(';', '-')
        new_elem['journal_is_in_doaj'] = elem['oa_details'][observation_date].get('journal_is_in_doaj')
        new_elem['journal_is_oa'] = elem['oa_details'][observation_date].get('journal_is_oa')
        new_elem['unpaywall_oa_status'] = elem['oa_details'][observation_date].get('unpaywall_oa_status')

        new_elem['oa_colors'] = INSIDE_FIELD_SEP.join(elem['oa_details'][observation_date].get('oa_colors', []))
        new_elem['licence_publisher'] = INSIDE_FIELD_SEP.join(elem['oa_details'][observation_date].get('licence_publisher', []))
        new_elem['licence_repositories'] = INSIDE_FIELD_SEP.join(elem['oa_details'][observation_date].get('licence_repositories', []))
        new_elem['repositories'] = INSIDE_FIELD_SEP.join(elem['oa_details'][observation_date].get('repositories', []))
    else:
        for f in ['is_oa', 'is_oa_hal', 'oa_host_type', 'journal_is_in_doaj', 'journal_is_oa', 'unpaywall_oa_status',
                    'oa_colors', 'licence_publisher', 'licence_repositories', 'repositories']:
            new_elem[f] = None

    for g in ['bso3_downloaded', 'bso3_analyzed_grobid', 'bso3_analyzed_softcite', 'bso3_analyzed_datastet']:
        new_elem[g] = False
        if elem.get(g):
            new_elem[g] = elem[g]

    new_elem['software_mentions'] = None
    new_elem['data_mentions'] = None
    for g in ['software', 'data']:
        for t in ['used', 'shared', 'created']:
            new_elem[f'{g}_{t}'] = None

    if isinstance(elem.get('softcite_details'), dict):
        new_elem['software_used'] = elem['softcite_details'].get('has_used')
        new_elem['software_shared'] = elem['softcite_details'].get('has_shared')
        new_elem['software_created'] = elem['softcite_details'].get('has_created')
        mentions = elem['softcite_details'].get('mentions')
        if isinstance(mentions, list):
            new_elem['software_mentions'] = INSIDE_FIELD_SEP.join(list(set([k['name'] for k in mentions if 'name' in k])))
    else:
        for f in ['software_used', 'software_shared', 'software_created', 'software_mentions']:
            new_elem[f] = None

    if isinstance(elem.get('datastet_details'), dict):
        new_elem['data_used'] = elem['datastet_details'].get('has_used')
        new_elem['data_shared'] = elem['datastet_details'].get('has_shared')
        new_elem['data_created'] = elem['datastet_details'].get('has_created')
        mentions = elem['datastet_details'].get('mentions')
        if isinstance(mentions, list):
            new_elem['data_mentions'] = INSIDE_FIELD_SEP.join(list(set([k['name'] for k in mentions if 'name' in k])))
    else:
        for f in ['data_used', 'data_shared', 'data_created', 'data_mentions']:
            new_elem[f] = None

    if 'year' not in new_elem or not isinstance(new_elem['year'], int) or new_elem['year']<2013:
        return None

    flatten_data = [new_elem]
    final_cols = ['observation_date', 'id', 'doi', 'pmid', 'hal_id', 'year', 'title',
       'journal_issns', 'journal_issn_l', 'journal_name', 'publisher',
       'publisher_dissemination', 'bso_classification', 'lang', 'genre', 'bso_country_corrected',
       'amount_apc_EUR', 'apc_source', 'domains', 'detected_countries',
       'bso_local_affiliations', 'rors', 'funding_anr', 'funding_europe',
       'bsso_classification', 'is_oa', 'is_oa_hal', 'oa_host_type', 'journal_is_in_doaj',
       'journal_is_oa', 'unpaywall_oa_status', 'oa_colors',
       'licence_publisher', 'licence_repositories', 'repositories',
       'software_mentions', 'data_mentions',
       'software_used', 'software_created', 'software_shared',
       'data_used', 'data_created', 'data_shared', "missing_doi_in_hal", "has_doi_in_hal", "doi_in_hal"
    ]
    df_flatten = pd.DataFrame(flatten_data)
    for f in final_cols:
        if f not in list(df_flatten.columns):
            df_flatten[f]=None
    df_flatten = df_flatten[final_cols]
    df_flatten['bso_country'] = df_flatten['bso_country_corrected']
    del df_flatten['bso_country_corrected']

    if write_header:
        df_flatten.to_csv(filename, sep=';', index=False)
    else:
        df_flatten.to_csv(filename, sep=';', index=False, header=False, mode='a')


def remove_wrong_match(publi):
    publi['bso_country_corrected'] = publi.get('bso_country')
    if 'fr' not in publi.get('bso_country'):
        return publi
    if isinstance(publi.get('sources'), list):
        for source in publi.get('sources'):
            if source in ['dois_fr', 'theses', 'HAL'] or '.csv' in source or '.xls' in source:
                return publi
    bso_country = []
    for c in publi.get('bso_country'):
        if c != 'fr' and c not in bso_country:
            bso_country.append(c)
    previous_affiliations = []
    for f in publi:
        if 'affiliations' in f and isinstance(publi[f], list) and 'bso_local' not in f:
            previous_affiliations += publi[f]
    if not isinstance(previous_affiliations, list):
        return publi
    if len(previous_affiliations) == 0:
        return publi
    has_fr = False
    for aff in previous_affiliations:
        if not isinstance(aff, dict):
            continue
        previous_detected_countries = aff.get('detected_countries')
        if aff.get('country') == 'France':
            has_fr = True
            continue
        aff_name = aff.get('name')
        if not isinstance(aff_name, str) or len(aff_name)<2:
            aff['detected_countries'] = []
            #logger.debug(f"REMOVE empty affiliation for {publi.get('id')}")
            continue
        aff_name_normalized = ';'+aff_name.lower().strip() + ';'
        aff_name_normalized = aff_name_normalized.replace(' ', ';').replace(',', ';').replace('.',';').replace(';;', ';')
        if ';france;' in aff_name_normalized and ';dieu;de;france;' not in aff_name_normalized:
            has_fr = True
            continue
        for f in [';paris;', ';université;', 'orléans;', ';cnrs;', ';inserm;', ';umr;', '.fr;', ';ehess;', ';cea;', ';inra', 'lyon', 'marseille', 'bordeaux', 'nancy', 'strasbourg', 'rennes', 'lille', 'nantes', 'french', 'inrae', 'onera', 'montpellier', 'toulouse', 'clermont', 'cirad', 'avignon', ';inra;']:
            if f in aff_name_normalized:
                has_fr = True
                continue
        for w in [";saint;louis;", ";orleans;", ";mn;", ";mo;", ";mi;", ";ma;", ";mn;", ";korea;",
                  "first;author",
                  ";public;health;", ";r&d;", ";com;", ";air;", ";ill;", ";oak;", ";us;", ";liban;"]:
            if w in aff_name_normalized and isinstance(previous_detected_countries, list) and len(previous_detected_countries) > 0:
                #logger.debug(f"REMOVE {aff_name_normalized} for {publi.get('id')}")
                aff['detected_countries'] = [c for c in previous_detected_countries if c not in FRENCH_ALPHA2]
        if ';di;' in aff_name_normalized and ';e;' in aff_name_normalized and isinstance(previous_detected_countries, list) and len(previous_detected_countries) > 0:
            #logger.debug(f"REMOVE {aff_name_normalized} for {publi.get('id')}")
            aff['detected_countries'] = [c for c in previous_detected_countries if c not in FRENCH_ALPHA2]
        if ';other;paper' in aff_name_normalized and isinstance(previous_detected_countries, list) and len(previous_detected_countries) > 0:
            logger.debug(f"REMOVE {aff_name_normalized} for {publi.get('id')}")
            aff['detected_countries'] = [c for c in previous_detected_countries if c not in FRENCH_ALPHA2]
        if ';search;for' in aff_name_normalized and isinstance(previous_detected_countries, list) and len(previous_detected_countries) > 0:
            logger.debug(f"REMOVE {aff_name_normalized} for {publi.get('id')}")
            aff['detected_countries'] = [c for c in previous_detected_countries if c not in FRENCH_ALPHA2]
        if ';smart;' in aff_name_normalized and ';e;' in aff_name_normalized and isinstance(previous_detected_countries, list) and len(previous_detected_countries) > 0:
            logger.debug(f"REMOVE {aff_name_normalized} for {publi.get('id')}")
            aff['detected_countries'] = [c for c in previous_detected_countries if c not in FRENCH_ALPHA2]
    if has_fr:
        return publi
    detected_countries = []
    for aff in previous_affiliations:
        if not isinstance(aff, dict):
            logger.debug(f"{aff} is not a dict in {publi.get('id')} from {publi.get('sources')}")
            continue
        if isinstance(aff.get('detected_countries'), list):
            for c in aff.get('detected_countries'):
                if c not in detected_countries:
                    detected_countries.append(c)
    publi['detected_countries'] = detected_countries
    for c in FRENCH_ALPHA2:
        if c in detected_countries:
            bso_country.append('fr')
    publi['bso_country_corrected'] = bso_country
    return publi
