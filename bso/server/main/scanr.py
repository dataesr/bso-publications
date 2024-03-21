import json
import pymongo
import os
import itertools
import pandas as pd

import dateutil.parser
from retry import retry

from bso.server.main.logger import get_logger
from bso.server.main.strings import normalize2
from bso.server.main.utils import clean_json, is_valid
from bso.server.main.denormalize_affiliations import get_orga, get_project
from bso.server.main.fields.field_detect import get_embeddings
from bso.server.main.utils_swift import delete_object

logger = get_logger(__name__)

NB_MAX_AUTHORS = 50
MIN_YEAR_PUBLISHED = 1960

NB_MAX_CO_ELEMENTS = 20

idref_sudoc_only = {}
vip_dict = {}

def clean_sudoc_extra(p):
    if 'sudoc' not in p['id']:
        return
    authors = p.get('authors', [])
    all_sudoc_only = True
    if isinstance(authors, list):
        for a in authors:
            if a.get('id'):
                current = analyze_sudoc(a['id'])
                if current is False:
                    all_sudoc_only = False
    if all_sudoc_only:
        sudoc_id = p['id'].replace('sudoc', '').upper()
        return f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json'
        #delete_object('sudoc', f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')
    return None


@retry(delay=200, tries=3)
def get_publications_for_idref(idref):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'publi_meta'
    mycoll = mydb[collection_name]
    res = []
    cursor = mycoll.find({ 'authors.person' : { '$in': [idref] } })
    for r in cursor:
        del r['_id']
        res.append(r)
    cursor.close()
    myclient.close()
    return res

def analyze_sudoc(idref):
    global idref_sudoc_only
    if idref not in idref_sudoc_only:
        publications = get_publications_for_idref(idref)
        sudoc_only = True
        for e in publications:
            if 'sudoc' not in e['id']:
                sudoc_only=False
                break
        idref_sudoc_only[idref] = sudoc_only
    return idref_sudoc_only[idref]

def to_light(p):
    for f in ['references']:
        if f in p:
            del p[f]
    authors = p.get('authors')
    if isinstance(authors, list) and len(authors)>NB_MAX_AUTHORS:
        other_authors_keys = [e for e in list(p.keys()) if (('authors_' in e) or ('_authors' in e))]
        for f in other_authors_keys:
            del p[f]
    return p

@retry(delay=200, tries=3)
def get_matches_for_publication(publi_ids):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['scanr']
    collection_name = 'person_matcher_output'
    mycoll = mydb[collection_name]
    res = list(mycoll.find({ 'publication_id' : { '$in': publi_ids } }))
    data = {}
    for r in res:
        publi_id = r.get('publication_id')
        author_key = r.get('author_key')
        person_id = r.get('person_id')
        if publi_id and author_key and person_id:
            data[f'{publi_id};{author_key}'] = person_id
    myclient.close()
    return data

def get_person_ids(publications):
    publi_ids = [e['id'] for e in publications]
    matches = get_matches_for_publication(publi_ids)
    for p in publications:
        publi_id = p.get('id')
        authors = p.get('authors')
        if not isinstance(authors, list):
            continue
        for a in authors:
            author_key = None
            if normalize2(a.get('first_name'), remove_space=True) and normalize2(a.get('last_name'), remove_space=True):
                author_key = normalize2(a.get('first_name'), remove_space=True)[0]+normalize2(a.get('last_name'), remove_space=True)
            elif normalize2(a.get('full_name'), remove_space=True):
                author_key = normalize2(a.get('full_name'), remove_space=True)
            publi_author_key = f'{publi_id};{author_key}'
            if publi_author_key in matches:
                res = matches[publi_author_key]
                a['id'] = res
    return publications

def fix_patents(patents):
    for i_p, patent in enumerate(patents):
        patent['id'] = str(patent['id'])
        for field_to_fix in ['title', 'summary']:
            if isinstance(patent.get(field_to_fix), list) and patent[field_to_fix]:
                patents[i_p][field_to_fix] = patent[field_to_fix][0]
        subpatents = patent.get('patents')
        if isinstance(subpatents, list):
            for j_p, subpatent in enumerate(subpatents):
                if 'pulicationDate' in subpatent:
                    subpatents[j_p]['publicationDate'] = subpatent['pulicationDate']
                    del subpatents[j_p]['pulicationDate']
            patents[i_p]['patents'] = subpatents
    return patents

def to_scanr_patents(patents, df_orga, denormalize=False):
    res = []
    for patent in patents:
        if denormalize:
            denormalized_affiliations = []
            affiliations = patent.get('affiliations', [])
            if affiliations:
                for aff in affiliations:
                    denormalized = get_orga(df_orga, aff)
                    if denormalized:
                        denormalized_affiliations.append(denormalized)
            if denormalized_affiliations:
                patent['affiliations'] = denormalized_affiliations
        res.append(patent)

    return res

def get_vip_dict():
    global vip_dict
    assert(len(vip_dict) == 0)
    df_vip = pd.read_json('/upw_data/vip.jsonl', lines=True)
    for ix, row in df_vip.iterrows():
        current_idref = 'idref'+row['idref']
        elt = {}
        for f in ['lastName', 'firstName', 'orcid', 'id_hal']:
            if isinstance(row[f], str):
                elt[f] = row[f]
        if elt.get('lastName') and elt.get('firstName'):
            elt['fullName'] = elt['firstName'] + ' ' + elt['lastName']
        for f in ['prizes']:
            if isinstance(row[f], list):
                elt[f] = row[f]
        assert(current_idref not in vip_dict)
        vip_dict[current_idref] = elt
    logger.debug(f'vip dict loaded with {len(vip_dict)} idrefs')
    return


def to_scanr(publications, df_orga, df_project, denormalize = False):
    global vip_dict
    if len(vip_dict)==0:
        get_vip_dict()
    scanr_publications = []
    for p in publications:
        text_to_autocomplete =[]
        elt = {'id': p['id']}
        text_to_autocomplete.append(p['id'])
        title_lang = None
        if 'lang' in p and isinstance(p['lang'], str) and len(p['lang'])==2:
            title_lang = p['lang']
        if p.get('title') and isinstance(p['title'], str) and len(p['title'])>2:
            elt['title'] = {'default': p['title']}
            if title_lang:
                elt['title'][title_lang] = p['title']
                text_to_autocomplete.append(p['title'])
        else:
            continue
        #field abstract / abstracts 
        abstracts = []
        if isinstance(p.get('abstracts'), list):
            abstracts = p['abstracts']
        elif isinstance(p.get('abstract'), list):
            abstracts = p['abstract']
        for ix, abstr in enumerate(abstracts):
            current_abs = None
            current_lang = ''
            if isinstance(abstr, dict):
                current_abs = abstr.get('abstract')
                current_lang = abstr.get('lang', '')
            elif isinstance(abstr, str):
                current_abs = abstr
            if current_abs is None:
                continue
            if 'summary' not in elt:
                elt['summary'] = {'default': current_abs}
            if current_lang[0:2] == 'fr':
                elt['summary']['fr'] = current_abs
            if current_lang[0:2] == 'en':
                elt['summary']['en'] = current_abs
        landingPage = None
        pdfUrl = None
        # identifiers
        if isinstance(p.get('doi'), str):
            elt['doiUrl'] = f"http://doi.org/{p['doi']}"
            landingPage = elt['doiUrl']
        external_ids = []
        for idi in p.get('all_ids', []):
            if idi[0:3] == 'doi':
                currentId = idi[3:]
                external_ids.append({'type': 'doi', 'id': currentId})
            if idi[0:3] == 'hal':
                external_ids.append({'type': 'hal', 'id': idi[3:]})
                if landingPage is None:
                    landingPage = f"https://hal.science/{idi[3:]}"
            if idi[0:4] == 'pmid':
                external_ids.append({'type': 'pmid', 'id': idi[4:]})
            if idi[0:3] == 'nnt':
                external_ids.append({'type': 'nnt', 'id': idi[3:]})
        if external_ids:
            elt['externalIds'] = external_ids
            for ext_id in external_ids:
                text_to_autocomplete.append(ext_id['id'])
        # dates
        if p.get('year') and p['year']==p['year']:
            elt['year'] = int(p['year'])
        else:
            logger.debug(f"no year for {p['id']}")
        for f_date in ['published_date', 'publication_date', 'defense_date']:
            if p.get(f_date) and isinstance(p[f_date], str):
                elt['publicationDate'] = dateutil.parser.parse(p[f_date]).isoformat()
                elt['year'] = int(elt['publicationDate'][0:4])
                break
        # genre
        for e in p.get('all_ids'):
            if e[0:3] == 'nnt':
                p['genre'] = 'these'
                landingPage = f"https://theses.fr/{e[3:]}"
            if e[0:7]=='haltel-':
                p['genre'] = 'these'
        if isinstance(p.get('genre'), str):
            elt['type'] = p['genre']
            if p['genre'] == 'these':
                elt['type'] = 'thesis'
        else:
            elt['type'] = 'other'
        if elt.get('type') == 'thesis':
            elt['productionType'] = 'thesis'
        else:
            elt['productionType'] = 'publication'
        # journal
        source = {}
        autocompletedJournal = []
        autocompletedPublisher = []
        if p.get('journal_name') and isinstance(p['journal_name'], str) and p['journal_name'] not in ['unknown']:
            source['title'] = p['journal_name']
            autocompletedJournal.append(p['journal_name'])
        if p.get('publisher_dissemination') and isinstance(p['publisher_dissemination'], str) and p['publisher_dissemination'] not in ['unknown']:
            source['publisher'] = p['publisher_dissemination']
            autocompletedPublisher.append(p['publisher_dissemination'])
        if p.get('journal_issns') and isinstance(p['journal_issns'], str):
            source['journalIssns'] = str(p['journal_issns']).split(',')
            autocompletedJournal += source['journalIssns']
        if denormalize:
            elt['autocompletedJournal'] = autocompletedJournal
            elt['autocompletedPublisher'] = autocompletedPublisher
        # OA
        elt['isOa'] = False
        if p.get('oa_details') and isinstance(p['oa_details'], dict):
            last_obs_date = max(p['oa_details'].keys())
            if isinstance(p['oa_details'][last_obs_date], list):
                logger.debug(f"oadetails for {p['id']} is not a dict")
                p['oa_details'][last_obs_date] = p['oa_details'][last_obs_date][0]
            if not isinstance(p['oa_details'][last_obs_date], dict):
                logger.debug(f"oadetails not a dict for {p['id']}")
                logger.debug(p['oa_details'][last_obs_date])
                continue
            elt['isOa'] = p['oa_details'][last_obs_date].get('is_oa', False)
            if source:
                source['isOa'] = p['oa_details'][last_obs_date].get('journal_is_oa', False)
                source['isInDoaj'] = p['oa_details'][last_obs_date].get('journal_is_in_doaj', False)
            oa_evidence = {}
            for loc_ix, oaloc in enumerate(p['oa_details'][last_obs_date].get('oa_locations', [])):
                # is_best from UPX or first location
                if oaloc.get('is_best') or loc_ix == 0:
                    oa_evidence['hostType'] = oaloc.get('host_type')
                    oa_evidence['version'] = oaloc.get('version')
                    oa_evidence['license'] = oaloc.get('license')
                    oa_evidence['url'] = oaloc.get('url')
                    oa_evidence['pdfUrl'] = oaloc.get('url_for_pdf')
                    pdfUrl = oa_evidence['pdfUrl']
                    if pdfUrl is None:
                        pdfUrl = oa_evidence['url']
                    oa_evidence['landingPageUrl'] = oaloc.get('url_for_landing_page')
                for f in ['hostType', 'version', 'license', 'url', 'pdfUrl', 'landingPageUrl']:
                    if f in oa_evidence and oa_evidence[f] is None:
                        del oa_evidence[f]
            if oa_evidence:
                elt['oaEvidence'] = oa_evidence

        
        elt['source'] = source      
        
        # domains
        domains = []
        if isinstance(p.get('classifications'), list):
            for c in p['classifications']:
                if c.get('label'):
                    domain = {'label': {'default': c['label']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
                    if domain['type'] in ['wikidata', 'sudoc']:
                        domains.append(domain)
                if c.get('label_fr'):
                    domain = {'label': {'default': c['label_fr']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
                    if domain['type'] in ['wikidata', 'sudoc']:
                        domains.append(domain)
        if isinstance(p.get('hal_classifications'), list):
            for c in p['hal_classifications']:
                if c.get('label'):
                    domain = {'label': {'default': c['label']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = 'HAL'
                    domains.append(domain)
        if isinstance(p.get('thematics'), list):
            for c in p['thematics']:
                if c.get('fr_label'):
                    domain = {'label': {'default': c.get('fr_label')}}
                    domain['code'] = c.get('code')
                    domain['type'] = c.get('reference')
        if p.get('bso_classification') and isinstance(p.get('bso_classification'), str):
            domains.append({'label': {'default': p['bso_classification']}, 'type': 'bso_classification'})
        if isinstance(p.get('bsso_classification'), dict) and isinstance(p['bsso_classification'].get('field'), str):
            domains.append({'label': {'default': p['bsso_classification']['field']}, 'type': 'bsso_classification'})
        #if p.get('sdg_classification'):
        #    domains.append({'label': {'default': p['bso_classification']}, 'type': 'bso_classification'})
        # keywords
        keywords = []
        if isinstance(p.get('keywords'), list):
            for k in p['keywords']:
                if k.get('keyword') and k['keyword'] not in keywords:
                    keywords.append(k['keyword'])
                    domains.append({'label': {'default': k['keyword']}, 'type': 'keyword'})
        if keywords:
            elt['keywords'] = {'default': keywords}
        if domains:
            elt['domains'] = domains
        if denormalize:
            map_code = {}
            for d in domains:
                if 'code' in d:
                    code = d.get('code')
                else:
                    code = d.get('label', {}).get('default', '')
                if isinstance(d.get('code'), str) and isinstance(d.get('label', {}).get('default'), str):
                    d['id_name'] = f"{d['code']}###{d['label']['default']}"
                domain_key = normalize2(d.get('label', {}).get('default', '').lower(), remove_space=True)
                if code not in map_code:
                    map_code[code] = d
                    map_code[code]['count'] = 1
                    map_code[code]['naturalKey'] = domain_key
                else:
                    map_code[code]['count'] += 1
            domains_with_count = list(map_code.values())
            domains_with_count = sorted(domains_with_count, key=lambda x:x['count'], reverse=True)
            if domains_with_count:
                elt['domains'] = domains_with_count
        # grants
        projects = []
        if isinstance(p.get('grants'), list):
            for g in p['grants']:
                if g.get('grantid'):
                    projects.append(g['grantid'])
                if g.get('id'):
                    projects.append(g['id'])
        if projects:
            elt['projects'] = list(set(projects))
        # affiliations
        affiliations = []
        if isinstance(p.get('affiliations'), list):
            for aff in p['affiliations']:
                #data from matcher
                if isinstance(aff.get('ids'), list):
                    for x in aff['ids']:
                        if x.get('id'):
                            affiliations.append(x['id'])
                #data scraped
                for t in ['grid', 'rnsr', 'ror', 'sirene', 'siren', 'siret']:
                    if isinstance(aff.get(t), list):
                        for x in aff[t]:
                            if x not in affiliations:
                                affiliations.append(x)
                    if isinstance(aff.get(t), str) and aff[t] not in affiliations:
                        affiliations.append(aff[t])
        #data from local bso
        if isinstance(p.get('bso_local_affiliations'), list):
            for aff in p['bso_local_affiliations']:
                if aff not in affiliations:
                    affiliations.append(aff)
        if affiliations:
            elt['affiliations'] = list(set(affiliations))
        
        denormalized_affiliations_dict = {} 
        if denormalize:
            elt['landingPage'] = landingPage
            elt['pdfUrl'] = pdfUrl
            # orga
            denormalized_affiliations = []
            affiliations = elt.get('affiliations')
            if isinstance(affiliations, list) and len(affiliations) > 0:
                for aff in affiliations:
                    denormalized = get_orga(df_orga, aff)
                    if denormalized and (denormalized not in denormalized_affiliations):
                        denormalized_affiliations.append(denormalized)
                        assert(isinstance(denormalized, dict))
            if denormalized_affiliations:
                elt['affiliations'] = denormalized_affiliations
                for aff in denormalized_affiliations:
                    denormalized_affiliations_dict[aff['id']] = aff

        ## authors
        authors=[]
        if isinstance(p.get('authors'), list):
            nb_authors = len([a for a in p['authors'] if a.get('role', 'author')[0:3] == 'aut'])
            elt['authorsCount'] = nb_authors
            for ix_aut, a in enumerate(p['authors']):
                # max auteurs
                if ix_aut > NB_MAX_AUTHORS:
                    continue
                author = {}
                potentialFullName = ''
                if a.get('first_name'):
                    author['firstName'] = a['first_name']
                    potentialFullName += a['first_name']
                if a.get('last_name'):
                    author['lastName'] = a['last_name']
                    potentialFullName += ' '+ a['last_name']
                    potentialFullName = potentialFullName.strip()
                if a.get('full_name'):
                    author['fullName'] = a['full_name']
                elif potentialFullName:
                    author['fullName'] = potentialFullName
                isFrench = 'NOT_FR'
                affiliations_ids = []
                raw_affiliations = []
                if isinstance(a.get('affiliations'), list):
                    raw_affiliations = a['affiliations']
                    for aff in a['affiliations']:
                        if isinstance(aff.get('ids'), list):
                            for x in aff['ids']:
                                if x.get('id'):
                                    affiliations_ids.append(x['id'])
                        for t in ['grid', 'ror', 'rnsr', 'sirene', 'siren', 'siret']:
                            if isinstance(aff.get(t), list):
                                for x in aff[t]:
                                    if x not in affiliations_ids:
                                        affiliations_ids.append(x)
                            if isinstance(aff.get(t), str) and aff[t] not in affiliations_ids:
                                affiliations_ids.append(aff[t])
                        if isinstance(aff.get('name'), str) and len(aff['name'])>3000:
                            del aff['name']
                        for aff_id in affiliations_ids:
                            if aff_id in denormalized_affiliations_dict and denormalized_affiliations_dict[aff_id].get('isFrench', False):
                                isFrench = 'FR'
                if a.get('id'):
                    author['person'] = a['id']
                    fullName = author.get('fullName', 'NO_FULLNAME')
                    if denormalize:
                        author['denormalized'] = {'id': a['id']}
                        if a['id'] in vip_dict:
                            extra_info = vip_dict[a['id']]
                            if 'fullName' in extra_info:
                                fullName = extra_info.get('fullName')
                            if 'firstName' in extra_info and 'lastName' in extra_info:
                                fullName = f"{extra_info['firstName']} {extra_info['lastName']}"
                            for f in ['orcid', 'id_hal']:
                                if extra_info.get(f):
                                    author['denormalized'][f] = extra_info[f]
                        author['id_name'] = a['id']+'###'+fullName+'###'+isFrench
                author['role'] = a.get('role', 'author')
                if author['role'][0:3] == 'aut':
                    author['role'] = 'author'
                    # si monoauteur => affiliations publi = affiliation auteur
                    if nb_authors == 1:
                        for x in elt.get('affiliations', []):
                            if x not in affiliations:
                                affiliations.append(x)
                if affiliations and (denormalize == False):
                    author['affiliations'] = affiliations
                if raw_affiliations and denormalize:
                    author['affiliations'] = raw_affiliations
                if author and (isinstance(author.get('fullName'), str) or isinstance(author.get('lastName'), str)):
                    authors.append(author)
            if authors:
                elt['authors'] = authors
                if 'sudoc' in elt['id']:
                    all_sudoc_only = True
                    for a in authors:
                        if a.get('person'):
                            current = analyze_sudoc(a['person'])
                            if current is False:
                                all_sudoc_only = False
                    if all_sudoc_only:
                        sudoc_id = elt['id'].replace('sudoc', '')
                        elt['year'] = None
                        delete_object('sudoc', f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')


        if denormalize:

            if text_to_autocomplete:
                elt['autocompleted'] = text_to_autocomplete
            # projects
            denormalized_projects = []
            projects = elt.get('projects')
            if isinstance(projects, list) and len(projects) > 0:
                for aff in projects:
                    denormalized = get_project(df_project, aff)
                    if denormalized and (denormalized not in denormalized_projects):
                        denormalized_projects.append(denormalized)
                        assert(isinstance(denormalized, dict))
            if denormalized_projects:
                elt['projects'] = denormalized_projects

            if isinstance(p.get('softcite_details'), dict) and isinstance(p['softcite_details'].get('raw_mentions'), list):
                softwares = {}
                for raw_m in p['softcite_details'].get('raw_mentions'):
                    if 'software-name' in raw_m:
                        current_key = None
                        if 'normalizedForm' in raw_m['software-name']:
                            current_key = raw_m['software-name']['normalizedForm']
                        elif 'rawForm' in raw_m['software-name']:
                            current_key = raw_m['software-name']['rawForm']
                        current_key = normalize_software(current_key)
                        if current_key:
                            if current_key not in softwares:
                                softwares[current_key] = {'softwareName': current_key, 'contexts':[], 'id_name': f'{current_key}###NO_ID'}
                                if 'wikidataId' in raw_m:
                                    softwares[current_key]['wikidata'] = raw_m['wikidataId']
                                    softwares[current_key]['id_name'] = f"{current_key}###{raw_m['wikidataId']}"
                            if 'context' in raw_m:
                                softwares[current_key]['contexts'].append(raw_m['context'])
                if softwares:
                    elt['softwares'] = list(softwares.values())
            
            # embeddings
            # TODO remove
            if 'embeddings' not in p and elt.get('year') and elt['year'] >= 2019 and 'doi' in elt['id']:
                p['embeddings'] = get_embeddings(p)
            if isinstance(p.get('embeddings'), list):
                elt['vector_text'] = p['embeddings']
            
            # for network mapping
            # authors network
            if authors:
                try:
                    co_authors = get_co_occurences([a for a in authors if (a.get('role') == 'author')], 'id_name')
                except:
                    logger.debug(authors)
                    co_authors = get_co_occurences([a for a in authors if (a.get('role') == 'author')], 'id_name')
                if co_authors:
                    elt['co_authors'] = co_authors
            # affiliations network
            if denormalized_affiliations:
                structures_to_combine = [a for a in denormalized_affiliations if ('Structure de recherche' in a.get('kind', []))]
                co_structures = get_co_occurences(structures_to_combine, 'id_name')
                if co_structures:
                    elt['co_structures'] = co_structures
                institutions_to_combine = [a for a in denormalized_affiliations if ('Structure de recherche' not in a.get('kind', []))]
                co_institutions = get_co_occurences(institutions_to_combine, 'id_name')
                if co_institutions:
                    elt['co_institutions'] = co_institutions
            # wikidata network
            if domains:
                domains_to_combine = [a for a in domains if ((a.get('type') == 'wikidata') and (a.get('count', 0) > 1))]
                co_domains = get_co_occurences(domains_to_combine, 'id_name')
                if co_domains:
                    elt['co_domains'] = co_domains
            # software from softcite
            if elt.get('softwares'):
                co_software = get_co_occurences(elt['softwares'], 'id_name')
                if co_software:
                    elt['co_software'] = co_software
        elt = clean_json(elt)
        if elt:
            if elt.get('year') and elt['year'] < MIN_YEAR_PUBLISHED:
                continue
            else:
                scanr_publications.append(elt)
    return scanr_publications

def get_co_occurences(my_list, my_field):
    elts_to_combine = [a for a in my_list if a.get(my_field)]
    if len(elts_to_combine) <= NB_MAX_CO_ELEMENTS:
        combinations = list(set(itertools.combinations([a[my_field] for a in elts_to_combine], 2)))
        res = [f'{a}---{b}' for (a,b) in combinations]
        return res
    return None

def normalize_software(s):
    if s.lower().strip() in ['script', 'scripts']:
        return 'scripts'
    return s.capitalize()
    
