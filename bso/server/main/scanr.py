import json
import pymongo
import os
import pandas as pd

import dateutil.parser

from bso.server.main.logger import get_logger
from bso.server.main.strings import normalize2

logger = get_logger(__name__)
    

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

def to_scanr(publications):
    scanr_publications = []
    for p in publications:
        elt = {'id': p['id']}
        if p.get('title'):
            elt['title'] = {'default': p['title']}
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
        # identifiers
        if isinstance(p.get('doi'), str):
            elt['doiUrl'] = f"http://doi.org/{p['doi']}"
        external_ids = []
        for idi in p.get('all_ids', []):
            if idi[0:3] == 'doi':
                external_ids.append({'type': 'scanr', 'id': idi[3:]})
                external_ids.append({'type': 'doi', 'id': idi[3:]})
            if idi[0:3] == 'hal':
                external_ids.append({'type': 'hal', 'id': idi[3:]})
            if idi[0:4] == 'pmid':
                external_ids.append({'type': 'pmid', 'id': idi[4:]})
            if idi[0:3] == 'nnt':
                external_ids.append({'type': 'nnt', 'id': idi[3:]})
        if external_ids:
            elt['externalIds'] = external_ids
        # dates
        for f_date in ['published_date', 'publication_date']:
            if p.get(f_date) and isinstance(p[f_date], str):
                elt['publicationDate'] = dateutil.parser.parse(p[f_date]).isoformat()
                elt['year'] = int(elt['publicationDate'][0:4])
                break
        # genre
        if isinstance(p.get('genre'), str):
            elt['type'] = p['genre']
        else:
            elt['type'] = 'other'
        if p.get('genre') == 'thesis':
            elt['productionType'] = 'thesis'
        else:
            elt['productionType'] = 'publication'
        # journal
        source = {}
        if p.get('journal_name'):
            source['title'] = p['journal_name']
        if p.get('publisher_dissemination'):
            source['publisher'] = p['publisher_dissemination']
        if p.get('journal_issns'):
            source['journalIssns'] = str(p['journal_issns']).split(',')
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
            source['isOa'] = p['oa_details'][last_obs_date].get('journal_is_oa', False)
            source['isInDoaj'] = p['oa_details'][last_obs_date].get('journal_is_in_doaj', False)
            oa_evidence = {}
            for oaloc in p['oa_details'][last_obs_date].get('oa_locations', []):
                if oaloc.get('is_best'):
                    oa_evidence['hostType'] = oaloc.get('host_type')
                    oa_evidence['version'] = oaloc.get('version')
                    oa_evidence['license'] = oaloc.get('license')
                    oa_evidence['url'] = oaloc.get('url')
                    oa_evidence['pdfUrl'] = oaloc.get('url_for_pdf')
                    oa_evidence['landingPageUrl'] = oaloc.get('url_for_landing_page')
                for f in ['hostType', 'version', 'license', 'url', 'pdfUrl', 'landingPageUrl']:
                    if f in oa_evidence and oa_evidence[f] is None:
                        del oa_evidence[f]
            if oa_evidence:
                elt['oaEvidence'] = oa_evidence
            if source:
                elt['source'] = source      
        # domains
        domains = []
        if isinstance(p.get('classifications'), list):
            for c in p['classifications']:
                if c.get('label'):
                    domain = {'label': {'default': c['label']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
                    domains.append(domain)
                if c.get('label_fr'):
                    domain = {'label': {'default': c['label_fr']}}
                    domain['code'] = str(c.get('code'))
                    domain['type'] = c.get('reference')
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
                if k.get('keyword'):
                    keywords.append(k['keyword'])
                    domains.append({'label': {'default': k['keyword']}, 'type': 'keyword'})
        if keywords:
            elt['keywords'] = {'default': keywords}
        if domains:
            elt['domains'] = domains
        # grants
        projects = []
        if isinstance(p.get('grants'), list):
            for g in p['grants']:
                if g.get('grantid'):
                    projects.append(g['grantid'])
                if g.get('id'):
                    projects.append(g['id'])
        if projects:
            elt['projects'] = projects
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
                if isinstance(aff.get('grid'), list):
                    for x in aff['grid']:
                        if x not in affiliations:
                            affiliations.append(x)
        #data from local bso
        if isinstance(p.get('bso_local_affiliations'), list):
            for aff in p['bso_local_affiliations']:
                if aff not in affiliations:
                    affiliations.append(aff)
        if affiliations:
            elt['affiliations'] = affiliations
        ## authors
        authors=[]
        if isinstance(p.get('authors'), list):
            for a in p['authors']:
                author = {}
                if a.get('first_name'):
                    author['firstName'] = a['first_name']
                if a.get('last_name'):
                    author['lastName'] = a['last_name']
                if a.get('full_name'):
                    author['fullName'] = a['full_name']
                if a.get('id'):
                    author['person'] = a['id']
                affiliations = []
                if isinstance(a.get('affiliations'), list):
                    for aff in a['affiliations']:
                        if isinstance(aff.get('ids'), list):
                            for x in aff['ids']:
                                if x.get('id'):
                                    affiliations.append(x['id'])
                if isinstance(a.get('grid'), list):
                    for x in a['grid']:
                        if x not in affiliations:
                            affiliations.append(x)
                author['role'] = a.get('role', 'author')
                if author['role'][0:3] == 'aut':
                    author['role'] = 'author'
                if affiliations:
                    author['affiliations'] = affiliations
                if author:
                    authors.append(author)
            if authors:
                elt['authors'] = authors
        elt = {f: elt[f] for f in elt if elt[f]==elt[f] }
        if elt:
            scanr_publications.append(elt)
    return scanr_publications

