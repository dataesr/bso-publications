import pandas as pd
import requests
from urllib.parse import urlencode
from bso.server.main.utils import EXCLUDED_ID
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def get_main_id(current_id, correspondance):
    if current_id in correspondance:
        for c in correspondance[current_id]:
            if c.get('main_id'):
                return c['main_id']
    return current_id

def get_correspondance():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations-v2.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[~df.id.isin(EXCLUDED_ID)]
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    correspondance = {}
    raw_rnsrs = data
    for r in raw_rnsrs:
        current_id = None
        externalIdsToKeep = [e for e in r.get('externalIds', []) if e['type'] in ['rnsr',  'ror', 'grid', 'bce', 'sirene', 'siren', 'siret', 'paysage', 'uai'] ]
        for e in externalIdsToKeep:
            e['main_id'] = r['id']
            current_id = e['id']
            if current_id not in correspondance:
                correspondance[current_id] = []
        if current_id is None:
            continue
        correspondance[current_id] += [k for k in externalIdsToKeep]
        for e in r.get('externalIds', []):
            if e['type'] in ['siren', 'siret', 'sirene', 'bce', 'grid', 'ror', 'bce', 'paysage', 'uai']:
                new_id = e['id']
                correspondance[new_id] += [k for k in externalIdsToKeep]
        if isinstance(r.get('institutions'), list):
            for e in r.get('institutions'):
                if isinstance(e, dict):
                    if e.get('structure'):
                        if isinstance(e.get('relationType'), str) and 'tutelle' in e['relationType'].lower():
                            elt = {'id': e['structure'], 'type': 'siren'}
                            if elt not in correspondance[current_id]:
                                correspondance[current_id].append(elt)
    logger.debug(f'{len(correspondance)} ids loaded with equivalent ids')
    return correspondance

def get_main_address(address):
    main_add = None
    if not isinstance(address, list):
        return main_add
    for add in address:
        if add.get('main', '') is True:
            main_add = add.copy()
            break
    if main_add:
        for f in ['main', 'citycode', 'urbanUnitCode', 'urbanUnitLabel', 'provider', 'score']:
            if main_add.get(f):
                del main_add[f]
    return main_add

def get_name_by_lang(e, lang):
    assert(lang in ['fr', 'en'])
    if not isinstance(e, dict):
        return None
    if isinstance(e.get(lang), str):
        return e[lang]
    return None

def get_default_name(e):
    if not isinstance(e, dict):
        return None
    for f in ['default', 'fr', 'en']:
        if isinstance(e.get(f), str):
            return e[f]
    return None

def compute_is_french_deprecated(elt_id, mainAddress):
    isFrench = True
    if 'grid' in elt_id or 'ror' in elt_id:
        isFrench = False
        if isinstance(mainAddress, dict) and isinstance(mainAddress.get('country'), str) and mainAddress['country'].lower().strip() == 'france':
            isFrench = True
    return isFrench

def get_orga_list():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations-v2.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[~df.id.isin(EXCLUDED_ID)]
    data = df.to_dict(orient='records')
    return data

def get_panel_erc(elt):
    if isinstance(elt.get('activities'), list):
        for a in elt['activities']:
            if a.get('type') == 'panel_erc':
                return a
    return  {}

def get_orga_map():
    data = get_orga_list()
    orga_map = {}
    for elt in data:
        res = {}
        data_to_encode = {}
        for e in ['id', 'kind', 'label', 'acronym', 'status', 'isFrench', 'is_main_parent', 'typologie_1', 'typologie_2', 'level']:
            if elt.get(e):
                res[e] = elt[e]
        for k in ['id', 'typologie_1', 'typologie_2']:
            if elt.get(k):
                data_to_encode[k] = elt[k]
        panel_erc= get_panel_erc(elt)
        if panel_erc:
            res['panel_erc'] = panel_erc
            data_to_encode['panel_erc'] = panel_erc
        if isinstance(elt.get('address'), list):
            res['mainAddress'] = get_main_address(elt['address'])
            if isinstance(res['mainAddress'], dict):
                if isinstance(res['mainAddress'].get('country'), str):
                    data_to_encode['country'] = res['mainAddress'].get('country')
                if isinstance(res['mainAddress'].get('city'), str):
                    data_to_encode['city'] = res['mainAddress'].get('city')
                if isinstance(res['mainAddress'].get('region'), str):
                    data_to_encode['region'] = res['mainAddress'].get('region')
                for k in ['gps', 'postcode', 'address']:
                    if k in res['mainAddress']:
                        del res['mainAddress'][k]
        if res.get('status') == 'valid':
            res['status'] = 'active'
        assert(res.get('status') in ['active', 'old'])
        if 'label' in res:
            fr_label = get_name_by_lang(res['label'], 'fr')
            en_label = get_name_by_lang(res['label'], 'en')
            default_label = get_default_name(res['label'])
            encoded_labels = []
            if fr_label:
                encoded_labels.append('FR_'+fr_label)
            if en_label:
                encoded_labels.append('EN_'+en_label)
            encoded_label = '|||'.join(encoded_labels)
            if len(encoded_labels)==0 and default_label:
                encoded_label = 'DEFAULT_' + default_label
            res['id_name'] = f"{elt['id']}###{encoded_label}"
            if default_label:
                data_to_encode['label'] = default_label
                res['id_name_default'] = f"{elt['id']}###{default_label}"
            elif fr_label:
                res['id_name_default'] = f"{elt['id']}###{fr_label}"
            if en_label:
                res['id_name_default'] = f"{elt['id']}###{en_label}"
        else:
            logger.debug('No Label ???')
            logger.debug(res)
        res['encoded_key'] = urlencode(data_to_encode)
        orga_map[elt['id']] = res
    return orga_map

def get_orga(orga_map, orga_id):
    if orga_id in orga_map:
        return orga_map[orga_id]
    return {'id': orga_id}

def get_projects_data():
    #url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz'
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects-v2.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[df.type!='Casdar']
    data = df.to_dict(orient='records')
    proj_map = {}
    for elt in data:
        res = {}
        for e in ['id', 'label', 'acronym', 'type', 'year']:
            if elt.get(e):
                res[e] = elt[e]
        proj_map[elt['id']] = res
    return proj_map

def get_link_orga_projects(corresp):
    #url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz'
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects-v2.jsonl.gz'
    df = pd.read_json(url, lines=True)
    df = df[df.type!='Casdar']
    data = df.to_dict(orient='records')
    proj_map = {}
    for elt in data:
        res = {}
        for e in ['id', 'label', 'acronym', 'type', 'year']:
            if elt.get(e):
                res[e] = elt[e]
        proj_map[elt['id']] = res
    map_orga_proj = {}
    for proj in data:
        proj_id = proj['id']
        if isinstance(proj.get('participants'), list):
            for part in proj.get('participants'):
                if isinstance(part, dict):
                    if part.get('structure'):
                        orga_id = get_main_id(part['structure'], corresp)
                        if orga_id not in map_orga_proj:
                            map_orga_proj[orga_id] = []
                        current_proj = proj_map[proj_id]
                        map_orga_proj[orga_id].append(current_proj)
    return map_orga_proj

def get_project_from_orga(map_orga_proj, orga_id):
    if orga_id in map_orga_proj:
        return map_orga_proj[orga_id]
    return []

def get_project(proj_map, proj_id):
    if proj_id in proj_map:
        return proj_map[proj_id]
    return {'id': proj_id}
