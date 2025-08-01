import pandas as pd
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def get_default_name(e):
    if not isinstance(e, dict):
        return None
    for f in ['default', 'en', 'fr']:
        if isinstance(e.get(f), str):
            return e[f]
    return None

def get_name_by_lang(e, lang):
    assert(lang in ['fr', 'en'])
    if not isinstance(e, dict):
        return None
    if isinstance(e.get(lang), str):
        return e[lang]
    return None

def compute_is_french(elt_id, mainAddress):
    isFrench = True
    if 'grid' in elt_id or 'ror' in elt_id:
        isFrench = False
        if isinstance(mainAddress, dict) and isinstance(mainAddress.get('country'), str) and mainAddress['country'].lower().strip() == 'france':
            isFrench = True
    return isFrench

def compute_country(elt_id, mainAddress):
    if isinstance(mainAddress, dict) and isinstance(mainAddress.get('country'), str):
        return mainAddress.get('country')
    elif 'grid' not in elt_id and 'ror' not in elt_id:
        return 'France'
    return None


def get_main_address(address):
    main_add = None
    for add in address:
        if add.get('main', '') is True:
            main_add = add.copy()
            break
    if main_add:
        for f in ['main', 'citycode', 'urbanUnitCode', 'urbanUnitLabel', 'localisationSuggestions', 'provider', 'score']:
            if main_add.get(f):
                del main_add[f]
    return main_add

def get_orga_data():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    orga_map = {}
    for elt in data:
        res = {}
        gps=''
        for e in ['id', 'label', 'acronym', 'kind', 'level', 'status']:
            if elt.get(e) and (isinstance(elt[e], str) or isinstance(elt[e], dict) or isinstance(elt[e], list)):
                res[e] = elt[e]
            if isinstance(elt.get('address'), list):
                res['mainAddress'] = get_main_address(elt['address'])
                if res['mainAddress']:
                    gps = res['mainAddress'].get('gps', {})
                    if gps:
                        lat = gps.get('lat')
                        lon = gps.get('lon')
                        if lat and lon:
                            gps=f'{lat}|{lon}'
        res['isFrench'] = compute_is_french(elt['id'], res.get('mainAddress'))
        res['country'] = compute_country(elt['id'], res.get('mainAddress'))
        if 'label' not in res:
            continue
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
        acronym_str = ''
        if isinstance(res.get('acronym'), dict):
            acronym_str = get_default_name(res['acronym'])
        if not isinstance(acronym_str, str):
            acronym_str = default_label
        res['id_name'] += f'###{acronym_str}'
        res['id_name'] += f'###{gps}'
        orga_map[elt['id']] = res
    return orga_map

def get_orga(orga_map, orga_id):
    if orga_id in orga_map:
        return orga_map[orga_id]
    return {'id': orga_id}

def get_correspondance():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    correspondance = {}
    raw_rnsrs = data
    for r in raw_rnsrs:
        current_id = None
        externalIdsToKeep = [e for e in r.get('externalIds', []) if e['type'] in ['rnsr',  'ror', 'grid', 'bce', 'sirene', 'siren', 'siret'] ]
        for e in externalIdsToKeep:
            current_id = e['id']
            if current_id not in correspondance:
                correspondance[current_id] = []
        if current_id is None:
            continue

        correspondance[current_id] += [k for k in externalIdsToKeep if k['id'] != current_id]
        for e in r.get('externalIds', []):
            if e['type'] in ['siren', 'siret', 'sirene', 'bce']:
                new_id = e['id']
                correspondance[new_id] += [k for k in externalIdsToKeep if k['id'] != new_id]

        for e in r.get('institutions'):
            if e.get('structure'):
                if isinstance(e.get('relationType'), str) and 'tutelle' in e['relationType'].lower():
                    elt = {'id': e['structure'], 'type': 'siren'}
                    if elt not in correspondance[current_id]:
                        correspondance[current_id].append(elt)
    logger.debug(f'{len(correspondance)} ids loaded with equivalent ids')
    return correspondance




def get_projects_data():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz'
    df = pd.read_json(url, lines=True)
    data = df.to_dict(orient='records')
    proj_map = {}
    for elt in data:
        res = {}
        for e in ['id', 'label', 'acronym', 'type', 'year']:
            if elt.get(e):
                res[e] = elt[e]
        if 'label' not in res:
            continue
        default = get_default_name(res['label'])
        if default is None:
            continue
        res['id_name'] = f"{elt['id']}###{default}"
        proj_map[elt['id']] = res
    return proj_map

def get_project(proj_map, proj_id):
    if proj_id in proj_map:
        return proj_map[proj_id]
    return {'id': proj_id}
