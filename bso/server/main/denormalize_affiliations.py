import pandas as pd
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def get_orga_data():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    orga_map = {}
    for elt in data:
        res = {}
        #for e in ['id', 'kind', 'label', 'acronym', 'nature', 'status', 'isFrench', 'address']:
        for e in ['id', 'label', 'acronym', 'address']:
            if elt.get(e):
                res[e] = elt[e]
        orga_map[elt['id']] = res
    return orga_map

def get_orga(orga_map, orga_id):
    if orga_id in orga_map:
        return orga_map[orga_id]
    return {'id': orga_id}

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
        proj_map[elt['id']] = res
    return proj_map

def get_project(proj_map, proj_id):
    if proj_id in proj_map:
        return proj_map[proj_id]
    return {'id': proj_id}
