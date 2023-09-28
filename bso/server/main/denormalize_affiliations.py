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
        for e in ['id', 'kind', 'label', 'acronym', 'nature', 'status', 'isFrench', 'address']:
            if elt.get(e):
                res[e] = elt[e]
        orga_map[elt['id']] = res
    return orga_map

def get_orga(orga_map, orga_id):
    #data = df[df.index==orga_id].to_dict(orient='records')
    #if len(data) == 0:
    #    return None
    #elt = data[0]
    if orga_id in orga_map:
        return orga_map[orga_id]
    return {'id': orga_id}
