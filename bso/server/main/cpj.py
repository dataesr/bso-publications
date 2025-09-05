import os
import ast
import pandas as pd
from bso.server.main.logger import get_logger
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.utils import download_file

logger = get_logger(__name__)
project_id = os.getenv("OS_TENANT_ID")

cpj_dict = {}

def load_cpj_data():
    global cpj_dict
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    file = "cpj.csv"
    path = os.path.join(MOUNTED_VOLUME, file)

    if not os.path.exists(path):
        download_file(
            f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/misc/{file}",
            upload_to_object_storage=False,
            destination=path,
        )

    data_cpj = pd.read_csv(path).to_dict(orient='records')
    cpj_dict = {}
    for d in data_cpj:
        if d.get('tags') and isinstance(d['tags'], str):
            d['tags'] = ast.literal_eval(d['tags'])
        cpj_dict[d['idref']] = d
    return cpj_dict

def get_cpj(idref):
    global cpj_dict
    if len(cpj_dict) == 0:
        load_cpj_data()
    if idref in cpj_dict:
        return cpj_dict[idref]
    return {}
