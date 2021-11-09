import os
import requests

from bso.server.main.logger import get_logger

SCIENTIFIC_TAGGER_SERVICE = os.getenv('SCIENTIFIC_TAGGER_SERVICE')

logger = get_logger(__name__)


def detect_fields(a_publication):
    for classif_type in ['bso', 'bsso', 'sdg']:
        r_classif = requests.post(f'{SCIENTIFIC_TAGGER_SERVICE}/classify_one', json={'publications': [a_publication],
                                                                                     'type': classif_type})
        try:
            a_publication = r_classif.json().get('publications')[0]
        except:
            logger.debug(f'Error in classif {classif_type} : {r_classif.text}')
    return a_publication
