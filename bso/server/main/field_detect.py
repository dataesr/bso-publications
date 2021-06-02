import requests
import os
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

SCIENTIFIC_TAGGER_SERVICE = os.getenv("SCIENTIFIC_TAGGER_SERVICE")

def detect_fields(a_publication):
    for classif_type in ['bso', 'bsso']:
        r_classif = requests.post(f"{SCIENTIFIC_TAGGER_SERVICE}/classify_one", json={"publications": [a_publication], "type": classif_type})
        try:
            a_publication = r_classif.json().get('publications')[0]
        except:
            logger.debug("error in classif {classif_type}")
            logger.debug(r_classif.text)
    return a_publication
