import os
import pandas as pd
from bso.server.main.logger import get_logger
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.utils import download_file

logger = get_logger(__name__)
project_id = os.getenv("OS_TENANT_ID")

data = {}


def load_ipcc_data():
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    file = "ipcc_ipbes_chapters.jsonl"
    path = os.path.join(MOUNTED_VOLUME, file)

    if not os.path.exists(path):
        download_file(
            f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/misc/{file}",
            upload_to_object_storage=False,
            destination=path,
        )

    data_ipcc = pd.read_json(path, lines=True, orient="records")
    data_ipcc = data_ipcc.set_index("doi")["ipcc"].to_dict()
    data["ipcc"] = data_ipcc


def tags_add_ipcc(tags: list, publication):
    if "ipcc" not in data:
        load_ipcc_data()

    publication_id = publication["id"]
    publication_predict_teds = publication.get("predict_teds", [])
    publication_tags = []

    # Add tags for ipcc publications
    if publication_id in data["ipcc"] and data["ipcc"].get(publication_id):

        publication_tags.append({"id": "ipcc", "label": {"fr": "giec", "en": "ipcc", "default": "ipcc"}})

        ipcc_wgs = []
        for ipcc_chapter in data["ipcc"][publication_id]:
            ipcc_wg = ipcc_chapter[0:3]
            if ipcc_wg not in ipcc_wgs:
                ipcc_wgs.append(ipcc_wg)
                publication_tags.append(
                    {
                        "id": f"ipcc_{ipcc_wg}",
                        "label": {"fr": f"giec_{ipcc_wg}", "en": f"ipcc_{ipcc_wg}", "default": f"ipcc_{ipcc_wg}"},
                    }
                )
            publication_tags.append(
                {
                    "id": f"ipcc_{ipcc_chapter}",
                    "label": {"fr": f"giec_{ipcc_chapter}", "en": f"ipcc_{ipcc_chapter}", "default": f"ipcc_{ipcc_chapter}"},
                }
            )


    # Add tags for ipcc prediction
    for predict in publication_predict_teds:
        if predict.get("label", "").startswith("ipcc"):
            publication_tags.append({
                "id": f"predict_{predict["label"]}",
                "label": {"default": f"predict_{predict["label"]}"}
            })
            
    tags.extend(publication_tags)

def add_tags(publications: list) -> list:
    logger.debug("Start add tags")

    for publication in publications:
        tags = []

        # tag ipcc publications
        tags_add_ipcc(tags, publication)

        if tags:
            publication["tags"] = tags

    logger.debug("Adding tags done")

    return publications
