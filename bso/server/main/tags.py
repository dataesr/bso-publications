import os
import pandas as pd
from bso.server.main.logger import get_logger
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.utils import download_file

logger = get_logger(__name__)
project_id = os.getenv("OS_TENANT_ID")

data = {}


def load_ipcc_ids() -> list:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    file = "data_ipcc.zip"
    path = os.path.join(MOUNTED_VOLUME, "data_ipcc.zip")

    if not os.path.exists(path):
        download_file(
            f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/misc/{file}",
            upload_to_object_storage=False,
            destination=path,
        )

    data_ipcc = pd.read_json(path, compression="zip")
    ipcc_ids = data_ipcc["doi"].to_list()
    data["ipcc_ids"] = ipcc_ids or []


def tags_add_ipcc(tags, publication_id: str):
    if "ipcc_ids" not in data:
        load_ipcc_ids()

    if publication_id in data["ipcc_ids"]:
        tags.append("ipcc")


def add_tags(publications: list) -> list:
    logger.debug("Start add tags")

    for publication in publications:
        tags = []

        # tag ipcc publications
        tags_add_ipcc(tags, publication["id"])

        if tags:
            publication["tags"] = tags

    logger.debug("Adding tags done")

    return publications
