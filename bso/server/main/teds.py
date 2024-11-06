from bso.server.main.logger import get_logger
import fasttext
import os

from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.utils import download_file

logger = get_logger(__name__)

project_id = os.getenv("OS_TENANT_ID")

teds_models = {}
ipcc_model_file = "fasttext_model_teds_20241106.bin"


def init_model_ipcc() -> None:
    logger.debug("Init model IPCC")
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    ipcc_model_name = f"{MOUNTED_VOLUME}{ipcc_model_file}"
    if not os.path.exists(ipcc_model_name):
        download_file(
            f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/models/{ipcc_model_file}",
            upload_to_object_storage=False,
            destination=ipcc_model_name,
        )
    ipcc_model = fasttext.load_model(ipcc_model_name)
    teds_models["ipcc"] = ipcc_model
    logger.debug("Init model IPCC done")


def add_predict_ipcc(publications):
    if "ipcc" not in teds_models:
        init_model_ipcc()

    logger.debug("Start predict ipcc")

    for p in publications:
        title = p["title"] if p.get("title") and isinstance(p["title"], str) else ""
        journal_name = p["journal_name"] if p.get("journal_name") and isinstance(p["journal_name"], str) else ""
        journal_issns = p["journal_issns"] if p.get("journal_issns") and isinstance(p["journal_issns"], str) else ""
        topics = []
        for topic in p.get("topics", []):
            name = topic.get("display_name")
            subfield = topic.get("subfield", {}).get("display_name")
            if name:
                topics.append(name)
            if subfield:
                topics.append(subfield)
        topics = " ".join(set(topics))

        input = f"{title} {topics} {journal_name} {journal_issns}"
        predictions = teds_models["ipcc"].predict(input, k=-1, threshold=0.5)

        ipcc_predictions = []
        for label, probability in zip(*predictions):
            label = label.replace("__label__", "")
            ipcc_predictions.append({"label": label, "probability": probability, "type": "ipcc"})

        if ipcc_predictions:
            if p.get("predict_teds"):
                p["predict_teds"].append(ipcc_predictions)
            else:
                p.update({"predict_teds": ipcc_predictions})

    logger.debug("End predict ipcc")

    return publications
