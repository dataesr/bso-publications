import os
import fasttext
import neattext.functions as nfx
from bso.server.main.logger import get_logger
from bso.server.main.config import MOUNTED_VOLUME
from bso.server.main.utils import download_file

logger = get_logger(__name__)
project_id = os.getenv("OS_TENANT_ID")

TEDS_MODELS_FILES = {
    "ipcc": "fasttext_model_teds_20241107.bin",
    "ipcc_wg": "fasttext_model_teds_wg_20241106.bin",
    "ipbes": "fasttext_model_teds_ipbes_20240601.bin",
}

teds_models = {}


def fasttext_load_from_file(model_file: str) -> any:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    model_path = os.path.join(MOUNTED_VOLUME, model_file)

    if not os.path.exists(model_path):
        download_file(
            f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/models/{model_file}",
            upload_to_object_storage=False,
            destination=model_path,
        )
    return fasttext.load_model(model_path)


def teds_init_models():
    for model_name, model_file in TEDS_MODELS_FILES.items():
        logger.debug(f"Init model {model_name}")
        teds_models[model_name] = fasttext_load_from_file(model_file)
        logger.debug(f"Init model {model_name} done")


def teds_get_publication_input(publication):
    title = publication["title"] if isinstance(publication.get("title"), str) else ""
    journal_name = publication["journal_name"] if isinstance(publication.get("journal_name"), str) else ""
    journal_issns = publication["journal_issns"] if isinstance(publication.get("journal_issns"), str) else ""

    topics = []
    for topic in publication.get("topics", []):
        name = topic.get("display_name")
        subfield = topic.get("subfield", {}).get("display_name")
        if name:
            topics.append(name)
        if subfield:
            topics.append(subfield)
    topics = " ".join(set(topics))

    input = f"{title} {topics} {journal_name} {journal_issns}"
    input_clean = nfx.remove_stopwords(input.lower().replace("\n", ""))

    return input_clean


def teds_predictions(input):
    predict_teds = []

    # Predict IPCC
    ipcc_predictions = teds_models["ipcc"].predict(input, k=1)
    ipcc_label = ipcc_predictions[0][0].replace("__label__", "")
    ipcc_probability = ipcc_predictions[1][0]
    predict_teds.append({"label": ipcc_label, "probability": ipcc_probability})

    # Predict IPCC working groups
    if ipcc_label == "ipcc":
        wg_predictions = teds_models["ipcc_wg"].predict(input, k=-1, threshold=0.5)
        for wg_label, wg_probability in zip(*wg_predictions):
            wg_label = "ipcc_" + wg_label.replace("__label__", "")
            predict_teds.append({"label": wg_label, "probability": wg_probability})

    # Predict IPBES
    ipbes_predictions = teds_models["ipbes"].predict(input, k=1)
    ipbes_label = ipbes_predictions[0][0].replace("__label__", "")
    ipbes_probability = ipbes_predictions[1][0]
    predict_teds.append({"label": ipbes_label, "probability": ipbes_probability})

    return predict_teds


def add_teds_predictions(publications):
    if not teds_models:
        teds_init_models()

    logger.debug("Start predict teds")

    for publication in publications:
        input = teds_get_publication_input(publication)
        predict_teds = teds_predictions(input)

        if predict_teds:
            publication["predict_teds"] = predict_teds

    logger.debug("Predict teds done")

    return publications
