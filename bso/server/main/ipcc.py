from bso.server.main.logger import get_logger
import fasttext
import os

logger = get_logger(__name__)


model_path = "/src/bso/server/main/fasttext/fasttext_model_teds.bin"


def fasttext_load():
    try:
        return fasttext.load_model(model_path)
    except Exception as error:
        logger.error(error)
        return None


model_teds = fasttext_load()


def add_predict_ipcc(publications):

    if model_teds is None:
        logger.warn("predict_ipcc model not loaded: abort prediction")
        return publications

    logger.debug("predict_ipcc_with_fasttext")

    for p in publications:
        title = p.get("title", "")
        source = p.get("journal_name", "")
        topics = []
        for topic in p.get("topics", []):
            name = topic.get("display_name")
            subfield = topic.get("subfield", {}).get("display_name")
            if name:
                topics.append(name)
            if subfield:
                topics.append(subfield)
        topics = list(set(topics))

        input = f"{title} {source} {', '.join(topics)}"
        prediction = model_teds.predict(input, k=1)[0][0]

        if prediction:
            p.update({"predict_ipcc": prediction})

    return publications
