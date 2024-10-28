from bso.server.main.logger import get_logger
import fasttext

logger = get_logger(__name__)

model_teds = fasttext.load_model("fasttext/fasttext_model_teds.bin")


def add_predict_ipcc(publications):
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
