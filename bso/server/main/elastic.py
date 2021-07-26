from elasticsearch import Elasticsearch, helpers

from bso.server.main.logger import get_logger
from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL

client = None
logger = get_logger(__name__)


def get_client():
    global client
    if client is not None:
        return client
    try:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK))
        return client
    except:
        logger.debug('Cannot connect to es')


client = get_client()


def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


def update_alias(alias: str, old_index: str, new_index: str) -> None:
    es = get_client()
    logger.debug(f'updating alias {alias} from {old_index} to {new_index}')
    res = es.indices.update_aliases({
        'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}
        ]
    })
    logger.debug(res)


def reset_index(index: str) -> None:
    es = get_client()
    try:
        delete_index(index)
    except:
        logger.debug('Index deletion failed')
    response = es.indices.create(
        index=index,
        body={'settings': {}, 'mappings': {}},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response:
        if response['acknowledged']:
            response = str(response['index'])
            logger.debug(f'Index mapping success for index: {response}')


def load_in_es(data: list, index: str) -> None:
    es = get_client()
    actions = [{'_index': index, '_source': datum} for datum in data]
    for success, info in helpers.parallel_bulk(client=es, actions=actions, chunk_size=500, request_timeout=60):
        if not success:
            logger.debug(f'A document failed: {info}')
    logger.debug(f'{len(data)} elements imported into {index}')
