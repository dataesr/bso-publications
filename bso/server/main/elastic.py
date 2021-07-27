from elasticsearch import Elasticsearch, helpers

from bso.server.main.logger import get_logger
from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL

client = None
logger = get_logger(__name__)


def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            logger.error(f'{func.__name__} raises an error through decorator "exception_handler".')
            logger.error(exception)
            return None
    return inner_function


@exception_handler
def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK))
    return client


@exception_handler
def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


@exception_handler
def update_alias(alias: str, old_index: str, new_index: str) -> None:
    es = get_client()
    logger.debug(f'updating alias {alias} from {old_index} to {new_index}')
    response = es.indices.update_aliases({
        'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}
        ]
    })
    logger.debug(response)


@exception_handler
def reset_index(index: str) -> None:
    es = get_client()
    delete_index(index)
    response = es.indices.create(
        index=index,
        body={'settings': {}, 'mappings': {}},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')


@exception_handler
def load_in_es(data: list, index: str) -> None:
    es = get_client()
    actions = [{'_index': index, '_source': datum} for datum in data]
    for success, info in helpers.parallel_bulk(client=es, actions=actions, chunk_size=500, request_timeout=60):
        if not success:
            logger.debug(f'A document failed: {info}')
    logger.debug(f'{len(data)} elements imported into {index}')
