import os
import requests
import time

from bso.server.main.logger import get_logger


AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/enrich_with_affiliations_id'


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
def get_matcher_results(publications: list) -> list:
    r = requests.post(matcher_endpoint_url, json={'publications': publications
                                                  'queue': 'matcher_short'})
    task_id = r.json()['data']['task_id']
    logger.debug(f'New task {task_id} for matcher')
    for i in range(0, 10000):
        r_task = requests.get(f'{AFFILIATION_MATCHER_SERVICE}/tasks/{task_id}').json()
        try:
            status = r_task['data']['task_status']
        except:
            logger.error(f'Error in getting task {task_id} status : {r_task}')
            status = 'error'
        if status == 'finished':
            return r_task['data']['task_result']
        elif status in ['started', 'queued']:
            time.sleep(2)
            continue
        else:
            logger.error(f'Error with task {task_id} : status {status}')
            return []
