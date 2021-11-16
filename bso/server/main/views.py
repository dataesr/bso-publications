import redis
import requests

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from bso.server.main.logger import get_logger
from bso.server.main.tasks import create_task_download_unpaywall, create_task_enrich, create_task_etl, \
    create_task_load_mongo, create_task_unpaywall_to_crawler
from bso.server.main.utils import dump_to_object_storage

default_timeout = 43200000
logger = get_logger(__name__)
main_blueprint = Blueprint('main', __name__, )


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/forward', methods=['POST'])
def run_task_forward():
    args = request.get_json(force=True)
    method = args.get('method', 'POST')
    if method.upper() == 'GET':
        response = requests.get(args.get('url'), timeout=1000)
    else:
        response = requests.post(args.get('url'), json=args.get('params'))
    try:
        response_object = response.json()
    except:
        logger.error('Response is not a valid json')
        logger.error(response.text)
        response_object = {}
    return jsonify(response_object), 202


@main_blueprint.route('/update_weekly', methods=['GET'])
def update_weekly():
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('unpaywall_to_crawler', default_timeout=default_timeout)
        task = q.enqueue(create_task_unpaywall_to_crawler)
    response_object = {
        'status': 'success',
        'data': {
            'task_id': task.get_id()
        }
    }
    return jsonify(response_object)


@main_blueprint.route('/enrich', methods=['POST'])
def run_task_enrich():
    logger.debug('Starting task enrich')
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_enrich, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/download_unpaywall', methods=['POST'])
def run_task_download_unpaywall():
    args = request.get_json(force=True)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_download_unpaywall, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/load_mongo', methods=['POST'])
def run_task_load_mongo():
    args = request.get_json(force=True)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_load_mongo, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications')
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id(),
                'task_status': task.get_status(),
                'task_result': task.result,
            }
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)


@main_blueprint.route('/etl', methods=['POST'])
def run_task_etl():
    logger.debug('Starting task etl')
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_etl, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/dump', methods=['GET'])
def run_task_dump():
    logger.debug('Starting task dump')
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('bso-publications', default_timeout=default_timeout)
        task = q.enqueue(dump_to_object_storage)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202
