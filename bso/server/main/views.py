import redis
import requests
import os

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from bso.server.main.logger import get_logger
from bso.server.main.tasks import create_task_download_unpaywall, create_task_enrich, \
    create_task_load_mongo, create_task_unpaywall_to_crawler, create_task_et, create_task_cache_affiliations, create_task_load_collection_from_object_storage
from bso.server.main.utils import dump_to_object_storage
#from bso.server.main.extract_transform import load_scanr_publications, upload_sword
from bso.server.main.zotero import make_file_ANR
from bso.server.main.extra_treatment import compute_extra
from bso.server.main.genre_these import compute_genre
from bso.server.main.etl import finalize

default_timeout = 43200000
logger = get_logger(__name__)
main_blueprint = Blueprint('main', __name__, )

PUBLIC_API_PASSWORD = os.getenv('PUBLIC_API_PASSWORD')

@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route("/load_collection_from_object_storage", methods=["POST"])
def run_task_load_collection_from_object_storage():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue('bso-publications', default_timeout=216000)
        logger.debug('Starting task load collection from object storage')
        logger.debug(args)
        task = q.enqueue(create_task_load_collection_from_object_storage, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202


@main_blueprint.route("/extra", methods=["POST"])
def run_extra():
    args = request.get_json(force=True)
    queue = args.get('queue')
    if queue is None:
        return
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(queue, default_timeout=216000)
        task = q.enqueue(compute_extra, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/genre_these", methods=["POST"])
def run_genre_these():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue('scanr-publications', default_timeout=216000)
        task = q.enqueue(compute_genre, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route('/zotero', methods=['POST'])
def run_task_zotero():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='zotero', default_timeout=default_timeout)
        task = q.enqueue(make_file_ANR, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


#@main_blueprint.route('/upload_sword', methods=['POST'])
#def run_task_upload_sword():
#    args = request.get_json(force=True)
#    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
#    index_name = args.get('index')
#    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
#        q = Queue(name='scanr-publications', default_timeout=default_timeout)
#        task = q.enqueue(upload_sword, index_name)
#    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
#    return jsonify(response_object), 202

#@main_blueprint.route('/load_scanr_publications', methods=['POST'])
#def run_task_load_scanr_publications():
#    args = request.get_json(force=True)
#    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
#    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
#        q = Queue(name='scanr-publications', default_timeout=default_timeout)
#        task = q.enqueue(load_scanr_publications, args)
#    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
#    return jsonify(response_object), 202


@main_blueprint.route('/forward', methods=['POST'])
def run_task_forward():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
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


@main_blueprint.route('/update_daily', methods=['GET'])
def update_daily():
    is_daily = True
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='unpaywall_to_crawler', default_timeout=default_timeout)
        task = q.enqueue(create_task_unpaywall_to_crawler, is_daily)
    response_object = {
        'status': 'success',
        'data': {
            'task_id': task.get_id()
        }
    }
    return jsonify(response_object)

@main_blueprint.route('/update_weekly', methods=['GET'])
def update_weekly():
    is_daily = False
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='unpaywall_to_crawler', default_timeout=default_timeout)
        task = q.enqueue(create_task_unpaywall_to_crawler, is_daily)
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
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_enrich, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/download_unpaywall', methods=['POST'])
def run_task_download_unpaywall():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_download_unpaywall, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/load_mongo', methods=['POST'])
def run_task_load_mongo():
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_load_mongo, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications')
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


@main_blueprint.route('/cache', methods=['POST'])
def run_task_cache():
    logger.debug('Starting task cache')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    queue = args.get('queue', 'scanr-publications')
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name=queue, default_timeout=default_timeout)
        task = q.enqueue(create_task_cache_affiliations, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/et', methods=['POST'])
def run_task_et():
    logger.debug('Starting task et')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    queue = args.get('queue', 'bso-publications')
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name=queue, default_timeout=default_timeout)
        task = q.enqueue(create_task_et, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route('/et_scanr', methods=['POST'])
def run_task_et_scanr():
    logger.debug('Starting task et scanr')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    queue = args.get('queue', 'scanr-publications')
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name=queue, default_timeout=default_timeout)
        task = q.enqueue(create_task_et, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/finalize', methods=['POST'])
def run_task_finalize_scanr():
    logger.debug('Starting task finalize scanr')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    queue = args.get('queue', 'scanr-publications')
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name=queue, default_timeout=default_timeout)
        task = q.enqueue(finalize, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/et_bso_all', methods=['POST'])
def run_task_et_bso_all():
    logger.debug('Starting task et bso all')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    args_extract = args.copy()
    args_extract['reset_file'] = True
    args_extract['extract'] = True
    extract_task = []
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_et, args_extract)
        extract_task.append(task)
    response_extract_object = {'status': 'success', 'data': {'task_id': task.get_id()}}

    args_transform = args.copy()
    args_transform['reset_file'] = False
    args_transform['extract'] = False
    args_transform['transform'] = True

    transform_tasks = []
    for idx in range(0, 8):
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(name='bso-publications', default_timeout=default_timeout)
            current_args_transform = args_transform.copy()
            current_args_transform['split_idx'] = idx
            task = q.enqueue(create_task_et, current_args_transform, depends_on=extract_task)
            transform_tasks.append(task)
            response_extract_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    
    args_finalize = args.copy()
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(finalize, args_finalize, depends_on=transform_tasks)
        extract_task.append(task)
    response_extract_object = {'status': 'success', 'data': {'task_id': task.get_id()}}

    return jsonify(response_extract_object), 202

@main_blueprint.route('/et_scanr_all', methods=['POST'])
def run_task_et_scanr_all():
    logger.debug('Starting task et scanr all')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    args_extract = args.copy()
    args_extract['reset_file'] = True
    args_extract['extract'] = True
    args_extract['transform'] = False
    args_extract['transform_scanr'] = False
    extract_task = []
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='scanr-publications', default_timeout=default_timeout)
        task = q.enqueue(create_task_et, args_extract)
        extract_task.append(task)
    response_extract_object = {'status': 'success', 'data': {'task_id': task.get_id()}}

    args_transform = args.copy()
    args_transform['reset_file'] = False
    args_transform['extract'] = False
    args_transform['transform'] = True
    if args.get('transform_scanr') is True:
        args_transform['transform_scanr'] = True
    else:
        args_transform['transform_scanr'] = False
    
    for idx in range(0, 9):
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(name='scanr-publications', default_timeout=default_timeout)
            current_args_transform = args_transform.copy()
            current_args_transform['split_idx'] = idx
            task = q.enqueue(create_task_et, current_args_transform, depends_on=extract_task)
            response_extract_object = {'status': 'success', 'data': {'task_id': task.get_id()}}

    return jsonify(response_extract_object), 202


@main_blueprint.route('/dump', methods=['POST'])
def run_task_dump():
    logger.debug('Starting task dump')
    args = request.get_json(force=True)
    assert(args.get('PUBLIC_API_PASSWORD') == PUBLIC_API_PASSWORD)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='bso-publications', default_timeout=default_timeout)
        task = q.enqueue(dump_to_object_storage, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202
