import redis
import requests

from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from bso.server.main.tasks import create_task_enrich, create_task_download_unpaywall, create_task_load_mongo

main_blueprint = Blueprint("main", __name__, )


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@main_blueprint.route("/forward", methods=["POST"])
def run_task_forward():
    args = request.get_json(force=True)
    print(args, flush=True)
    response_object = requests.post(args.get("url"), json=args.get("params"))
    return jsonify(response_object), 202

@main_blueprint.route("/enrich", methods=["POST"])
def run_task_enrich():
    args = request.get_json(force=True)
    print(args, flush=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("bso-publications", default_timeout=216000)
        task = q.enqueue(create_task_enrich, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/download_unpaywall", methods=["POST"])
def run_task_download_unpaywall():
    args = request.get_json(force=True)
    print(args, flush=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("bso-publications", default_timeout=21600)
        task = q.enqueue(create_task_download_unpaywall, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/load_mongo", methods=["POST"])
def run_task_load_mongo():
    args = request.get_json(force=True)
    print(args, flush=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("bso-publications", default_timeout=216000)
        task = q.enqueue(create_task_load_mongo, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("bso-publications")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
