import datetime
import os

from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo

PV_MOUNT = "/upw_data"


def create_task_enrich(arg):
    publis = arg.get('publications', [])
    return enrich(publis)


def create_task_download_unpaywall(arg):
    snap = None
    if arg.get('type') == "snapshot":
        snap = download_snapshot(asof=arg.get('asof'))
    elif arg.get('type') == "daily":
        today = datetime.date.today()
        snap = download_daily(f"{today}")
    return snap


def create_task_load_mongo(arg):
    if arg.get('asof'):
        asof = arg.get('asof')
        global_metadata = arg.get("global_metadata", False)
        upload_to_object_storage = True
        if global_metadata:
            upload_to_object_storage = False
        filename = download_snapshot(asof, upload_to_object_storage=upload_to_object_storage).split('/')[-1]
        print(f"filename after download is {filename}", flush=True)
        for f in os.listdir(PV_MOUNT):
            if f == filename:
                snapshot_to_mongo(f"{PV_MOUNT}/{f}", global_metadata=global_metadata)
