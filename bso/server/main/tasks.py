import datetime
import os

from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo

PV_MOUNT = '/upw_data'


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    return enrich(publications)


def create_task_download_unpaywall(args: dict) -> str:
    snap = None
    if args.get('type') == 'snapshot':
        snap = download_snapshot(asof=args.get('asof'))
    elif args.get('type') == 'daily':
        today = datetime.date.today()
        snap = download_daily(f'{today}')
    return snap


def create_task_load_mongo(arg: dict) -> None:
    asof = arg.get('asof')
    if asof:
        global_metadata = arg.get('global_metadata', False)
        upload_to_object_storage = not global_metadata
        filename = download_snapshot(asof, upload_to_object_storage=upload_to_object_storage).split('/')[-1]
        print(f'filename after download is {filename}', flush=True)
        for f in os.listdir(PV_MOUNT):
            if f == filename:
                snapshot_to_mongo(f'{PV_MOUNT}/{f}', global_metadata=global_metadata)
