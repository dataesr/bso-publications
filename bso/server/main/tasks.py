import datetime
import os

from bso.server.main.affiliation_matcher import filter_publications_by_country
from bso.server.main.elastic import load_in_es
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo

PV_MOUNT = '/upw_data'
logger = get_logger(__name__)


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    french_publications = filter_publications_by_country(publications=publications)
    return enrich(publications=french_publications)


def create_task_download_unpaywall(args: dict) -> str:
    if args.get('type') == 'snapshot':
        snap = download_snapshot(asof=args.get('asof'))
    elif args.get('type') == 'daily':
        today = datetime.date.today()
        snap = download_daily(date=f'{today}')
    else:
        snap = None
    return snap


def create_task_load_mongo(args: dict) -> None:
    asof = args.get('asof')
    if asof:
        filename = download_snapshot(asof).split('/')[-1]
        logger.debug(f'Filename after download is {filename}')
        for f in os.listdir(PV_MOUNT):
            if f == filename:
                #snapshot_to_mongo(f=f'{PV_MOUNT}/{f}', global_metadata=True, delete_input=False)
                snapshot_to_mongo(f=f'{PV_MOUNT}/{f}', global_metadata=False, delete_input=True)


def create_task_etl(args: dict) -> None:
    index = 'bso-publications'
    publications = args.get('publications', [])
    french_publications = filter_publications_by_country(publications=publications)
    data = enrich(publications=french_publications)
    load_in_es(data=data, index=index)
