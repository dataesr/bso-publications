import datetime
import os

from bso.server.main.affiliation_matcher import filter_publications_by_country
from bso.server.main.elastic import load_in_es
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import get_objects

PV_MOUNT = '/upw_data'
logger = get_logger(__name__)


def create_task_enrich(args: dict) -> list:
    publications = args.get('publications', [])
    return enrich(publications=publications)


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
    asof = args.get('asof', 'nodate')  # if nodate, today's snapshot will be used
    filename = download_snapshot(asof).split('/')[-1]
    logger.debug(f'Filename after download is {filename}')
    path = f'{PV_MOUNT}/{filename}'
    if os.path.exists(path=path):
        snapshot_to_mongo(f=path, global_metadata=True, delete_input=False)
        snapshot_to_mongo(f=path, global_metadata=False, delete_input=False)
        logger.debug(f'Deleting file {path}')
        os.remove(path)


def create_task_etl(args: dict) -> None:
    index = 'bso-publications'
    publications = get_objects(container='pubmed', path=f'enriched/2021/03/22/enriched_20210322.json.gz')
    filtered_publications = filter_publications_by_country(publications=publications, country='fr')
    enriched_publications = enrich(publications=filtered_publications)
    load_in_es(data=enriched_publications, index=index)
