import datetime
import os
import dateutil.parser

from bso.server.main.affiliation_matcher import filter_publications_by_country
from bso.server.main.elastic import load_in_es, reset_index
from bso.server.main.logger import get_logger
from bso.server.main.unpaywall_enrich import enrich
from bso.server.main.unpaywall_feed import download_daily, download_snapshot, snapshot_to_mongo
from bso.server.main.utils_swift import get_objects_by_prefix

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
    index = args.get('index', 'bso-publications')
    logger.debug(f"reset index {index}")
    reset_index(index=index)
    start_string = args.get('start', "2013-01-01")
    end_string = args.get('end', datetime.date.today().isoformat())
    start_date = dateutil.parser.parse(start_string).date()
    end_date = dateutil.parser.parse(end_string).date()
    nb_days = (end_date - start_date).days
    for days in range(nb_days):
        current_date = start_date + datetime.timedelta(days=days)
        current_date_str = current_date.strftime("%Y/%m/%d")
        logger.debug(f'Getting parsed objects for {current_date_str} from object storage')
        publications = get_objects_by_prefix(container='pubmed', prefix=f'parsed/{current_date_str}')
        logger.debug(f'{len(publications)} publications retrieved from object storage')
        logger.debug(f'Start country detection')
        filtered_publications = filter_publications_by_country(publications=publications,
                                                               countries_to_keep=['fr', 'gp', 'mq', 'gf', 're'])
        logger.debug(f'{len(filtered_publications)} / {len(publications)} publications remaining')
        enriched_publications = enrich(publications=filtered_publications)
        logger.debug(f'Now indexing in {index}')
        load_in_es(data=enriched_publications, index=index)
