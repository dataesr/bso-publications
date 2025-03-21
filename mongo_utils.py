from bso.server.main.unpaywall_mongo import full_mongo_dump
import datetime

#today = datetime.date.today().isoformat().replace('-', '')
#full_mongo_dump(today)

upload_s3(container='mongodumps', source = f'/upw_data/mongo_dumps/20250317', destination='bso/20250317', is_public=False)
