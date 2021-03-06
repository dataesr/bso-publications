import os

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE', 'http://localhost:5004')
APP_ENV = os.getenv('APP_ENV')
ES_LOGIN_BSO_BACK = os.getenv('ES_LOGIN_BSO_BACK', '')
ES_PASSWORD_BSO_BACK = os.getenv('ES_PASSWORD_BSO_BACK', '')
ES_URL = os.getenv('ES_URL', 'http://localhost:9200')
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://mongo:27017/')
MOUNTED_VOLUME = os.getenv('MOUNTED_VOLUME', '/upw_data/')
