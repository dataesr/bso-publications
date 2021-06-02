import datetime
import os
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search, A
from elasticsearch import helpers
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

es = None
try:
    es = Elasticsearch([os.getenv("ES_URL")],http_auth=(os.getenv("ES_LOGIN"),os.getenv('ES_PASSWORD')))
except:
    logger.debug("cannot connect to es")

def delete_index(myIndex):
    logger.debug("deleting "+myIndex)
    del_docs = es.delete_by_query(index=myIndex, body={"query": {"match_all": {}}})
    logger.debug(del_docs)
    del_index = es.indices.delete(index=myIndex, ignore=[400, 404])
    logger.debug(del_index)
    return

def reset_index(myIndex, filters={}, char_filters={}, tokenizers={}, analyzers={}):
    try:
        delete_index(myIndex)
    except:
        logger.debug("delete failed")

    response = es.indices.create(
              index=myIndex,
              body={
                  "settings": {},
                  "mappings": {}   
              },
              ignore=400 # ignore 400 already exists code
          )
  
    if 'acknowledged' in response:
        if response['acknowledged'] == True:
            logger.debug("INDEX MAPPING SUCCESS FOR INDEX:" + str(response['index']))
 
def load_in_es(data_to_import, myIndex):
    start = datetime.datetime.today()
    actions = [
        {
          "_index": myIndex,
          "_type": "_doc",
          "_id": j,
          "_source": data_to_import[j]
        }
    for j in range(0, len(data_to_import))
    ]
    bulk_res = helpers.bulk(es, actions, chunk_size=500)

    end = datetime.datetime.today()
    delta = (end - start)
    nb = len(data_to_import)
    logger.debug(f"{nb} elts imported into {myIndex} in {delta}")

