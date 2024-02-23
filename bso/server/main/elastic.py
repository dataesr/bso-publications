from elasticsearch import Elasticsearch, helpers

from bso.server.main.config import ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK, ES_URL
from bso.server.main.decorator import exception_handler
from bso.server.main.logger import get_logger

client = None
logger = get_logger(__name__)


@exception_handler
def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK))
    return client

@exception_handler
def refresh_index(index):
    logger.debug(f'Refreshing {index}')
    es = get_client()
    response = es.indices.refresh(index=index)
    logger.debug(response)

@exception_handler
def get_doi_not_in_index(index, dois):
    es = get_client()
    results = es.search(
        index=index,
        body={"query": {"bool": {"filter": [{'terms': {'doi.keyword': dois}}]}}, "fields": ['doi'], "size": len(dois),
              "_source": False},
        request_timeout=60*5
    )
    existing_dois = set([e['fields']['doi'][0] for e in results['hits']['hits']])
    not_indexed_dois = set(dois) - existing_dois
    res = []
    for doi in list(not_indexed_dois):
        res += get_doi_not_in_index_one(index, doi)
    logger.debug(f'{len(res)} dois not in index detected')
    return res


@exception_handler
def get_doi_not_in_index_one(index, doi):
    es = get_client()
    results = es.search(
        index=index,
        request_cache=False,
        body={"query": {"bool": {"filter": [{'term': {'doi.keyword': doi}}]}}, "fields": ['doi'], "_source": True},
        request_timeout=60*5
    )
    existing_dois = set([e['fields']['doi'][0] for e in results['hits']['hits']])
    not_indexed_dois = set([doi]) - existing_dois
    return list(not_indexed_dois)


@exception_handler
def update_local_affiliations(index, current_dois, local_affiliations):
    es = get_client()
    logger.debug(f'updating with local affiliations {local_affiliations} for {len(current_dois)} dois')
    body = {
        "script": {
            "lang": "painless",
            "refresh": True,
            "conflicts": "proceed",
            "inline":  "if (ctx._source.bso_local_affiliations == null) {ctx._source.bso_local_affiliations ="
                       " new ArrayList();} ctx._source.bso_local_affiliations.addAll(params.local_affiliations);"
                       "ctx._source.bso_local_affiliations = ctx._source.bso_local_affiliations.stream().distinct()"
                       ".sorted().collect(Collectors.toList())",
            "params": {"local_affiliations": local_affiliations}
        },
        "query": {
            "bool": {
              "filter": [{
                "terms": {
                  "doi.keyword": current_dois
                }
              }]
            }
        }
    }
    es.update_by_query(index=index, body=body, request_timeout=60*5)


@exception_handler
def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


@exception_handler
def update_alias(alias: str, old_index: str, new_index: str) -> None:
    es = get_client()
    logger.debug(f'updating alias {alias} from {old_index} to {new_index}')
    response = es.indices.update_aliases({
        'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}
        ]
    })
    logger.debug(response)

def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        },
        "autocomplete": {
          "type": "custom",
          "tokenizer": "icu_tokenizer",
          "filter": [
            "lowercase",
            'french_elision',
            'icu_folding',
            "autocomplete_filter"
          ]
        }
    }

def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        }
    }

@exception_handler
def reset_index(index: str) -> None:
    es = get_client()
    delete_index(index)
    
    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }
    
    dynamic_match = None
    if 'bso-publications' in index:
        # dynamic_match = "*oa_locations"
        dynamic_match = None
    elif 'publications-' in index:
        dynamic_match = "*authors"

    mappings = { 'properties': {} }
    # attention l'analyzer .keyword ne sera pas présent pour ce champs !

    if 'bso-' in index:
        for f in ['affiliations.name', 'authors.full_name']:
            mappings['properties'][f] = { 
                    'type': 'text',
                    'analyzer': 'light',
                    'fields': {
                        'keyword': {
                            'type':  'keyword'
                        }
                    }
                }
        for f in ['title', 'authors.first_name', 'authors.last_name']:
            mappings['properties'][f] = { 
                    'type': 'text',
                    'analyzer': 'light' 
                }

    if dynamic_match:
        mappings["dynamic_templates"] = [
                {
                    "objects": {
                        "match": dynamic_match,
                        "match_mapping_type": "object",
                        "mapping": {
                            "type": "nested"
                        }
                    }
                }
            ]
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')
    else:
        logger.debug(f'ERROR !')
        logger.debug(response)

@exception_handler
def reset_index_scanr(index: str) -> None:
    es = get_client()
    delete_index(index)
    

    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }
    
    mappings = { 'properties': {} }
    mappings['properties']['autocompleted'] = {
                'type': 'search_as_you_type',
                'analyzer': 'light'
                #'type': 'text',
                #'analyzer': 'autocomplete'
            }
    for f in ['title.default', 'affiliations.label.default', 'authors.firstName', 'authors.lastName', 'authors.fullName', 'authors.affiliations.name', 
              'source.title', 'keywords.default', 'domains.label.default', 'project.label.default']: 
        mappings['properties'][f] = { 
                'type': 'text',
                'analyzer': 'light',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    for f in [ 'summary.default']: 
        mappings['properties'][f] = { 
                'type': 'text',
                'analyzer': 'light',
            }



    mappings["vector_text"]: {
        "type": "dense_vector",
        "dims": 512,
        "index": True,
        "similarity": "dot_product"
      }
    mappings["_source"] = {
      "excludes": [
        "vector_*"
      ]
    }

    dynamic_match = None
    #if 'publications-' in index:
    #    dynamic_match = "*authors"

    #for f in ['id']:
    #    mappings['properties'][f] = {
    #            'type': 'long'
    #            }
    # attention l'analyzer .keyword ne sera pas présent pour ce champs !
    #for f in ['title', 'affiliations.name', 'authors.first_name', 'authors.last_name', 'authors.full_name', 'authors.affiliations.name']:
    #    mappings['properties'][f] = {
    #            'type': 'text',
    #            'analyzer': 'light'
    #        }

    if dynamic_match:
        mappings["dynamic_templates"] = [
                {
                    "objects": {
                        "match": dynamic_match,
                        "match_mapping_type": "object",
                        "mapping": {
                            "type": "nested"
                        }
                    }
                }
            ]
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')

@exception_handler
def load_in_es(data: list, index: str) -> list:
    es = get_client()
    actions = [{'_index': index, '_source': datum} for datum in data]
    ix = 0
    indexed = []
    for success, info in helpers.parallel_bulk(client=es, actions=actions, chunk_size=500, request_timeout=60,
                                               raise_on_error=False):
        if not success:
            logger.debug(f'A document failed: {info}')
        else:
            indexed.append(data[ix])
        ix += 1
    logger.debug(f'{len(data)} elements imported into {index}')
    return indexed
