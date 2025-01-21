# Baromètre publications
[![Discord Follow](https://dcbadge.vercel.app/api/server/TudsqDqTqb?style=flat)](https://discord.gg/TudsqDqTqb)
![GitHub](https://img.shields.io/github/license/dataesr/bso-publications)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dataesr/bso-publications)
![Release](https://github.com/dataesr/bso-publications/actions/workflows/release.yml/badge.svg)
[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/dataesr/bso-publications)](https://archive.softwareheritage.org/browse/origin/?origin_url=https://github.com/dataesr/bso-publications)

## API

The underlying dataset of the French Open Science Monitor is open and can be downloaded https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.jsonl.gz
One can also consume the data through an Elasticsearch endpoint (cf documentation Elasticsearch).
Contact us to get an user and password at bso [at] recherche [dot] gouv [dot] fr
Running example below

```
from elasticsearch import Elasticsearch

ES_HOST = "https://cluster-production.elasticsearch.dataesr.ovh/"
ES_INDEX = "bso-publications"

es = Elasticsearch(ES_HOST, http_auth=(ES_USER, ES_PASSWORD))

body = {
   "query":{
      "bool":{
         "must":[
            {
               "terms":{
                  "bso_country_corrected.keyword":[
                     "fr"
                  ]
               }
            },
            {
               "terms":{
                  "id_type.keyword":[
                     "doi"
                  ]
               }
            },
            {
               "terms":{
                  "genre.keyword":[
                     "journal-article",
                     "proceedings",
                     "book-chapter",
                     "book",
                     "preprint"
                  ]
               }
            },
            {
               "range":{
                  "year":{
                     "gte":2019,
                     "lte":2022
                  }
               }
            }
         ]
      }
   }
}
body['aggs'] = {}
body['size'] = 1
res = es.search(index=ES_INDEX, body=body)
res
```

## Release
To create a new release:
```shell
make release VERSION=X.X.X
```

## Commands

To build a Docker image:

`make docker-build`

To publish the previously built image:

`make docker-push`

## Generate publication
```shell
cd doc
sh build_pdf.sh
```
