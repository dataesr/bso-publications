# bso-publications
[![Discord Follow](https://dcbadge.vercel.app/api/server/dkcww8vs?style=flat)](https://discord.gg/dkcww8vs)
![GitHub](https://img.shields.io/github/license/dataesr/bso-publications)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dataesr/bso-publications)
![Build](https://github.com/dataesr/bso-publications/actions/workflows/build.yml/badge.svg)

## API Baromètre

Les données issues du Baromètre sont en OpenData https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.jsonl.gz
De plus, les données sont consommables par une API ouverte (cf documentation Elasticsearch).
Exemple de requête

```
from elasticsearch import Elasticsearch

ES_HOST = "https://cluster.elasticsearch.dataesr.ovh/"
ES_INDEX = "bso-publications"
ES_PASSWORD = "vn84q9Xef9U7pmU"
ES_USER = "BSO"

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

## Generate publication
```shell
cd doc
sh build_pdf.sh
```
