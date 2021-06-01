import io

import numpy as np
import pandas as pd
import requests

s = requests.get('https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/apc_de.csv').content

apc = {}
df_openapc = pd.read_csv(io.StringIO(s.decode('utf-8')))
openapc_doi = {}
for i, row in df_openapc.iterrows():
    if not pd.isnull(row['doi']):
        doi = row['doi'].lower().strip()
        if '10.' in doi:
            openapc_doi[doi] = row['euro']
    for issn in ['issn', 'issn_l', 'issn_print', 'issn_electronic']:
        if not pd.isnull(row[issn]):
            key = row[issn].strip() + ';' + str(row['period'])[0:4]
            if key not in apc:
                apc[key] = []
            apc[key].append(row['euro'])
            break
apc_avg = {}
for k in apc:
    apc_avg[k] = np.mean(apc[k])


def detect_openapc(doi: str, issns: list, date_str: str) -> dict:
    if doi in openapc_doi:
        return {
            'has_apc': True,
            'amount_apc_EUR': openapc_doi[doi],
            'apc_source': 'openAPC',
            'amount_apc_openapc_EUR': openapc_doi[doi]
        }
    for issn in issns:
        if isinstance(issn, str) and isinstance(date_str, str):
            key = issn.strip() + ';' + date_str[0:4]
            if key in apc_avg:
                return {
                    'has_apc': True,
                    'amount_apc_EUR': apc_avg[key],
                    'apc_source': 'openAPC estimation',
                    'amount_apc_openapc_EUR': apc_avg[key]
                }
    return {'has_apc': None}
