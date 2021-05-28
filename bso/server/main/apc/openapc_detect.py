import pandas as pd
import requests
import io
import string
import numpy as np

s=requests.get("https://github.com/OpenAPC/openapc-de/blob/master/data/apc_de.csv?raw=true").content

df_openapc=pd.read_csv(io.StringIO(s.decode('utf-8')))
apc = {}
openapc_doi = {}
for i, row in df_openapc.iterrows():
    if not pd.isnull(row['doi']):
        doi = row['doi'].lower().strip()
        if '10.' in doi:
            openapc_doi[doi] = row['euro']
    for issn in ['issn', 'issn_l', 'issn_print', 'issn_electronic']:
        if not pd.isnull(row[issn]):
            key = row[issn].strip() + ";" + str(row['period'])[0:4]
            if key not in apc:
                apc[key] = []
            apc[key].append(row['euro'])
            break
            
apc_avg = {}
for k in apc:
    apc_avg[k] = np.mean(apc[k])

def detect_openapc(doi, issns, date_str):
    if doi in openapc_doi:
        return {"has_apc": True, "amount_apc_EUR": openapc_doi[doi], "apc_source": "openAPC",
                "amount_apc_openapc_EUR": openapc_doi[doi]}
    for issn in issns:
        if issn:
            if date_str:
                key = issn.strip()+";"+date_str[0:4]
                if key in apc_avg:
                    return  {"has_apc": True, "amount_apc_EUR": apc_avg[key], "apc_source": "openAPC estimation",
                         "amount_apc_openapc_EUR": apc_avg[key]}

    return {"has_apc": None}
