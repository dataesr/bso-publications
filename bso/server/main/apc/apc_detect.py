import pandas as pd
import requests
import io
import string
from bso.server.main.apc.doaj_detect import detect_doaj
from bso.server.main.apc.openapc_detect import detect_openapc

def detect_apc(doi, journal_issns, published_date):
    issns = []
    if journal_issns:
        issns = [k.strip() for k in journal_issns.split(',')]

    res_doaj = detect_doaj(issns, published_date)
    res_openapc = detect_openapc(doi, issns, published_date)

    res = {"has_apc": None}
    if res_openapc["has_apc"] is not None:
        res.update(res_openapc)
    
    if res_openapc["has_apc"] is None and res_doaj["has_apc"] is not None:
        res.update(res_doaj)

    for f in ["amount_apc_doaj_EUR", "amount_apc_doaj", "currency_apc_doaj"]:
        if f in res_doaj and res_doaj[f]:
            res[f] = res_doaj[f]

    return res
