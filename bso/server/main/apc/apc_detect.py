from bso.server.main.apc.doaj_detect import detect_doaj
from bso.server.main.apc.openapc_detect import detect_openapc


def detect_apc(doi, journal_issns, published_date) -> dict:
    issns = []
    if journal_issns and isinstance(journal_issns, str):
        issns = [k.strip() for k in journal_issns.split(',')]
    res_doaj = detect_doaj(issns, published_date)
    res_openapc = detect_openapc(doi, issns, published_date)
    res = {'has_apc': None}
    if res_openapc.get('has_apc'):
        res.update(res_openapc)
    if not res_openapc.get('has_apc') and res_doaj.get('has_apc'):
        res.update(res_doaj)
    for field in ['amount_apc_doaj_EUR', 'amount_apc_doaj', 'currency_apc_doaj']:
        if field in res_doaj and res_doaj.get(field):
            res[field] = res_doaj[field]
    return res
