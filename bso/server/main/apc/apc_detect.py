from bso.server.main.apc.doaj_detect import detect_doaj
from bso.server.main.apc.openapc_detect import detect_openapc

# estimation des apc par publication

def detect_apc(doi: str, journal_issns: str, published_date: str) -> dict:
    issns = []
    if journal_issns and isinstance(journal_issns, str):
        issns = [k.strip() for k in journal_issns.split(',')]

    # estimation via le DOAJ
    res_doaj = detect_doaj(issns, published_date)
    
    # estimation via openAPC
    res_openapc = detect_openapc(doi, issns, published_date)

    res = {'has_apc': None}
    # on commence par tenter d'estimer d'éventuels APC avec openAPC
    if res_openapc.get('has_apc'):
        res.update(res_openapc)
    # si pas d'APC détecté avec openAPC, on vérifie si des APC sont renseignés dans le DOAJ
    if not res_openapc.get('has_apc') and res_doaj.get('has_apc'):
        res.update(res_doaj)
   
    # dans tous les cas, on récupère les infos du DOAJ s'il y en a
    for field in ['amount_apc_doaj_EUR', 'amount_apc_doaj', 'currency_apc_doaj']:
        if field in res_doaj and res_doaj.get(field):
            res[field] = res_doaj.get(field)
    return res
