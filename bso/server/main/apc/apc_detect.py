from bso.server.main.apc.doaj_detect import detect_doaj
from bso.server.main.apc.openapc_detect import detect_openapc

# estimation des apc par publication

def detect_apc(doi: str, journal_issns: str, published_date: str, dois_info: dict) -> dict:
    issns = []
    if journal_issns and isinstance(journal_issns, str):
        issns = [k.strip() for k in journal_issns.split(',')]

    is_oa_publisher = False
    last_obs_date = max([k for k in dois_info.keys() if k != 'global'])
    oa_loc = dois_info[last_obs_date].get('oa_locations', [])
    if oa_loc is None:
        oa_loc = []
    for loc in oa_loc:
        if loc is None:
            continue
        host_type = loc.get('host_type')
        if host_type == 'publisher':
            is_oa_publisher = True


    # estimation via le DOAJ
    res_doaj = detect_doaj(issns, published_date)
    
    # estimation via openAPC
    res_openapc = detect_openapc(doi, issns, published_date)

    res = {'has_apc': None}
    is_openapc_estimation_ok = False
    # on commence par tenter d'estimer d'éventuels APC avec openAPC
    if res_openapc.get('has_apc'):
        res.update(res_openapc)
        if res_openapc.get('apc_source') in ['openAPC_estimation_issn_year', 'openAPC']: #present dans openAPC ou estimation avec la moyenne sur la revue x annee
            is_openapc_estimation_ok = True
    # si OA avec hébergement éditeur et pas d'APC détecté avec openAPC, on vérifie si des APC sont renseignés dans le DOAJ
    if (is_oa_publisher) and (not is_openapc_estimation_ok) and (res_doaj.get('has_apc')):
        res.update(res_doaj)
   
    # dans tous les cas, on récupère les infos du DOAJ s'il y en a
    for field in ['amount_apc_doaj_EUR', 'amount_apc_doaj', 'currency_apc_doaj']:
        if field in res_doaj and res_doaj.get(field):
            res[field] = res_doaj.get(field)
    return res
