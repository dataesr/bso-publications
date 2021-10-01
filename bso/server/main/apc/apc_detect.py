from bso.server.main.apc.doaj_detect import detect_doaj
from bso.server.main.apc.openapc_detect import detect_openapc

# estimation des apc par publication

def detect_apc(doi: str, journal_issns: str, publisher: str, published_date: str, dois_info: dict) -> dict:
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
    res_openapc = detect_openapc(doi, issns, publisher, published_date)

    res = {'has_apc': None}
    if not is_oa_publisher:
        return res
    is_openapc_estimation_accurate = False
    # on commence par tenter d'estimer d'éventuels APC avec openAPC
    if res_openapc.get('has_apc'):
        res.update(res_openapc)
        if res_openapc.get('apc_source') not in  ['openAPC_estimation_year']:  #present dans openAPC ou estimation assez fine
            is_openapc_estimation_accurate = True
    # dans tous les cas, on récupère les infos du DOAJ s'il y en a
    for field in ['amount_apc_doaj_EUR', 'amount_apc_doaj', 'currency_apc_doaj']:
        if field in res_doaj and res_doaj.get(field):
            res[field] = res_doaj.get(field)
    # s'il y a une info dans le DOAJ et (pas d'info openAPC ou info trop imprécise dans openAPC), on met à jour avec doaj
    if res_doaj['has_apc'] is not None and (res_openapc['has_apc'] is None or is_openapc_estimation_accurate is False):
        res.update(res_doaj)
    # enfin, si le DOAJ donne l'info diamond, on garde cette info
    if res_doaj['has_apc'] is False:
        res.update(res_doaj)

   
    return res
