from bso.server.main.strings import dedup_sort


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def normalize_license(x: str) -> str:
    if x is None:
        return 'no license'
    elif 'elsevier-specific' in x:
        return 'elsevier-specific'
    elif '-specific' in x:
        return 'publisher-specific'
    elif x in ['pd', 'cc0']:
        return 'cc0-public-domain'
    return x


def reduce_license(all_licenses: list) -> list:
    if 'cc0' in all_licenses:
        return ['cc0-public-domain']
    ccbys = [e for e in all_licenses if 'cc-by' in e]
    if len(ccbys) > 0:
        min_ccy_length = min([len(e) for e in ccbys])
        return [e for e in ccbys if len(e) == min_ccy_length]
    for k in ['publisher-specific', 'implied-oa', 'elsevier-oa']:
        if k in all_licenses:
            return [k]
    return ['no license']


def reduce_status(all_statuses: list) -> list:
    statuses = []
    if 'green' in all_statuses:
        statuses.append('green')
    for status in ['diamond', 'gold', 'hybrid']:
        if status in all_statuses:
            statuses.append(status)
            break
    return statuses


def get_color_with_publisher_prio(oa_colors: list) -> list:
    if len(oa_colors) == 1 and 'green' in oa_colors:
        oa_colors_with_priority = ['green_only']
    else:
        oa_colors_with_priority = [c for c in oa_colors if c != 'green']
    return oa_colors_with_priority


def get_millesime(x: str) -> str:
    if x[0:4] < '2021':
        return x[0:4]
    month = int(x[4:6])
    if 1 <= month <= 3:
        return x[0:4] + 'Q1'
    if 4 <= month <= 6:
        return x[0:4] + 'Q2'
    if 7 <= month <= 9:
        return x[0:4] + 'Q3'
    if 10 <= month <= 12:
        return x[0:4] + 'Q4'
    return 'unk'


def format_upw_millesime(elem: dict, asof: str, has_apc: bool) -> dict:
    res = {'snapshot_date': asof}
    millesime = get_millesime(asof)
    res['observation_date'] = millesime
    res['is_oa'] = elem.get('is_oa', False)
    if res['is_oa'] is False:
        res['oa_host_type'] = ["closed"]
        res['oa_colors'] = ["closed"]
        res['oa_colors_with_priority_to_publisher'] = ["closed"]
        return res
    oa_loc = elem.get('oa_locations', [])
    if oa_loc is None:
        oa_loc = []
    host_types = []
    oa_colors = []
    repositories = []
    repositories_url, repositories_institution = [], []
    licence_repositories = []
    licence_publisher = []
    for loc in oa_loc:
        if loc is None:
            continue
        licence = normalize_license(loc.get('license'))
        host_type = loc.get('host_type')
        host_types.append(host_type)
        if host_type == 'repository':
            status = 'green'
            current_repo_url = loc['url'].split('/')[2]
            current_repo_pmh = None
            pmh_id = loc.get('pmh_id')
            if pmh_id:
                pmh_id_l = pmh_id.split(':')
                if len(pmh_id_l) > 1:
                    current_repo_pmh = pmh_id_l[1]
            current_repo_instit = loc.get('repository_institution')
            repositories.append(current_repo_pmh)
            repositories_url.append(current_repo_url)
            repositories_institution.append(current_repo_instit)
            licence_repositories.append(licence)
        elif host_type == "publisher":
            licence_publisher.append(licence)
            if has_apc is False and elem.get('journal_is_in_doaj'):
                status = "diamond"
            elif elem.get('journal_is_oa') == 1:
                status = 'gold'
            else:
                status = 'hybrid'
            # elif license not in ['elsevier-specific', 'no license']:
            #    status = 'hybrid'
            # else:
            #    status = 'bronze'
        else:
            status = 'unknown'
        oa_colors.append(status)
    if licence_publisher:
        res['licence_publisher'] = reduce_license(licence_publisher)
    if licence_repositories:
        res['licence_repositories'] = reduce_license(licence_repositories)
    if repositories:
        res['repositories'] = dedup_sort(repositories)
    if repositories_url:
        res['repositories_url'] = dedup_sort(repositories_url)
    if repositories_institution:
        res['repositories_institution'] = dedup_sort(repositories_institution)
    res['oa_colors'] = reduce_status(oa_colors)
    res['oa_colors_with_priority_to_publisher'] = get_color_with_publisher_prio(res['oa_colors'])
    res['oa_host_type'] = ";".join(dedup_sort(host_types))
    return res
