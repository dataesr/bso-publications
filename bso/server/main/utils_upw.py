from bso.server.main.strings import dedup_sort
from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def chunks(lst, n):
    if len(lst) == 0:
        return [[]]
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def normalize_license(x: str) -> str:
    if x is None:
        normalized_license = 'no license'
    elif 'elsevier-specific' in x:
        normalized_license = 'elsevier-specific'
    elif '-specific' in x:
        normalized_license = 'publisher-specific'
    elif x in ['pd', 'cc0']:
        normalized_license = 'cc0-public-domain'
    else:
        normalized_license = x
    return normalized_license


def reduce_license(all_licenses: list) -> list:
    if 'cc0' in all_licenses:
        return ['cc0-public-domain']
    ccbys = [e for e in all_licenses if 'cc-by' in e]
    if len(ccbys) > 0:
        min_ccy_length = min([len(e) for e in ccbys])
        return [e for e in ccbys if len(e) == min_ccy_length]
    for k in ['publisher-specific', 'implied-oa', 'elsevier-oa']:
        if k in all_licenses:
            return ['other']
    return ['no license']


def reduce_status(all_statuses: list) -> list:
    statuses = []
    if 'green' in all_statuses:
        statuses.append('green')
    for status in ['accord_elsevier', 'diamond', 'gold', 'hybrid', 'other']:
        if status in all_statuses:
            statuses.append(status)
            break
    # status accord_elsevier prioritaire, mais ensuite on remplace par 'other'
    return ['other' if x == 'accord_elsevier' else x for x in statuses]


def get_repository(a_repo: str) -> str:
    if a_repo.replace('www.', '')[0:3].lower() == 'hal':
        return 'HAL'
    for r in ['bioRxiv', 'medRxiv', 'arXiv', 'Research Square', 'Zenodo', 'Archimer', 'RePEc', 'CiteSeerX', 'univOAK']:
        if r.lower().replace(' ', '') in a_repo.lower():
            return r
    if 'lilloa' in a_repo.lower():
        return 'LillOA (Lille Open Archive)'
    if 'ucl.ac.uk' in a_repo.lower():
        return 'UCL Discovery'
    if 'lirias' in a_repo.lower() and 'kuleuven' in a_repo.lower():
        return 'LIRIAS (KU Leuven)'
    if 'pure.atira.dk' in a_repo.lower():
        return 'Pure (Denmark)'
    if 'digital.csic.es' in a_repo.lower():
        return 'DIGITAL.CSIC (Spain)'
    if 'escholarship.org/ark' in a_repo.lower():
        return 'California Digital Library - eScholarship'
    if 'jupiter.its.unimelb.edu.au' in a_repo.lower():
        return 'University of Melbourne - Minerva Access'
    if 'helda.helsinki' in a_repo.lower():
        return 'HELDA - Digital Repository of the University of Helsinki'
    if 'osti.gov' in a_repo.lower():
        return 'US Office of Scientific and Technical Information'
    for f in ['pubmedcentral', 'ncbi.nlm.nih.gov/pmc', 'europepmc']:
        if f in a_repo:
            return 'PubMed Central'
    return a_repo


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


def format_upw_millesime(elem: dict, asof: str, has_apc: bool, publisher: str, genre: str) -> dict:
    res = {'snapshot_date': asof}
    millesime = get_millesime(asof)
    res['observation_date'] = millesime
    for f in ['is_oa', 'journal_is_in_doaj', 'journal_is_oa']:
        current_value = elem.get(f, False)
        if current_value:
            res[f] = current_value
        else:
            res[f] = False
    res['unpaywall_oa_status'] = elem.get('oa_status')
    if res['is_oa'] is False:
        res['oa_host_type'] = 'closed'
        res['oa_colors'] = ['closed']
        res['oa_colors_with_priority_to_publisher'] = ['closed']
        return {millesime: res}
    oa_loc = elem.get('oa_locations', [])
    if oa_loc is None:
        oa_loc = []
    host_types = []
    oa_colors = []
    repositories = []
    repositories_pmh, repositories_url, repositories_institution = [], [], []
    licence_repositories = []
    licence_publisher = []
    oa_locations = []
    nb_valid_loc = 0
    for loc in oa_loc:
        if loc is None:
            continue
        if (publisher == 'Springer-Nature') and (genre == 'book') and (isinstance(loc.get('pmh_id'), str)) and ('aleph.bib-bvb.de' in loc.get('pmh_id')):
            # to fix false positive detection by unpaywall
            continue
        if isinstance(loc.get('url'), str):
            loc['url'] = loc['url'].lower().strip()
        licence = normalize_license(loc.get('license'))
        loc['license_normalized'] = licence
        host_type = loc.get('host_type')
        if host_type == 'repository':
            current_repo_instit = loc.get('repository_institution')
            current_repo_url = None
            if loc.get("url", False):
                current_repo_url = loc['url'].split('/')[2]
                if '.ncbi.' in current_repo_url:
                    current_repo_url = '/'.join(loc['url'].split('/')[2:4])
            current_repo_pmh = None
            pmh_id = loc.get('pmh_id')
            if pmh_id:
                pmh_id_l = pmh_id.replace('https://', '').replace('http://', '').split(':')
                if len(pmh_id_l) >= 2:
                    current_repo_pmh = pmh_id_l[1]
                    if current_repo_pmh.lower() == "oai" and len(pmh_id_l) >= 3:
                        current_repo_pmh = pmh_id_l[2]

            current_repo = None
            if current_repo_pmh and isinstance(current_repo_pmh, str):
                repositories_pmh.append(current_repo_pmh)
                current_repo = get_repository(current_repo_pmh)
            if current_repo_url and isinstance(current_repo_url, str) and current_repo_url.lower() not in ['doi.org']:
                repositories_url.append(current_repo_url)
                if current_repo is None:
                    current_repo = get_repository(current_repo_url)
            if current_repo_instit and isinstance(current_repo_instit, str):
                repositories_institution.append(current_repo_instit)
                if current_repo is None:
                    current_repo = get_repository(current_repo_instit)
            # if current_repo in ['mdpi.com', 'edpsciences.org']:
            #    continue # not green !
            loc['repository_normalized'] = current_repo
            if current_repo:
                repositories.append(current_repo)
            if licence:
                licence_repositories.append(licence)
            status = 'green'
        elif host_type == 'publisher':
            if licence:
                licence_publisher.append(licence)
            if ('author manuscript' in loc.get('evidence')) and ('accepted' in loc.get('version')) and (publisher == 'Elsevier'):
                status = 'accord_elsevier'  # accord Elsevier
            elif (has_apc is not None) and (not has_apc) and elem.get('journal_is_in_doaj'):
                status = 'diamond'
            elif elem.get('journal_is_oa') == 1: # and (has_apc is True): 20221126
                status = 'gold'
            #elif (has_apc is True): 20221026
            elif licence != 'no license':
                status = 'hybrid'
            else:
                status = 'other'
        else:
            status = 'unknown'
        host_types.append(host_type)
        oa_locations.append(loc)
        nb_valid_loc += 1
        oa_colors.append(status)
    if licence_publisher:
        res['licence_publisher'] = dedup_sort(reduce_license(licence_publisher))
    if licence_repositories:
        res['licence_repositories'] = dedup_sort(reduce_license(licence_repositories))
    if repositories:
        res['repositories'] = dedup_sort(repositories)
    if repositories_url:
        res['repositories_url'] = dedup_sort(repositories_url)
    if repositories_pmh:
        res['repositories_pmh'] = dedup_sort(repositories_pmh)
    if repositories_institution:
        res['repositories_institution'] = dedup_sort(repositories_institution)
    if oa_locations:
        res['oa_locations'] = oa_locations
    res['oa_colors'] = reduce_status(oa_colors)
    res['oa_colors_with_priority_to_publisher'] = get_color_with_publisher_prio(res['oa_colors'])
    res['oa_host_type'] = ";".join(dedup_sort(host_types))
    
    if nb_valid_loc == 0:
        #logger.debug(f'exclude false positive OA for {elem}')
        res['is_oa'] = False
        res['oa_host_type'] = 'closed'
        res['oa_colors'] = ['closed']
        res['oa_colors_with_priority_to_publisher'] = ['closed']
    return {millesime: res}
