import pandas as pd
import re
from bso.server.main.funders.anr import get_anr_details
from bso.server.main.funders.anses import get_anses_details
from bso.server.main.logger import get_logger

logger = get_logger(__name__)


def normalize_grant(grant):
    grants = []
    if not isinstance(grant.get('grantid'), str):
        return [grant]
    for grantid in re.split(';|,| ', grant['grantid']):
        grantid = grantid.strip()
        if not grantid:
            continue
        current_grant = grant.copy()
        current_grant['grantid'] = grantid
        if 'funding_year' in grant and not isinstance(grant['funding_year'], int):
            current_grant['funding_year'] = int(grant['funding_year'])
        # ANR
        if grantid[0:4].upper()=='ANR-':
            grant_anr = get_anr_details(grantid)
            if grant_anr:
                grants.append(grant_anr)
        # ANSES
        if grantid[0:6].upper()=='ANSES-':
            grant_anses = get_anses_details(grantid)
            if grant_anses:
                grants.append(grant_anses)
        # others
        elif isinstance(grant.get('agency'), str):
            agency = grant['agency']
            if 'NIH HHS' in agency:
                current_grant['agency'] = 'NIH HHS'
                current_grant['sub_agency'] = agency
                grants.append(current_grant)
            if 'H2020' in agency:
                current_grant['agency'] = 'H2020'
                current_grant['sub_agency'] = 'H2020'
                grants.append(current_grant)
    return grants

