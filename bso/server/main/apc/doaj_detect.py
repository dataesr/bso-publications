import io
import string

import pandas as pd
import requests
from currency_converter import CurrencyConverter

from bso.server.main.logger import get_logger

logger = get_logger(__name__)
c = CurrencyConverter(fallback_on_missing_rate=True, fallback_on_wrong_date=True)

doaj_infos = {}

def is_digit_only(x: str) -> bool:
    digits_only = True
    for w in x.strip():
        if w not in string.digits:
            digits_only = False
    return digits_only


def split_currency(x):
    if pd.isnull(x):
        return None
    if is_digit_only(x):
        return {'amount': int(x), 'currency': 'USD'}
    currency = x[-3:].upper()
    for w in currency:
        if w not in string.ascii_uppercase:
            return None
    amount = x.replace(currency, '').strip()
    try:
        amount = int(amount)
    except:
        return None
    return {'amount': amount, 'currency': currency}

def init_doaj():
    logger.debug('init DOAJ infos')
    s = requests.get('https://doaj.org/csv').content
    df_doaj = pd.read_csv(io.StringIO(s.decode('utf-8')))
    # the column stating if there are APC changes name from time to time !!
    has_apc_potential_col = [col for col in df_doaj.columns if 'apc' in col.lower()]
    has_apc_col = 'has_apc_col'
    for col in has_apc_potential_col:
        if (len(df_doaj[col].unique().tolist())) == 2:
            has_apc_col = col
            break

    global doaj_infos
    doaj_infos = {}
    for i, row in df_doaj.iterrows():
        for issn_type in ['Journal ISSN (print version)', 'Journal EISSN (online version)']:
            if issn_type in row:
                issn = row[issn_type]
                if not isinstance(issn, str):
                    continue
                if len(issn) < 3:
                    continue
                apc_amount = None
                apc_amount_EUR = None
                apc_currency = None
                if not pd.isnull(row['APC amount']):
                    apc_amount = row['APC amount']
                    if 'Currency' in row:
                        apc_currency = row['Currency']
                    else:
                        apc_currency = None

                    if apc_currency is None:
                        sp = split_currency(apc_amount)
                        if sp:
                            apc_amount = sp.get('amount')
                            apc_currency = sp.get('currency')
                has_apc = None
                if has_apc_col in row:
                    has_apc = row[has_apc_col]
                else:
                    logger.warning('Missing has_APC info in DOAJ')
                if has_apc == 'Yes':
                    has_apc = True
                elif has_apc == 'No':
                    has_apc = False
                    apc_amount = 0
                    apc_currency = 'EUR'
                current_info = {
                    'has_apc': has_apc,
                    'apc_amount': apc_amount,
                    'apc_currency': apc_currency,
                    'source': 'doaj'
                }
                doaj_infos[issn] = current_info


def detect_doaj(issns: list, date_str: str) -> dict:
    if len(doaj_infos) == 0:
        init_doaj()
    for issn in issns:
        if issn in doaj_infos:
            # si l'ISSN du doi est dans le DOAJ, on récupère les infos du DOAJ (après une éventuelle conversion en euros si besoin)
            info = doaj_infos[issn]
            amount = info['apc_amount']
            currency = info['apc_currency']
            has_apc = info['has_apc']
            if amount is not None and currency:
                try:
                    amount_eur = c.convert(amount, currency, 'EUR', date=pd.to_datetime(date_str))
                except:
                    amount_eur = None
                return {
                    'has_apc': has_apc,
                    'amount_apc_EUR': amount_eur,
                    'apc_source': 'doaj',
                    'amount_apc_doaj_EUR': amount_eur,
                    'amount_apc_doaj': amount,
                    'currency_apc_doaj': currency
                }
    return {'has_apc': None}
