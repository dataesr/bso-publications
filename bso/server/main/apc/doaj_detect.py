import io
import pandas as pd
import requests
import string

from currency_converter import CurrencyConverter

c = CurrencyConverter(fallback_on_wrong_date=True)
s = requests.get("https://doaj.org/csv").content
df_doaj = pd.read_csv(io.StringIO(s.decode('utf-8')))


def is_digit_only(x):
    digits_only = True
    for w in x.strip():
        if w not in string.digits:
            digits_only = False
    return digits_only


def split_currency(x):
    if pd.isnull(x):
        return None
    if is_digit_only(x):
        return {"amount": int(x), "currency": "USD"}
    currency = x[-3:].upper()
    for w in currency:
        if w not in string.ascii_uppercase:
            return None
    amount = x.replace(currency, "").strip()
    try:
        amount = int(amount)
    except:
        return None
    return {"amount": amount, "currency": currency}


doaj_infos = {}
for i, row in df_doaj.iterrows():
    for issn_type in ["Journal ISSN (print version)", "Journal EISSN (online version)"]:
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
            if 'Journal article processing charges (APCs)' in row:
                has_apc = row['Journal article processing charges (APCs)']
            elif 'APC' in row:
                has_apc = row['APC']
            else:
                print("missing has_APC info in DOAJ", flush=True)
            if has_apc == "Yes":
                has_apc = True
            elif has_apc == "No":
                has_apc = False
            current_info = {
                'has_apc': has_apc,
                'apc_amount': apc_amount,
                'apc_currency': apc_currency,
                'source': 'doaj'
            }
            doaj_infos[issn] = current_info


def detect_doaj(issns, date_str):
    for issn in issns:
        if issn in doaj_infos:
            info = doaj_infos[issn]
            amount = info['apc_amount']
            currency = info['apc_currency']
            has_apc = info['has_apc']
            if amount and currency:
                try:
                    amount_eur = c.convert(amount, currency, 'EUR', date=pd.to_datetime(date_str))
                except:
                    amount_eur = None
                return {
                    "has_apc": has_apc,
                    "amount_apc_EUR": amount_eur,
                    "apc_source": "doaj",
                    "amount_apc_doaj_EUR": amount_eur,
                    "amount_apc_doaj": amount,
                    "currency_apc_doaj": currency
                }
    return {"has_apc": None}
