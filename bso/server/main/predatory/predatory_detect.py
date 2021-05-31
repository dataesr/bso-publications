import pandas as pd
import requests
from bs4 import BeautifulSoup

from bso.server.main.strings import normalize


def get_domain(url):
    url_with_no_http = url.replace("https://", "").replace("http://", "")
    url_with_no_http = url_with_no_http.replace("www.", "")
    return url_with_no_http.split('.')[0]


def update_list_publishers():
    publishers_url = "https://beallslist.net/"
    soup = BeautifulSoup(requests.get(publishers_url).text, 'lxml')
    publishers = []
    for e in soup.find_all('li'):
        if e.find('a'):
            a = e.find('a')
            if 'target' in a.attrs:
                url = a.attrs['href']
                domain = get_domain(url)
                name = a.get_text(' ')
                publishers.append({'url': url, 'publisher': name, 'domain': domain})

    df_publishers = pd.DataFrame(publishers)
    return df_publishers.to_dict(orient="records")


def update_list_journals():
    journals_url = "https://beallslist.net/standalone-journals/"
    soup = BeautifulSoup(requests.get(journals_url).text, 'lxml')
    journals = []
    for e in soup.find_all('li'):
        if e.find('a'):
            a = e.find('a')
            if 'target' in a.attrs:
                url = a.attrs['href']
                domain = get_domain(url)
                name = a.get_text(' ')
                journals.append({'url': url, 'journal': name, 'domain': domain})

    df_journals = pd.DataFrame(journals)
    return df_journals.to_dict(orient="records")


pred_publishers = update_list_publishers()
pred_journals = update_list_journals()
pred_j = [normalize(e['journal']) for e in pred_journals]
pred_p = [normalize(e['publisher']) for e in pred_publishers]
pred_j_domain = [normalize(e['domain']) for e in pred_journals]
pred_p_domain = [normalize(e['domain']) for e in pred_publishers]


def detect_predatory(publisher, journal):
    predatory_publisher = False
    predatory_journal = False
    if publisher and normalize(publisher) in pred_p:
        predatory_publisher = True
        predatory_journal = True
    elif publisher and normalize(publisher) in pred_p_domain:
        predatory_publisher = True
        predatory_journal = True
    elif journal and normalize(journal) in pred_j:
        predatory_journal = True
    elif journal and normalize(journal) in pred_j_domain:
        predatory_journal = True
    return {
        'predatory_publisher': predatory_publisher,
        'predatory_journal': predatory_journal
    }
