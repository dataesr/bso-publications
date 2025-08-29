import pandas as pd
import requests
from bs4 import BeautifulSoup

from bso.server.main.strings import normalize
from bso.server.main.publisher.publisher_detect import detect_publisher
from bso.server.main.logger import get_logger

logger = get_logger(__name__)


def get_domain(url: str) -> str:
    url_with_no_http = url.replace('https://', '').replace('http://', '')
    url_with_no_http = url_with_no_http.replace('www.', '')
    return url_with_no_http.split('.')[0]

def update_list_publishers():
    bealls_publishers = pd.read_json('bso/server/main/predatory/publishers_bealls.json').to_dict(orient='records')
    publishers = []
    for p in bealls_publishers:
        if p.get('url'):
            url = p['url']
            domain = get_domain(url)
            publisher_clean = detect_publisher(p['publisher_raw'], '2020', None)['publisher_normalized']
            publishers.append({'url': url, 'publisher': publisher_clean, 'domain': domain})
            if 'omicsonline' in url:
                other_publisher_clean =  detect_publisher('OMICS Publishing Group', '2020', None)['publisher_normalized']
                publishers.append({'url': url, 'publisher': other_publisher_clean, 'domain': domain})
    publishers.append({'url': 'https://www.hindawi.com/', 'publisher': 'Hindawi', 'domain': 'hindawi.com'})
    df_publishers = pd.DataFrame(publishers)
    return df_publishers.to_dict(orient='records')

def update_list_journals():
    bealls_journals = pd.read_json('bso/server/main/predatory/journals_bealls.json').to_dict(orient='records')
    journals = []
    for j in bealls_journals:
        if j.get('url'):
            url = j['url']
            domain = get_domain(url)
            journals.append({'url': url, 'journal': j['journal_raw'], 'domain': domain})
    df_journals = pd.DataFrame(journals)
    return df_journals.to_dict(orient='records')

#def update_list_publishers() -> dict:
#    publishers_url = 'https://beallslist.net/'
#    publishers = []
#    soup = BeautifulSoup(requests.get(publishers_url).text, 'lxml')
#    for e in soup.find_all('li'):
#        if e.find('a'):
#            a = e.find('a')
#            if 'target' in a.attrs:
#                url = a.attrs['href']
#                domain = get_domain(url)
#                if normalize(domain) in ['mdpi']:
#                    continue
#                name = a.get_text(' ')
#                publisher_clean = detect_publisher(name, '2020', None)['publisher_normalized'] 
#                publishers.append({'url': url, 'publisher': publisher_clean, 'domain': domain})
#                if 'omicsonline' in url:
#                    other_publisher_clean =  detect_publisher('OMICS Publishing Group', '2020', None)['publisher_normalized']
#                    publishers.append({'url': url, 'publisher': other_publisher_clean, 'domain': domain})
#    df_publishers = pd.DataFrame(publishers)
#    return df_publishers.to_dict(orient='records')


#def update_list_journals() -> dict:
#    journals_url = 'https://beallslist.net/standalone-journals/'
#    soup = BeautifulSoup(requests.get(journals_url).text, 'lxml')
#    journals = []
#    for e in soup.find_all('li'):
#        if e.find('a'):
#            a = e.find('a')
#            if 'target' in a.attrs:
#                url = a.attrs['href']
#                domain = get_domain(url)
#                name = a.get_text(' ')
#                journals.append({'url': url, 'journal': name, 'domain': domain})
#    df_journals = pd.DataFrame(journals)
#    return df_journals.to_dict(orient='records')

pred_publishers, pred_journals = [], []
pred_publishers = update_list_publishers()
pred_journals = update_list_journals()

pred_j = [normalize(e['journal']) for e in pred_journals]
pred_p = [normalize(e['publisher']) for e in pred_publishers]
pred_j_domain = [normalize(e['domain']) for e in pred_journals]
pred_p_domain = [normalize(e['domain']) for e in pred_publishers]


def detect_predatory(p_id, publisher: str, journal: str) -> dict:
    predatory_publisher = False
    predatory_journal = False
    publisher_clean = detect_publisher(publisher, '2020', None)['publisher_normalized']
    if isinstance(journal, str) and normalize(journal) == normalize('Journal of Natural Products') and publisher_clean == 'American Chemical Society':
        # two journals have the same name : the one from ACS is not on bealls
        predatory_publisher = False
        predatory_journal = False
    elif isinstance(publisher_clean, str) and normalize(publisher_clean) in pred_p:
        predatory_publisher = True
        predatory_journal = True
    elif isinstance(publisher_clean, str) and normalize(publisher_clean) in pred_p_domain:
        predatory_publisher = True
        predatory_journal = True
    elif isinstance(journal, str) and normalize(journal) in pred_j:
        predatory_journal = True
    elif isinstance(journal, str) and normalize(journal) in pred_j_domain:
        predatory_journal = True
    if 'doi10.3389' in p_id and (predatory_publisher is False):
        logger.debug(f'Strange predatory data;{publisher};{journal}')
    return {'publisher_in_bealls_list': predatory_publisher, 'journal_or_publisher_in_bealls_list': predatory_journal}
