import os

import fasttext

from bso.server.main.apc.apc_detect import detect_apc
from bso.server.main.predatory.predatory_detect import detect_predatory
from bso.server.main.strings import remove_punction
from bso.server.main.unpaywall_mongo import get_doi_full
from bso.server.main.utils import download_file
from bso.server.main.utils_upw import chunks, format_upw_millesime

PV_MOUNT = '/src/models/'
models = {}
os.system(f'mkdir -p {PV_MOUNT}')
project_id = os.getenv('OS_TENANT_ID')


def init_model_lang() -> None:
    print('init model lang', flush=True)
    lid_model_name = f'{PV_MOUNT}lid.176.bin'
    if not os.path.exists(lid_model_name):
        download_file(f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/models/lid.176.bin',
                      upload_to_object_storage=False, destination=lid_model_name)
    lid_model = fasttext.load_model(lid_model_name)
    models['lid'] = lid_model


def identify_language(text: str) -> str:
    if 'lid' not in models:
        init_model_lang()
    if len(text) < 3 or text is None:
        return None
    text = remove_punction(text.replace('\n', ' ').replace('\xa0', ' ')).strip()
    return (models['lid'].predict(text, 1)[0][0]).replace('__label__', '')


def format_upw(dois_infos: dict, extra_data: dict) -> list:
    final = []
    for doi in dois_infos:
        if 'global' not in dois_infos[doi]:
            res = {'doi': doi}
        else:
            res = dois_infos[doi]['global']
        if doi in extra_data:
            res.update(extra_data[doi])
        if 'z_authors' in res:
            if 'authors' in res:
                # todo implement a merge 
                del res['z_authors']
            else:
                res['authors'] = res['z_authors']
                del res['z_authors']
        # apc
        info_apc = detect_apc(doi, res.get('journal_issns'), res.get('published_date', '2020-01-01'))
        res.update(info_apc)
        # language
        if 'language' not in res or len(res['language']) < 2:
            publi_title_abstract = ''
            if res.get('title'):
                publi_title_abstract += res.get('title') + ' '
            if res.get('abstract'):
                publi_title_abstract += res.get('abstract')
            if len(publi_title_abstract) > 5:
                res['language'] = identify_language(publi_title_abstract.strip())
        # predatory info
        pred_info = detect_predatory(res.get('publisher'), res.get('journal_name'))
        res.update(pred_info)
        # retraction info
        # retraction_info = detect_retraction(x.get('doi'), x.get('pmid'))
        # res.update(retraction_info)
        res['oa_details'] = []
        for asof in dois_infos[doi]:
            if asof == 'global':
                continue
            else:
                tmp = format_upw_millesime(dois_infos[doi][asof], asof, res['has_apc'])
                res['oa_details'].append(tmp)
        final.append(res)
    return final


def enrich(publications: list) -> list:
    publis_dict = {}
    for p in publications:
        if 'doi' in p:
            doi = p['doi'].lower()
            publis_dict[doi] = p
    all_updated = []
    for publi_chunk in chunks(publications, 100):
        doi_chunk = [p.get('doi') for p in publi_chunk if ('doi' in p and '10' in p['doi'])]
        data = get_doi_full(doi_chunk)
        all_updated += format_upw(data, publis_dict)
    return all_updated
