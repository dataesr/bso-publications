import pandas as pd

from bso.server.main.logger import get_logger

logger = get_logger(__name__)

def json_to_csv(json_file, observation_date, split_year = False):
    df = pd.read_json(json_file, lines=True, chunksize = 25000)
    output_csv_file = json_file.replace('.jsonl', '.csv')
    ix = 0
    for c in df:
        if ix == 0:
            write_header = True
        else:
            write_header = False
        pandas_to_csv(c, observation_date, output_csv_file, write_header, split_year)
        ix += 1
    return output_csv_file


def pandas_to_csv(df, observation_date, filename, write_header=True, split_year = False):

    simple_fields = ['id','doi', 'pmid', 'hal_id', 'year', 'title',
                     'journal_issns', 'journal_issn_l', 'journal_name', 'publisher', 'publisher_dissemination',
                     'bso_classification', 'lang', 'genre',
                    'amount_apc_EUR', 'apc_source']
    array_fields = ['domains', 'detected_countries', 'bso_local_affiliations']

    flatten_data = []
    for elem in df.to_dict(orient='records'):
        new_elem = {'observation_date': observation_date}
        id_elem = elem['id']
        for f in simple_fields:
            if isinstance(elem.get(f), str):
                new_elem[f] = elem[f].replace('\u2028',' ').replace('\n', ' ').replace('  ', ' ')
            elif elem.get(f):
                new_elem[f] = elem[f]
            else:
                new_elem[f] = None

        for f in array_fields:
            if isinstance(elem.get(f), list):
                new_elem[f] = ';'.join(elem[f])
            else:
                new_elem[f] = None

        funding_anr, funding_europe = [], []
        grants = elem.get('grants')
        if isinstance(grants, list):
            for g in grants:
                if g.get('agency') == 'ANR':
                    funding_anr.append(g.get('grantid'))
                if g.get('agency') == 'H2020':
                    funding_europe.append(g.get('grantid'))
        new_elem['funding_anr'] = ';'.join(list(set(funding_anr)))
        new_elem['funding_europe'] = ';'.join(list(set(funding_europe)))


        if isinstance(elem.get('bsso_classification'), dict) and isinstance(elem['bsso_classification'].get('field'), list):
            new_elem['bsso_classification'] = "###".join(elem['bsso_classification'].get('field'))
        else:
            new_elem['bsso_classification'] = None

        if 'oa_details' in elem and observation_date in elem['oa_details']:
            new_elem['is_oa'] = elem['oa_details'][observation_date].get('is_oa')
            new_elem['oa_host_type'] = elem['oa_details'][observation_date].get('oa_host_type')
            new_elem['journal_is_in_doaj'] = elem['oa_details'][observation_date].get('journal_is_in_doaj')
            new_elem['journal_is_oa'] = elem['oa_details'][observation_date].get('journal_is_oa')
            new_elem['unpaywall_oa_status'] = elem['oa_details'][observation_date].get('unpaywall_oa_status')

            new_elem['oa_colors'] = ';'.join(elem['oa_details'][observation_date].get('oa_colors', []))
            new_elem['licence_publisher'] = ';'.join(elem['oa_details'][observation_date].get('licence_publisher', []))
            new_elem['licence_repositories'] = ';'.join(elem['oa_details'][observation_date].get('licence_repositories', []))
            new_elem['repositories'] = ';'.join(elem['oa_details'][observation_date].get('repositories', []))
        else:
            #print(f'no oa_details for {id_elem}')
            for f in ['is_oa', 'oa_host_type', 'journal_is_in_doaj', 'journal_is_oa', 'unpaywall_oa_status',
                     'oa_colors', 'licence_publisher', 'licence_repositories', 'repositories']:
                new_elem[f] = None

        new_elem['software_mentions'] = None
        new_elem['data_mentions'] = None
        for g in ['software', 'data']:
            for t in ['used', 'shared', 'created']:
                new_elem[f'{g}_{t}'] = None

        if isinstance(elem.get('softcite_details'), dict):
            new_elem['software_used'] = elem['softcite_details'].get('has_used')
            new_elem['software_shared'] = elem['softcite_details'].get('has_shared')
            new_elem['software_created'] = elem['softcite_details'].get('has_created')
            mentions = elem['softcite_details'].get('mentions')
            if isinstance(mentions, list):
                new_elem['software_mentions'] = ";".join(list(set([k['name'] for k in mentions if 'name' in k])))
        if isinstance(elem.get('datastet_details'), dict):
            new_elem['data_used'] = elem['datastet_details'].get('has_used')
            new_elem['data_shared'] = elem['datastet_details'].get('has_shared')
            new_elem['data_created'] = elem['datastet_details'].get('has_created')
            mentions = elem['datastet_details'].get('mentions')
            if isinstance(mentions, list):
                new_elem['data_mentions'] = ";".join(list(set([k['name'] for k in mentions if 'name' in k])))

        flatten_data.append(new_elem)
    final_cols = ['observation_date', 'id', 'doi', 'pmid', 'hal_id', 'year', 'title',
       'journal_issns', 'journal_issn_l', 'journal_name', 'publisher',
       'publisher_dissemination', 'bso_classification', 'lang', 'genre',
       'amount_apc_EUR', 'apc_source', 'domains', 'detected_countries',
       'bso_local_affiliations', 'funding_anr', 'funding_europe',
       'bsso_classification', 'is_oa', 'oa_host_type', 'journal_is_in_doaj',
       'journal_is_oa', 'unpaywall_oa_status', 'oa_colors',
       'licence_publisher', 'licence_repositories', 'repositories',
       'software_mentions', 'data_mentions',
       'software_used', 'software_created', 'software_shared',
       'data_used', 'data_created', 'data_shared'
       ]
    df_flatten = pd.DataFrame(flatten_data)[final_cols]

    if write_header:
        df_flatten.to_csv(filename, sep=';', index=False)
    else:
        df_flatten.to_csv(filename, sep=';', index=False, header=False, mode='a')

    if split_year:
        first_year = 2013
        last_year = int(observation_date[0:4])
        for y in range(first_year, last_year+1):
            df_flatten_year = df_flatten[df_flatten.year==y]
            filename_year = filename.replace('.csv', f'_{y}.csv')
            if write_header:
                df_flatten_year.to_csv(filename_year, sep=';', index=False)
            else:
                df_flatten_year.to_csv(filename_year, sep=';', index=False, header=False, mode='a')

