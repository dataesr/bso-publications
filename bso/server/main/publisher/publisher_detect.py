import pandas as pd
import re
import unidecode
import dateutil.parser

df_publisher_lexical = pd.read_csv('bso/server/main/publisher/publisher_correspondance.csv', skiprows=2)
df_publisher_group = pd.read_csv('bso/server/main/publisher/publisher_group.csv', skiprows=2)
df_publisher_dissemination = pd.read_csv('bso/server/main/publisher/publisher_dissemination.csv', skiprows=2)

df_group = pd.merge(df_publisher_lexical, df_publisher_group, on='publisher_clean', how='left')
for i, row in df_group.iterrows():
    if pd.isnull(row.publisher_group):
        df_group.at[i, 'publisher_group'] = row.publisher_clean

    if pd.isnull(row.group_start_date):
        df_group.at[i, 'group_start_date'] = 2010.0

df_dissemination = pd.merge(df_group, df_publisher_dissemination, on='publisher_group', how='left')
for i, row in df_dissemination.iterrows():
    if pd.isnull(row.publisher_dissemination):
        df_dissemination.at[i, 'publisher_dissemination'] = row.publisher_group

    if pd.isnull(row.dissemination_start_date):
        df_dissemination.at[i, 'dissemination_start_date'] = row.group_start_date

rgx_list = []
for i, row in df_dissemination.iterrows():
    rgx_list.append({'pattern': re.compile(row.publisher_raw, re.IGNORECASE),
                     'publisher_normalized': row.publisher_clean,
                     'publisher_group': row.publisher_group,
                     'publisher_dissemination': row.publisher_dissemination,
                     'group_start_date': row.group_start_date,
                     'dissemination_start_date': row.dissemination_start_date
                     })

df_doi = pd.read_csv('bso/server/main/publisher/publisher_doi.csv', dtype=str)
doi_map = {}
for i, row in df_doi.iterrows():
    doi_map[row.doi_prefix.strip()]=row.publisher.strip()

def detect_publisher(raw_input, published_date, doi):
    raw_input_to_normalize = raw_input
    if isinstance(doi, str):
        doi_prefix_short = doi.lower().strip().split('/')[0]
        doi_prefix_long = '.'.join(doi.lower().strip().split('.')[0:2])
        # order is important ; doi_prefix_long to catch openedition books with 10.4000/books.*
        for doi_prefix in [doi_prefix_long, doi_prefix_short]:
            if doi_prefix in doi_map:
                publisher_doi_prefix = doi_map[doi_prefix]
                raw_input_to_normalize = publisher_doi_prefix
                break
    if not isinstance(raw_input, str):
        return {'publisher_normalized': raw_input_to_normalize, 'publisher_group': raw_input_to_normalize, 'publisher_dissemination': raw_input_to_normalize}
    unaccented_string = unidecode.unidecode(raw_input_to_normalize).replace(',', ' ').replace('  ', ' ')
    unaccented_string = unaccented_string.replace("’", "'")
    without_parenthesis = re.sub(r'\([^)]*\)', '', unaccented_string)
    tested_string = without_parenthesis.strip()
    if pd.isnull(published_date):
        published_date = '2010'
    if '/' in tested_string:
        return {'publisher_normalized': raw_input_to_normalize, 'publisher_group': raw_input_to_normalize, 'publisher_dissemination': raw_input_to_normalize}
    for r in rgx_list:
        if re.search(r['pattern'], tested_string):
            publisher_normalized = r['publisher_normalized']
            publisher_group = r['publisher_normalized']
            publisher_dissemination = r['publisher_normalized']
            try:
                current_date = dateutil.parser.parse(published_date).isoformat()
            except:
                current_date = dateutil.parser.parse('20100101').isoformat()
            
            group_year = str(r.get('group_start_date')).replace('.0', '')
            group_date = dateutil.parser.parse(f'{group_year}0101').isoformat()
            if current_date >= group_date:
                publisher_group = r['publisher_group']
            
            dissemination_year = str(r.get('dissemination_start_date')).replace('.0', '')
            dissemination_date = dateutil.parser.parse(f'{dissemination_year}0101').isoformat()
            if current_date >= dissemination_date:
                publisher_dissemination = r['publisher_dissemination']
            return {'publisher_normalized': publisher_normalized, 'publisher_group': publisher_group, 'publisher_dissemination': publisher_dissemination}
    return {'publisher_normalized': raw_input_to_normalize, 'publisher_group': raw_input_to_normalize, 'publisher_dissemination': raw_input_to_normalize}
