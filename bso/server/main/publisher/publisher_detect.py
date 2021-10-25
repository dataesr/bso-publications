import pandas as pd
import re
import unidecode
import dateutil.parser

df_publisher_lexical = pd.read_csv('bso/server/main/publisher/publisher_correspondance.csv', skiprows=2)
df_publisher_group = pd.read_csv('bso/server/main/publisher/publisher_group.csv', skiprows=2)
df_publisher = pd.merge(df_publisher_lexical, df_publisher_group, on='publisher_clean', how='left')
df_doi = pd.read_csv('bso/server/main/publisher/publisher_doi.csv', dtype=str)

rgx_list = []
for i, row in df_publisher.iterrows():
    publisher_group = row.publisher_group
    if pd.isnull(publisher_group):
        publisher_group = row.publisher_clean
        
    group_start_date = row.group_start_date
    if pd.isnull(group_start_date):
        group_start_date = 2010.0
    rgx_list.append({'pattern': re.compile(row.publisher_raw, re.IGNORECASE),
                     'publisher_normalized': row.publisher_clean,
                     'publisher_group': publisher_group,
                     'group_start_date': group_start_date,
                     })

doi_map = {}
for i, row in df_doi.iterrows():
    doi_map[row.doi_prefix.strip()]=row.publisher.strip()

def detect_publisher(raw_input, published_date, doi):
    if isinstance(doi, str):
        doi_prefix = doi.lower().strip().split('/')[0]
        if doi_prefix in doi_map:
            return {'publisher_normalized': doi_map[doi_prefix], 'publisher_group': doi_map[doi_prefix]}
    if not isinstance(raw_input, str):
        return {'publisher_normalized': raw_input, 'publisher_group': raw_input}
    unaccented_string = unidecode.unidecode(raw_input).replace(',', ' ').replace('  ', ' ')
    unaccented_string = unaccented_string.replace("’", "'")
    without_parenthesis = re.sub(r'\([^)]*\)', '', unaccented_string)
    tested_string = without_parenthesis.strip()
    if pd.isnull(published_date):
        published_date = '2010'
    if '/' in tested_string:
        return {'publisher_normalized': raw_input, 'publisher_group': raw_input}
    for r in rgx_list:
        if re.search(r['pattern'], tested_string):
            publisher_normalized = r['publisher_normalized']
            publisher_group = r['publisher_normalized']
            try:
                current_date = dateutil.parser.parse(published_date).isoformat()
            except:
                current_date = dateutil.parser.parse('20100101').isoformat()
            group_year = str(r.get('group_start_date')).replace('.0', '')
            group_date = dateutil.parser.parse(f'{group_year}0101').isoformat()
            if current_date >= group_date:
                publisher_group = r['publisher_group']
            return {'publisher_normalized': publisher_normalized, 'publisher_group': publisher_group}
    return {'publisher_normalized': raw_input, 'publisher_group': raw_input}
