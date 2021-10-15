import pandas as pd
import re
import unidecode
import dateutil.parser

df_publisher = pd.read_csv('bso/server/main/publisher/publisher_correspondance.csv')
df_publisher['group_start_date'] = df_publisher['group_start_date'].apply(lambda x:str(x).replace('.0', ''))
rgx_list = []
for i, row in df_publisher.iterrows():
    rgx_list.append({'pattern': re.compile(row.publisher_raw, re.IGNORECASE),
                     'publisher_normalized': row.publisher_clean,
                     'publisher_group': row.publisher_group,
                     'group_start_date': row.group_start_date})

def detect_publisher(raw_input, published_date):
    unaccented_string = unidecode.unidecode(raw_input).replace(',', ' ').replace('  ', ' ')
    unaccented_string = unaccented_string.replace("’", "'")
    without_parenthesis = re.sub(r'\([^)]*\)', '', unaccented_string)
    tested_string = without_parenthesis.strip()
    if '/' in tested_string:
        return {'publisher_normalized': raw_input, 'publisher_group': raw_input}
    for r in rgx_list:
        if re.search(r['pattern'], tested_string):
            publisher_normalized = r['publisher_normalized']
            publisher_group = r['publisher_normalized']
            if r.get('publisher_group'):
                current_date = dateutil.parser.parse(published_date).isoformat()
                group_date = dateutil.parser.parse(r.get('group_start_date', '19000101')).isoformat()
                print(group_date)
                if current_date >= group_date:
                    publisher_group = r['publisher_group']
            return {'publisher_normalized': publisher_normalized, 'publisher_group': publisher_group}
    return {'publisher_normalized': raw_input, 'publisher_group': raw_input}


