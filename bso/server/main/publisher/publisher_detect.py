import pandas as pd
import re
import unidecode

df_publisher = pd.read_csv('bso/server/main/publisher/publisher_correspondance.csv')

rgx_list = []
for i, row in df.iterrows():
    rgx_list.append({'pattern': re.compile(row.publisher_raw, re.IGNORECASE), 'publisher_normalized': row.publisher_clean})

def detect_publisher(raw_input):
    unaccented_string = unidecode.unidecode(raw_input).replace(',', ' ').replace('  ', ' ')
    unaccented_string = unaccented_string.replace("’", "'")
    without_parenthesis = re.sub(r'\([^)]*\)', '', unaccented_string)
    tested_string = without_parenthesis.strip()
    if '/' in tested_string:
        return {'publisher_normalized': raw_input}
    for r in rgx_list:
        if re.search(r['pattern'], tested_string):
            return r['publisher']
            return {'publisher_normalized': r['publisher_normalized']}
    return {'publisher_normalized': raw_input}
