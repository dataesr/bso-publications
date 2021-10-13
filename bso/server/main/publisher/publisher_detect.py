import pandas as pd

df_publisher = pd.read_csv('bso/server/main/publisher/publisher_correspondance.csv')
publisher_map = {}
for i, row in df_publisher.iterrows():
    raw = row.publisher_raw.strip()
    target = row.publisher.strip()
    publisher_map[raw] = target

# normalise le nom de l'éditeur

def detect_publisher(publisher: str) -> str:
    publisher_normalized = publisher_map.get(publisher.strip(), publisher)   
    return {'publisher_normalized': publisher_normalized}
