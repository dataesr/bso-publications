import json
import os
import pandas as pd
import requests

from retry import retry
from bso.server.main.utils_swift import download_object, upload_object_with_destination
from bso.server.main.utils import to_jsonl

from bso.server.main.logger import get_logger

logger = get_logger(__name__)

DATAESR_HEADER = os.getenv('DATAESR_HEADER')

genre_dict = {}
cor = {}
dew = pd.read_csv('/src/bso/server/main/dewey_discipline.csv')
dew.at[46, 'dewey'] = 'Sciences humaines'
for i, row in dew.iterrows():
    cor[row['index']] = row['dewey'].strip()

def extract_genre():
    global genre_dict
    cmd = f"cat /upw_data/scanr/persons_denormalized.jsonl | jq  -r '[.idref,.gender,.firstName,.lastName]|@csv' | grep -v ',,' > /upw_data/scanr/gender.csv"
    os.system(cmd)
    df = pd.read_csv('/upw_data/scanr/gender.csv', header=None, names=['idref', 'gender', 'first_name', 'last_name'])
    for r in df.itertuples():
        genre_dict[r.idref] = r.gender


def compute_genre(args):
    extract_genre()
    if args.get('parse', False):
        parse_these_with_genre(args)
    if args.get('stats', False):
        compute_stats(args)

def compute_stats(args):
    dump_date = args.get('dump_date')
    dump_year = int(dump_date[0:4])
    data = []
    for current_file in os.listdir(f'/upw_data/theses/{dump_date}/parsed'):
        new_current_file = current_file.replace('.json.gz', '_with_genre.jsonl')
        download_object('theses', f'{dump_date}/genre/{new_current_file}', new_current_file)
        data.append(pd.read_json(new_current_file, lines=True))
    df_these = pd.concat(data)
    logger.debug(f'{len(df_these)} theses loaded')

    # 1. Auteur des thèses
    # 1.1 Evolution par année de soutenance
    os.system(f"echo '1.1 Evolution par année de soutenance' > results_these_genre_{dump_date}")
    df_piv = pd.DataFrame(pd.pivot_table(df_these[df_these.annee>=2010], index=['annee'], columns=['genre_auteur'], values='nb_auteur', aggfunc='sum')).reset_index()
    df_piv['perc_f'] = df_piv.F/(df_piv.F + df_piv.M)
    df_piv.sort_values(by='annee').to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')

    # 1.2 Distinction par champ disciplinaire (2010-dump_year-1)
    os.system(f"echo '1.2 Distinction par champ disciplinaire (2010-{dump_year-1})' >> results_these_genre_{dump_date}")
    df_piv = pd.DataFrame(pd.pivot_table(df_these[(df_these.annee>=2010) & (df_these.annee<=dump_year-1)], index=['my_dewey'], columns=['genre_auteur'], values='nb_auteur', aggfunc='sum')).reset_index()
    df_piv['perc_f'] = df_piv.F/(df_piv.F + df_piv.M)
    df_piv.sort_values(by='perc_f', ascending=False).to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')

    # 2.1 Nombre de thèses encadrées par au moins une femme
    os.system(f"echo '2.1 These encadree au moins par une femme par annee' >> results_these_genre_{dump_date}")
    pd.pivot_table(df_these[(df_these.annee>=2010) & (df_these.annee<=dump_year-3)],
               index=['annee'],
               values=['dir_F', 'dir_M', 'has_dir_with_genre'],
               aggfunc='sum').to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')
    
    # 2.2 par discipline
    os.system(f"echo '2.2 These encadree au moins par une femme par discpline (2010 - {dump_year - 3})' >> results_these_genre_{dump_date}")
    pd.pivot_table(df_these[(df_these.annee>=2010) & (df_these.annee<=dump_year - 3)],
               index=['my_dewey'],
               values=['dir_F', 'dir_M', 'has_dir_with_genre'],
               aggfunc='sum').to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')


    # 3. Nombre de directeur.trice distinct (quel que soit le nombre de thèses encadrées)
    discip_annee = {}
    annee = {}
    discip = {}

    for i, row in df_these.iterrows():

        if row['annee'] < 2010:
            continue

        if row['annee'] > dump_year-3:
            continue

        if row['annee'] not in annee:
            annee[row['annee']] = {'F': set([]), 'M': set([])}

        if row['my_dewey'] not in discip:
            discip[row['my_dewey']] = {'F': set([]), 'M': set([])}

        directeur_F, directeur_M = [], []
        if isinstance(row['dir_F_idref'], str):
            directeur_F = [k for k in row['dir_F_idref'].split(';') if len(k)>5]
        if isinstance(row['dir_M_idref'], str):
            directeur_M = [k for k in row['dir_M_idref'].split(';') if len(k)>5]

        annee[row['annee']]['F'].update(directeur_F)
        annee[row['annee']]['M'].update(directeur_M)

        discip[row['my_dewey']]['F'].update(directeur_F)
        discip[row['my_dewey']]['M'].update(directeur_M)

    # 3.1 par année
    vivier_annee = []
    for y in annee:
        new_line = {
            'annee': y,
            'nb_directeur_F': len(annee[y]['F']),
            'nb_directeur_H': len(annee[y]['M'])
        }
        vivier_annee.append(new_line)
    os.system(f"echo '3.1 Vivier par année' >> results_these_genre_{dump_date}")
    pd.DataFrame(vivier_annee).sort_values(by='annee').to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')

    # 3.2 par discipline
    vivier_discip = []
    for y in discip:
        new_line = {
            'discipline': y,
            'nb_directeur_F': len(discip[y]['F']),
            'nb_directeur_H': len(discip[y]['M']),
        }
        vivier_discip.append(new_line)
    os.system(f"echo '3.2 Vivier par discipline (2010 - {dump_year - 3})' >> results_these_genre_{dump_date}")
    pd.DataFrame(vivier_discip).to_csv('tmp')
    os.system(f'cat tmp >> results_these_genre_{dump_date}')
    os.system(f'echo '' >> results_these_genre_{dump_date}')
    logger.debug(f'get results_these_genre_{dump_date}')
    upload_object_with_destination('theses', f'results_these_genre_{dump_date}', f'{dump_date}/genre/results_these_genre_{dump_date}')


def parse_these_with_genre(args):
    dump_date = args.get('dump_date')
    for current_file in os.listdir(f'/upw_data/theses/{dump_date}/parsed'):
        current_theses=[]
        logger.debug(f'reading {current_file}')
        current_df = pd.read_json(f'/upw_data/theses/{dump_date}/parsed/{current_file}')
        current_data = current_df[~pd.isna(current_df.year)].to_dict(orient='records')
        for ix, d in enumerate(current_data):
            year = d['year']
            if year < 2010:
                continue
            discipline = None
            dewey = None
            for t in d.get('classifications'):
                if t.get('reference')=="degree discipline":
                    discipline = t.get("label_fr")
                    break
            for t in d.get('classifications'):
                if t.get('reference')=="dewey" and "00" not in t.get('code'):
                    dewey = t.get("label_fr")
                    break
            if not isinstance(d.get('authors'), list):
                continue
            auteur = [x for x in d.get('authors', []) if x.get('role')=="author"][0]
            directeurs = [x for x in d.get('authors', []) if x.get('role')=="directeurthese"]
            my_dewey = get_disc(dewey, discipline)
            
            genre_auteur = get_genre(auteur)
            auteur['genre'] = genre_auteur
            if 'affiliations' in auteur:
                del auteur['affiliations']

            nb_directeur = len(directeurs)
            dir_F, dir_M, dir_U = 0, 0, 0
            has_dir_with_genre = 0
            directrices_idref, directeurs_idref = [], []

            for k in directeurs:
                genre_dir = get_genre(k)
                k['genre'] = genre_dir
                if genre_dir == "F":
                    dir_F = 1
                    has_dir_with_genre = 1
                    if k.get('idref'):
                        directrices_idref.append(k['idref'])
                elif genre_dir == "M":
                    dir_M = 1
                    has_dir_with_genre = 1
                    if k.get('idref'):
                        directeurs_idref.append(k['idref'])
                elif genre_dir == "unknown":
                    dir_U += 1
                else:
                    logger.debug("PROBLEME")
                    logger.debug(d)

            new_line = {
                "annee": year,
                "directeurs": directeurs,
                "auteur": auteur,
                "genre_auteur": genre_auteur,
                "nb_auteur": 1,
                "nb_directeur": nb_directeur,
                "dir_F": dir_F,
                "dir_M": dir_M,
                "dir_F_idref": ";".join(directrices_idref),
                "dir_M_idref": ";".join(directeurs_idref),
                "has_dir_with_genre": has_dir_with_genre,
                "discipline": discipline,
                "dewey": dewey,
                "my_dewey": my_dewey,
                "these_id": d['nnt_id']
                   }
            current_theses.append(new_line)
            if ix and ix % 100 == 0:
                logger.debug(f'{ix}/{len(current_data)}')
        new_current_file = current_file.replace('.json.gz', '_with_genre.jsonl')
        to_jsonl(current_theses, new_current_file)
        upload_object_with_destination('theses', new_current_file, f'{dump_date}/genre/{new_current_file}')
        os.system(f'rm -rf {new_current_file}')

def get_disc(dewey, discipline):
    if discipline and 'informatique' in discipline.lower():
        return 'Informatique'
    if dewey in cor:
        return cor[dewey]
    if discipline is None:
        return 'Autre'
    if 'histoire' in discipline.lower() or 'littérature' in discipline.lower() or 'lettre' in discipline.lower():
        return 'Sciences humaines'
    if 'art' in discipline.lower() or 'relig' in discipline.lower() or 'philo' in discipline.lower() or 'lang' in discipline.lower():
        return 'Sciences humaines'
    if 'socio' in discipline.lower() or 'droit' in discipline.lower() or 'éco' in discipline.lower():
        return 'Sciences sociales'

    for x in dew.dewey.unique().tolist():
        if x.lower() in discipline.lower():
            return x

    return 'Autre'

@retry(delay=200, tries=3)
def get_genre(person):
    global genre_dict
    assert(len(genre_dict)>1000)
    header = {'Authorization': f'Basic {DATAESR_HEADER}' }
    if 'idref' in person and len(person['idref'])>5:
        if person['idref'] in genre_dict:
            gd = genre_dict[person['idref']]
            if gd in ['M', 'F']:
                return gd
        #idref = 'idref'+person['idref']
        #r_get = requests.get("http://185.161.45.213/persons/persons/"+idref, headers=header)
        #res_idref = r_get.json()
        #if 'gender' in res_idref and res_idref['gender'] in ['M', 'F']:
        #    return res_idref['gender']

    name = person.get('first_name')
    if name is None or len(name) < 3:
        name = person.get('full_name')

    url = "http://185.161.45.213/persons/persons/_gender?q=" + name
    r_detect = requests.get(url, headers=header)
    res = r_detect.json()
    if res.get('status') == 'detected':
        if len(res['data']) > 1:
            print('PROBLEME')
            print(person)
            print()
        return res['data'][0]['gender']
    return "unknown"

