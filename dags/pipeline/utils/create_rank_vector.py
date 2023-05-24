import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def contain(lst, item):
    return item in lst

def join_col(x):
    return ' '.join(x['attraction_types']) + ' '.join(x['activities'])

def concat_list(x):
    return x['attraction_types'] + x['activities']

def word_transform(items):
    whitelist = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ&')
    answer = []
    for item in items:
        answer.append(''.join(filter(whitelist.__contains__, item)).rstrip().lower())
    return answer

def calc_rank(feature, attractions):
    df = attractions[attractions.features.apply(lambda x: contain(x, feature))].sort_values('popularity', ascending=False).copy()
    df.loc[:, 'rank'] = range(len(df))
    return df

def norm_rank(feature, attractions):
    df = attractions[attractions.features.apply(lambda x: contain(x, feature))].copy()
    normalized_rank = 1 - (calc_rank(feature, attractions)['rank']/len(df))
    df.loc[:, feature] = normalized_rank
    return df[['place_id', feature]].sort_values(feature, ascending=False)

def create_rank_vector(ti):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_pop = ti.xcom_pull(key='data_popularity', task_ids='get_popularity')

    df_pif = pd.read_json(data_pif, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    df_pop = pd.DataFrame(eval(data_pop))

    attractions = df_p[df_p.category_code == 'ATTRACTION']
    attractions = attractions.merge(df_pif[['place_id', 'attraction_types', 'activities']], how='left', on='place_id')
    attractions = attractions.merge(df_pop, how="inner", on=["place_id", "place_name", "destination"])

    attractions.activities = attractions.activities.apply(lambda x: word_transform(x) if isinstance(x, list) else [])
    attractions.attraction_types = attractions.attraction_types.apply(lambda x: word_transform(x) if isinstance(x, list) else [])

    attractions['features'] = attractions.apply(concat_list, axis=1).tolist()

    df = attractions[['place_id', 'place_name', 'features', 'popularity']]
    df_rank_vec = attractions.loc[:,['place_id']]
    attraction_type = attractions.features.explode().unique()

    for type in attraction_type:
        df_rank_vec = pd.merge(df_rank_vec, norm_rank(type, df), how='left')
        
    df_rank_vec.fillna(0, axis=1, inplace=True)
    df_rank_vec.drop(np.nan, axis=1, inplace=True)

    db = create_engine('postgresql://data:data@postgres-data/data', client_encoding='utf8')
    conn = db.connect()

    df_rank_vec.to_sql('attraction_rank_vector', con=conn, if_exists='replace', index=False)
    df_pop.to_sql('popularity', con=conn, if_exists='replace', index=False)
    
    # ti.xcom_push(key="rank_vector", value=df_rank_vec.to_json(orient='records'))
    # print(df_rank_vec)
    
