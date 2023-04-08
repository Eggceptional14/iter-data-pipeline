import json
import pandas as pd


def split_category(ti):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')

    df_pif = pd.read_json(data_pif, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    print(df_pif.info())
    print(df_p.info())
    
    #split to accommodation and its related tables
    is_type_accom = df_pif.category_code == 'ACCOMMODATION'

    df_accom = df_pif[is_type_accom][['place_id', 'register_license_id', 'hotel_star', 'display_checkin_time', 'display_checkout_time', 'number_of_rooms', 'price_range']].copy()
    df_accom['standard'] = df_p[is_type_accom]['standard']
    df_accom['awards'] = df_p[is_type_accom]['awards']
    df_accom['hit_score'] = df_p[is_type_accom]['hit_score']

    # print(df_accom.info())
    df_accom_type = df_pif[is_type_accom & (~df_pif.accommodation_types.isna())][['place_id', 'accommodation_types']].copy()
    df_accom_type = df_accom_type[~df_accom_type.accommodation_types.isna()].explode('accommodation_types')
    df_accom_type.reset_index(drop=True, inplace=True)
    df_accom_type.rename(columns={'accommodation_types': 'description'}, inplace=True)
    # print(df_accom_type.head())

    #split to attraction and its related tables
    is_type_attr = df_pif.category_code == 'ATTRACTION'

    df_attr = df_p[is_type_attr][['place_id', 'hit_score']].copy()

    df_attr_type = df_pif[is_type_attr & (~df_pif.attraction_types.isna())][['place_id', 'attraction_types']].copy()
    df_attr_type = df_attr_type[~df_attr_type.attraction_types.isna()].explode('attraction_types')
    df_attr_type.reset_index(drop=True, inplace=True)
    df_attr_type.rename(columns={'attraction_types': 'description'}, inplace=True)
    # print(df_attr_type.head())

    #split to restaurant and its related tables
    is_type_res = df_pif.category_code == 'RESTAURANT'

    df_res = df_p[is_type_res][['place_id', 'standard', 'awards', 'hit_score']]

    df_res_type = df_pif[is_type_res & (~df_pif.restaurant_types.isna())][['place_id', 'restaurant_types']].copy()
    df_res_type = df_res_type[~df_res_type.restaurant_types.isna()].explode('restaurant_types')
    df_res_type.reset_index(drop=True, inplace=True)
    df_res_type.rename(columns={'restaurant_types': 'description'}, inplace=True)
    # print(df_res_type.head())


    df_cuisine_type = df_pif[is_type_res][['place_id', 'cuisine_types']].copy()
    df_cuisine_type = df_cuisine_type[~df_cuisine_type.cuisine_types.isna()].explode('cuisine_types')
    df_cuisine_type.reset_index(drop=True, inplace=True)
    df_cuisine_type.rename(columns={'cuisine_types': 'description'}, inplace=True)
    df_cuisine_type = df_cuisine_type[~df_cuisine_type.description.isna()]
    # print(df_cuisine_type.info())

    #split to shop and its related tables
    is_type_shop = df_pif.category_code == 'SHOP'

    df_shop = df_p[is_type_shop][['place_id', 'standard']]

    df_shop_type = df_pif[is_type_shop & (~df_pif.shop_types.isna())][['place_id', 'shop_types']].copy()
    df_shop_type = df_shop_type[~df_shop_type.shop_types.isna()].explode('shop_types')
    df_shop_type.reset_index(drop=True, inplace=True)
    df_shop_type.rename(columns={'shop_types': 'description'}, inplace=True)
    # print(df_shop_type.head())

    #split target table
    df_target = df_pif[~df_pif.targets.isna()][['place_id', 'targets']].copy()
    df_target = df_target.explode('targets')
    df_target.reset_index(drop=True, inplace=True)
    df_target.rename(columns={'targets': 'description'}, inplace=True)
    # print(df_target.head())

    #split activity table
    df_activity = df_pif[~df_pif.activities.isna()][['place_id', 'activities']].copy()
    df_activity = df_activity.explode('activities')
    df_activity.reset_index(drop=True, inplace=True)
    df_activity.rename(columns={'activities': 'description'}, inplace=True)
    # print(df_activity.head())

    #create place table
    df_place_final = df_p.drop(columns=['standard', 'awards', 'hit_score'])
    df_place_final['introduction'] = df_pif['introduction']
    df_place_final['detail'] = df_pif['detail']
    # print(df_place_final.category_code.value_counts())
    # df_place_final = df_place_final[df_place_final.category_code != 'OTHER']
    # print(df_place_final.category_code.value_counts())


    ti.xcom_push(key="data_accom", value=df_accom.to_json(orient='records'))
    ti.xcom_push(key="data_accom_type", value=df_accom_type.to_json(orient='records'))

    ti.xcom_push(key="data_attr", value=df_attr.to_json(orient='records'))
    ti.xcom_push(key="data_attr_type", value=df_attr_type.to_json(orient='records'))

    ti.xcom_push(key="data_res", value=df_res.to_json(orient='records'))
    ti.xcom_push(key="data_res_type", value=df_res_type.to_json(orient='records'))
    ti.xcom_push(key="data_cuisine_type", value=df_cuisine_type.to_json(orient='records'))

    ti.xcom_push(key="data_shop", value=df_shop.to_json(orient='records'))
    ti.xcom_push(key="data_shop_type", value=df_shop_type.to_json(orient='records'))

    ti.xcom_push(key="data_target", value=df_target.to_json(orient='records'))
    ti.xcom_push(key="data_activity", value=df_activity.to_json(orient='records'))

    ti.xcom_push(key="data_place_fv", value=df_place_final.to_json(orient='records'))


    'alter table cuisine_type alter column description type varchar(256)[] USING description::character varying(256)[];'

