import pandas as pd


def location_cleaning(ti):
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    location_df = pd.read_json(data, orient='records')
    # print(location_df)

    provinces = set(location_df.province)
    for province in provinces:
        location_df.province = location_df.province.replace(province, province.upper(), regex=True)
    
    district = set(location_df.district)
    for dis in district:
        location_df.district = location_df.district.replace(dis, dis.upper())

    subdis = set(location_df.sub_district)
    for sub in subdis:
        location_df.sub_district = location_df.sub_district.replace(sub, sub.upper())

    # print(location_df[["province", "district", "sub_district"]].head(10))
    print(location_df.head(10))
    location_df = location_df[~location_df.place_id.isna()]
    out = location_df.to_json(orient="records")

    ti.xcom_push(key='location_df', value=out)

def sha_cleaning(ti):
    data = ti.xcom_pull(key='data_sha', task_ids='split_nested')
    sha_df = pd.read_json(data, orient='records')

    sha_df.drop(sha_df[sha_df['sha_name'] == ""].index, inplace=True)

    print(sha_df.head(10))
    sha_df = sha_df[~sha_df.place_id.isna()]
    out = sha_df.to_json(orient="records")
    ti.xcom_push(key='sha_df', value=out)
    
def contact_cleaning(ti):
    data = ti.xcom_pull(key='data_contact', task_ids='split_nested')
    contact_df = pd.read_json(data, orient='records')
    # print(data)
    # print(contact_df)

    contact_df.phones = contact_df.phones.apply(lambda d: d if isinstance(d, list) else [])
    contact_df.mobiles = contact_df.mobiles.apply(lambda d: d if isinstance(d, list) else [])
    contact_df.emails = contact_df.emails.apply(lambda d: d if isinstance(d, list) else [])
    contact_df.urls = contact_df.urls.apply(lambda d: d if isinstance(d, list) else [])

    contact_df.fax = contact_df.fax.fillna("")

    print(contact_df.head(10))
    contact_df = contact_df[~contact_df.place_id.isna()]
    out = contact_df.to_json(orient="records")
    ti.xcom_push(key='contact_df', value=out)
    
def facilities_cleaning(ti):
    data = ti.xcom_pull(key='data_facility', task_ids='split_nested')
    facilities_df = pd.read_json(data, orient='records')

    new_col = []
    for index, row in facilities_df.drop(columns=['place_id']).iterrows():
        tmp = []
        for item in row:
            # print(item)
            if item != None:
                tmp.append(item['description'])
        new_col.append(tmp)

    facilities_df['description'] = new_col
    facilities_df = facilities_df.loc[:, ['place_id', 'description']]
    facilities_df = facilities_df.explode('description')
    facilities_df.reset_index(drop=True, inplace=True)
    facilities_df = facilities_df[~facilities_df.description.isna()]

    print(facilities_df.head(10))
    facilities_df = facilities_df[~facilities_df.place_id.isna()]
    out = facilities_df.to_json(orient="records")
    ti.xcom_push(key='facilities_df', value=out)

def services_cleaning(ti):
    data = ti.xcom_pull(key='data_service', task_ids='split_nested')
    services_df = pd.read_json(data, orient='records')
    
    new_col = []
    for index, row in services_df.drop(columns=['place_id']).iterrows():
        tmp = []
        for item in row:
            if item != None:
                tmp.append(item['description'])
        new_col.append(tmp)

    services_df['description'] = new_col
    services_df = services_df.loc[:, ['place_id', 'description']]
    services_df = services_df.explode('description')
    services_df.reset_index(drop=True, inplace=True)
    services_df = services_df[~services_df.description.isna()]
    
    print(services_df.head(10))
    services_df = services_df[~services_df.place_id.isna()]
    out = services_df.to_json(orient="records")
    ti.xcom_push(key='services_df', value=out)

def places_cleaning(ti):
    data = ti.xcom_pull(key='data_places', task_ids='split_nested')
    places_df = pd.read_json(data, orient='records')
    
    # replace missing latitude and longitude with value exceed its maximum
    places_df.latitude.fillna(91, inplace=True)
    places_df.longitude.fillna(181, inplace=True)

    # replace missing urls with empty string
    places_df.web_picture_urls.fillna("", inplace=True)
    places_df.mobile_picture_urls.fillna("", inplace=True)

    # replace missing hit_score with -1
    places_df.hit_score.fillna(-1.0, inplace=True)

    # convert nested to list of payment methods
    pm = pd.json_normalize(places_df.payment_methods)
    new_col = []
    for index, row in pm.iterrows():
        temp = []
        for item in row:
            if item != None:
                temp.append(item['description'])
        new_col.append(temp)

    places_df.drop(columns=['payment_methods'], inplace=True)
    places_df['payment_methods'] = new_col

    # fill how to travel missing value with empty string
    places_df.how_to_travel = places_df.how_to_travel.apply(lambda d: d if isinstance(d, list) else [])

    # fill standard missing value with empty string
    places_df.standard.fillna("", inplace=True)

    # replace missing awards with empty list
    places_df.awards = places_df.awards.apply(lambda d: d if isinstance(d, list) else [])

    # clean up destination column
    places_df.destination = places_df.destination.apply(lambda x: x.upper())
    df_thaiprov = pd.read_csv('./dags/pipeline/data/province_th.csv')
    length = len(df_thaiprov)
    for i in range(length):
        thai_name = df_thaiprov.iloc[i]["Thai Name"]
        eng_name = df_thaiprov.iloc[i]["Name"]
        places_df.destination = places_df.destination.replace(thai_name, eng_name.upper())

    th = ["หัวหิน", "พัทยา", "เกาะลันตา", "เกาะช้าง"]
    en = ["hua hin", "pattaya", "ko lanta", "ko chang"]

    i = 0
    for place in th:
        places_df.destination = places_df.destination.replace(place, en[i].upper())
        i += 1

    change = ['BURI RAM', 'SI SA KET']
    for place in change:
        places_df.destination = places_df.destination.replace(place, place.replace(" ", ""))

    places_df.drop(columns=['update_date', 'thumbnail_url', 'map_code', 'area'], inplace=True)
    print(places_df.head(10))
    print(places_df.info())
    out = places_df.to_json(orient="records")
    ti.xcom_push(key='places_df', value=out)
