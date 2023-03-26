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

    print(location_df[["province", "district", "sub_district"]].head(10))
    out = location_df.to_json(orient="records")

    ti.xcom_push(key='location_df', value=out)

def sha_cleaning(ti):
    data = ti.xcom_pull(key='data_sha', task_ids='split_nested')
    sha_df = pd.DataFrame(data)

    sha_df.drop(sha_df[sha_df['sha_name'] == ""].index, inplace=True)

    print(sha_df.head(10))
    out = sha_df.to_json(orient="records")
    ti.xcom_push(key='sha_df', value=out)
    
def contact_cleaning(ti):
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    contact_df = pd.DataFrame(data)

    contact_df.fillna("")

    print(contact_df.head(10))
    out = contact_df.to_json(orient="records")
    ti.xcom_push(key='contact_df', value=out)
    
def facilities_cleaning(ti): #incomplete
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    facilities_df = pd.DataFrame(data)
    
    new_col = []
    for index, row in data.iterrows():
        tmp = []
        for item in row:
            if item != None:
                tmp.append(item['description'])

        if len(tmp) > 0:
            new_col.append(tmp)
        else:
            new_col.append("")

    facilities_df['description'] = new_col
    facilities_df.drop(columns=[0,1,2,3,4,5,6,7,8,9,10], inplace=True)

    out = facilities_df.to_json(orient="records")
    ti.xcom_push(key='facilities_df', value=out)

def services_cleaning(ti): #incomplete
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    services_df = pd.DataFrame(data)
    
    out = services_df.to_json(orient="records")
    ti.xcom_push(key='services_df', value=out)

def places_cleaning(ti): #incomplete
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    places_df = pd.DataFrame(data)

    out = places_df.to_json(orient="records")
    ti.xcom_push(key='places_df', value=out)
