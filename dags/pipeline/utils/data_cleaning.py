import pandas as pd


def location_cleaning(ti):
    data = ti.xcom_pull(key='data_location', task_ids='split_nested')
    location_df = pd.DataFrame(data)

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