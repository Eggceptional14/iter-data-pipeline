import json
import pandas as pd


def place_upsert(ti):
    place_objs = place_format_transform(ti)
    restaurant_objs = restaurant_format_transform(ti, place_objs)


def place_format_transform(ti):
    # load place data from downstream tasks
    data_place = json.loads(ti.xcom_pull(key="data_place_fv", task_ids="split_category"))
    data_sha = json.loads(ti.xcom_pull(key="sha_df", task_ids="sha_cln"))
    data_location = json.loads(ti.xcom_pull(key="location_df", task_ids="location_cln"))
    data_contact = json.loads(ti.xcom_pull(key="contact_df", task_ids="contact_cln"))
    data_facility = ti.xcom_pull(key="facilities_df", task_ids="facilities_cln")
    data_service = ti.xcom_pull(key="services_df", task_ids="services_cln")

    # convert json into dataframe
    # convert only these two because we want to use the function of pandas to 
    # convert from exploded version into a list of facilities and services
    df_facility = pd.read_json(data_facility, orient='records')
    df_service = pd.read_json(data_service, orient='records')

    # implode back to list form
    df_facility = df_facility.groupby(['place_id']).agg({'description': list}).reset_index()
    df_service = df_service.groupby(['place_id']).agg({'description': list}).reset_index()

    # transform back to json object
    facility_list = json.loads(df_facility.to_json(orient='records'))
    service_list = json.loads(df_service.to_json(orient='records'))

    output = []

    # loop through place 
    for place in data_place:
        place_id = place['place_id']
        sha_data = next((sha for sha in data_sha if sha['place_id'] == place_id), None)
        if sha_data is not None:
            sha = {
                'sha_name': sha_data['sha_name'],
                'sha_type_code': sha_data['sha_type_code'],
                'sha_type_description': sha_data['sha_type_description'],
                'sha_cate_id': sha_data['sha_cate_id'],
                'sha_cate_description': sha_data['sha_cate_description']
            }
        else:
            sha = {}
        
        location_data = next((location for location in data_location if location['place_id'] == place_id), None)
        if location_data is not None:
            location = {
                'address': location_data['address'],
                'sub_district': location_data['sub_district'],
                'district': location_data['district'],
                'province': location_data['province'],
                'postcode': location_data['postcode']
            }
        else:
            location = {}
        
        contact_data = next((contact for contact in data_contact if contact['place_id'] == place_id), None)
        if contact_data is not None:
            contact = {
                'mobile_number': contact_data['mobiles'],
                'phone_number': contact_data['phones'],
                'fax_number': [contact_data['fax']] if contact_data['fax'] else [],
                'emails': contact_data['emails'],
                'urls': contact_data['urls']
            }
        else:
            contact = {}
        
        facility_data = next((facility['description'] for facility in facility_list if facility['place_id'] == place_id), None)
        service_data = next((service['description'] for service in service_list if service['place_id'] == place_id), None)
        
        # append json object
        output.append({
            'place_id': place_id,
            'place_name': place['place_name'],
            'latitude': place['latitude'],
            'longitude': place['longitude'],
            'sha': sha,
            'location': location,
            'contact': contact,
            'introduction': place['introduction'],
            'detail': place['detail'],
            'destination': place['destination'],
            'category_code': place['category_code'],
            'category_description': place['category_description'],
            'how_to_travels': place['how_to_travel'],
            'mobile_picture_urls': place['mobile_picture_urls'],
            'web_picture_urls': place['web_picture_urls'],
            'payment_methods': place['payment_methods'],
            'facilities': facility_data,
            'services': service_data
        })

    # json_formatted_str = json.dumps(output[0], indent=2)
    # print(json_formatted_str)

    return output

def restaurant_format_transform(ti, place_json):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_tag = ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln")
    data_ophr = ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln")
    data_mchl = ti.xcom_pull(key="mcl_cleaned", task_ids="mcl_cln")

    df_place = pd.DataFrame(place_json)
    df_p = pd.read_json(data_p, orient='records')
    df_pinfo = pd.read_json(data_pif, orient='records')
    df_rinfo = df_pinfo[df_pinfo.category_code == 'RESTAURANT']

    df_place['standard'] = df_p['standard'].copy()
    df_place['hit_score'] = df_p['hit_score'].copy()
    df_place['awards'] = df_p['awards'].copy()
    df_place['restaurant_types'] = df_rinfo['restaurant_types'].copy()
    df_place['cuisine_types'] = df_rinfo['cuisine_types'].copy()

    df_restaurant = df_place[df_place.category_code == 'RESTAURANT']

    # rst_list = df_restaurant.to_json(orient='records')
    
    df_tag = pd.read_json(data_tag, orient='records')
    df_tag = df_tag.groupby(['place_id']).agg({'description': list}).reset_index()
    
    df_restaurant = pd.merge(df_restaurant, df_tag, on='place_id', how='left')
    df_restaurant.rename(columns={'description': 'tags'}, inplace=True)
    # print(df_restaurant.info())

    df_ophr = pd.read_json(data_ophr, orient='records')
    grouped = df_ophr.groupby(['place_id']).apply(lambda x: x[['day', 'opening_time', 'closing_time']].apply(row_to_dict, axis=1).tolist()).reset_index(name='opening_hours')
    df_restaurant = pd.merge(df_restaurant, grouped, on='place_id', how='left')
    # print(df_rinfo.info())
    # print(df_restaurant[['place_name', 'opening_hours']])

    df_mchl = pd.read_json(data_mchl, orient='records')
    grouped = df_mchl.groupby(['place_id']).apply(lambda x: x[['name', 'year']].apply(row_to_dict, axis=1).tolist()).reset_index(name='michelins')
    df_restaurant = pd.merge(df_restaurant, grouped, on='place_id', how='left')
    # print(list(df_restaurant[~df_restaurant.michelins.isna()].head(1).michelins))

    return df_restaurant.to_json(orient='records')

def row_to_dict(row):
    return row.to_dict()