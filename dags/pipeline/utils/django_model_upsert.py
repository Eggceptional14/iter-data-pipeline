import json
import pandas as pd


def place_upsert(ti):
    place_objs = place_format_transform(ti)

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
            'id': place_id,
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