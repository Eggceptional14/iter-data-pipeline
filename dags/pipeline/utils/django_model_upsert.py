import json
import pandas as pd
import requests

def get_token():
    '''
    Helper function to get token to make requests
    '''
    try:
        # Define the login endpoint URL and payload
        url = "http://dev.se.kmitl.ac.th:1337/api/user/login/"
        # url = "https://localhost:1337/api/user/login/"
        payload = {"email": "admin@iter.com", "password": "admin"}

        session = requests.Session()

        session.trust_env = False

        # Send the POST request to the login endpoint
        response = session.post(url, json=payload)

        # If the request was successful, extract the access token from the response and store it in a variable
        if response.status_code == 200:
            token = response.json().get("token").get("access")
            return token
        else:
            # If the request failed, print an error message and return None
            print(f"Error: {response.status_code} - {response.reason}")
            return None

    except Exception as e:
        print(f"An error occurred while trying to get the token: {e}")
        return None
    
def create_place(token, payload_request):
    '''
    API request to create a place in the django server at port 1337
    '''
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = payload_request

    session = requests.Session()
    session.trust_env = False

    response = session.post('http://dev.se.kmitl.ac.th:1337/api/places/', headers=headers, json=payload)

    if response.status_code == 201:
        print('Place created successfully!')
    elif response.status_code == 400 and "Place with this ID already exists." in response.json():
        print('Place already exists, skipping creation')
    else:
        print('Error creating place:', response.content)

def create_restaurant(token, payload_request):
    '''
    API request to create a place in the django server at port 1337
    '''
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = payload_request
    session = requests.Session()
    session.trust_env = False

    response = session.post('http://dev.se.kmitl.ac.th:1337/api/places/restaurants/', headers=headers, json=payload)

    if response.status_code == 201:
        print('Restaurant created successfully!')
    elif response.status_code == 400 and "Place with this ID already exists." in response.json():
        print('Restaurant already exists, skipping creation')
    else:
        print('Error creating Restaurant:', response.content)

def create_attraction(token, payload_request):
    '''
    API request to create a place in the django server at port 1337
    '''
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = payload_request
    session = requests.Session()
    session.trust_env = False

    response = session.post('http://dev.se.kmitl.ac.th:1337/api/places/attractions/', headers=headers, json=payload)

    if response.status_code == 201:
        print('Attraction created successfully!')
    elif response.status_code == 400 and "Place with this ID already exists." in response.json():
        print('Attraction already exists, skipping creation')
    else:
        print('Error creating Attraction:', response.content)

def create_accomodation(token, payload_request):
    '''
    API request to create a place in the django server at port 1337
    '''
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = payload_request
    session = requests.Session()
    session.trust_env = False

    response = session.post('http://dev.se.kmitl.ac.th:1337/api/places/accommodations/', headers=headers, json=payload)

    if response.status_code == 201:
        print('Accommodation created successfully!')
    elif response.status_code == 400 and "Place with this ID already exists." in response.json():
        print('Accommodation already exists, skipping creation')
    else:
        print('Error creating Accommodation:', response.content)
    
def create_shop(token, payload_request):
    '''
    API request to create a place in the django server at port 1337
    '''
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = payload_request
    session = requests.Session()
    session.trust_env = False

    response = session.post('http://dev.se.kmitl.ac.th:1337/api/places/shops/', headers=headers, json=payload)

    if response.status_code == 201:
        print('Shop created successfully!')
    elif response.status_code == 400 and "Place with this ID already exists." in response.json():
        print('Shop already exists, skipping creation')
    else:
        print('Error creating Shop:', response.content)

def place_upsert(ti):

    place_objs = place_format_transform(ti)
    '''
    Code to create places via an api request to the django server on dev.se.kmitl.ac.th
    '''
    token = get_token()

    # print(places_json[0])
    # print(f"To check if places dumps is correct: {json.dumps(places_json[0])}")

    
    restaurant_objs = restaurant_format_transform(ti, place_objs)
    restaurant_json = json.loads(restaurant_objs)
    for i in range(0, len(restaurant_json)):
        try:
            print(f"Current place being created: {restaurant_json[i]}")
            create_place(token, restaurant_json[i])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Restaurant with this ID already exists." in e.response.json():
                print(f"Skipping place {i} because it already exists")
                continue
            else:
                print(f"Error creating place {i}: {e}")

    accomm_objs = accommodation_format_transform(ti, place_objs)
    accom_json = json.loads(accomm_objs)
    for i in range(0, len(restaurant_json)):
        try:
            print(f"Current place being created: {restaurant_json[i]}")
            create_place(token, restaurant_json[i])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Accomodation with this ID already exists." in e.response.json():
                print(f"Skipping place {i} because it already exists")
                continue
            else:
                print(f"Error creating place {i}: {e}")

    attraction_objs = attraction_format_transform(ti, place_objs)
    attraction_json = json.loads(attraction_objs)
    for i in range(0, len(restaurant_json)):
        try:
            print(f"Current place being created: {restaurant_json[i]}")
            create_place(token, restaurant_json[i])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Attraction with this ID already exists." in e.response.json():
                print(f"Skipping place {i} because it already exists")
                continue
            else:
                print(f"Error creating place {i}: {e}")

    shop_objs = shop_format_transform(ti, place_objs)
    shop_json = json.loads(shop_objs)
    for i in range(0, len(restaurant_json)):
        try:
            print(f"Current place being created: {restaurant_json[i]}")
            create_place(token, restaurant_json[i])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Shop with this ID already exists." in e.response.json():
                print(f"Skipping place {i} because it already exists")
                continue
            else:
                print(f"Error creating place {i}: {e}")

    places_json = json.loads(place_objs)
    for i in range(0, len(places_json)):
        try:
            print(f"Current place being created: {places_json[i]}")
            create_place(token, places_json[i])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Place with this ID already exists." in e.response.json():
                print(f"Skipping place {i} because it already exists")
                continue
            else:
                print(f"Error creating place {i}: {e}")
    # print(json.dumps(attraction_objs[0], indent=2, cls=NpEncoder))

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
                "sha_name": sha_data["sha_name"],
                "sha_type_code": sha_data["sha_type_code"],
                "sha_type_description": sha_data["sha_type_description"],
                "sha_cate_id": sha_data["sha_cate_id"],
                "sha_cate_description": sha_data["sha_cate_description"]
            }
        else:
            sha = {}
        
        location_data = next((location for location in data_location if location["place_id"] == place_id), None)
        if location_data is not None:
            location = {
                "address": location_data["address"],
                "sub_district": location_data["sub_district"],
                "district": location_data["district"],
                "province": location_data["province"],
                "postcode": location_data["postcode"]
            }
        else:
            location = {}
        
        contact_data = next((contact for contact in data_contact if contact["place_id"] == place_id), None)
        if contact_data is not None:
            contact = {
                "mobile_number": contact_data["mobiles"],
                "phone_number": contact_data["phones"],
                "fax_number": [contact_data["fax"]] if contact_data["fax"] else [],
                "emails": contact_data["emails"],
                "urls": contact_data["urls"]
            }
        else:
            contact = {}
        
        facility_data = next((facility["description"] for facility in facility_list if facility["place_id"] == place_id), None)
        service_data = next((service["description"] for service in service_list if service["place_id"] == place_id), None)
        
        # append json object
        output.append({
            "place_id": place_id,
            "place_name": place["place_name"],
            "latitude": place["latitude"],
            "longitude": place["longitude"],
            "sha": sha,
            "location": location,
            "contact": contact,
            "introduction": place["introduction"],
            "detail": place["detail"],
            "destination": place["destination"],
            "category_code": place["category_code"],
            "category_description": place["category_description"],
            "how_to_travels": place["how_to_travel"],
            "mobile_picture_urls": place["mobile_picture_urls"],
            "web_picture_urls": place["web_picture_urls"],
            "payment_methods": place["payment_methods"],
            "facilities": facility_data,
            "services": service_data
        })
    output = pd.DataFrame(output)
    # json_formatted_str = json.dumps(output[0], indent=2)
    # print(json_formatted_str)
    # print(output.to_json(orient='records'))
    return output.to_json(orient='records')

# def place_format_transform(ti):
#     # load place data from downstream tasks
#     data_place = json.loads(ti.xcom_pull(key="data_place_fv", task_ids="split_category"))
#     data_sha = json.loads(ti.xcom_pull(key="sha_df", task_ids="sha_cln"))
#     data_location = json.loads(ti.xcom_pull(key="location_df", task_ids="location_cln"))
#     data_contact = json.loads(ti.xcom_pull(key="contact_df", task_ids="contact_cln"))
#     data_facility = ti.xcom_pull(key="facilities_df", task_ids="facilities_cln")
#     data_service = ti.xcom_pull(key="services_df", task_ids="services_cln")

#     # convert json into dataframe
#     # convert only these two because we want to use the function of pandas to 
#     # convert from exploded version into a list of facilities and services
#     df_facility = pd.read_json(data_facility, orient='records')
#     df_service = pd.read_json(data_service, orient='records')

#     # implode back to list form
#     df_facility = df_facility.groupby(['place_id']).agg({'description': list}).reset_index()
#     df_service = df_service.groupby(['place_id']).agg({'description': list}).reset_index()

#     # transform back to json object
#     facility_list = json.loads(df_facility.to_json(orient='records'))
#     service_list = json.loads(df_service.to_json(orient='records'))

#     output = []

#     # loop through place 
#     for place in data_place:
#         place_id = place['place_id']
#         sha_data = next((sha for sha in data_sha if sha['place_id'] == place_id), None)
#         if sha_data is not None:
#             sha = {
#                 'sha_name': sha_data['sha_name'],
#                 'sha_type_code': sha_data['sha_type_code'],
#                 'sha_type_description': sha_data['sha_type_description'],
#                 'sha_cate_id': sha_data['sha_cate_id'],
#                 'sha_cate_description': sha_data['sha_cate_description']
#             }
#         else:
#             sha = {}
        
#         location_data = next((location for location in data_location if location['place_id'] == place_id), None)
#         if location_data is not None:
#             location = {
#                 'address': location_data['address'],
#                 'sub_district': location_data['sub_district'],
#                 'district': location_data['district'],
#                 'province': location_data['province'],
#                 'postcode': location_data['postcode']
#             }
#         else:
#             location = {}
        
#         contact_data = next((contact for contact in data_contact if contact['place_id'] == place_id), None)
#         if contact_data is not None:
#             contact = {
#                 'mobile_number': contact_data['mobiles'],
#                 'phone_number': contact_data['phones'],
#                 'fax_number': [contact_data['fax']] if contact_data['fax'] else [],
#                 'emails': contact_data['emails'],
#                 'urls': contact_data['urls']
#             }
#         else:
#             contact = {}
        
#         facility_data = next((facility['description'] for facility in facility_list if facility['place_id'] == place_id), None)
#         service_data = next((service['description'] for service in service_list if service['place_id'] == place_id), None)
        
#         # append json object
#         output.append({
#             'place_id': place_id,
#             'place_name': place['place_name'],
#             'latitude': place['latitude'],
#             'longitude': place['longitude'],
#             'sha': sha,
#             'location': location,
#             'contact': contact,
#             'introduction': place['introduction'],
#             'detail': place['detail'],
#             'destination': place['destination'],
#             'category_code': place['category_code'],
#             'category_description': place['category_description'],
#             'how_to_travels': place['how_to_travel'],
#             'mobile_picture_urls': place['mobile_picture_urls'],
#             'web_picture_urls': place['web_picture_urls'],
#             'payment_methods': place['payment_methods'],
#             'facilities': facility_data,
#             'services': service_data
#         })
#     output = pd.DataFrame(output)
#     # json_formatted_str = json.dumps(output[0], indent=2)
#     # print(json_formatted_str)

#     return output.to_json(orient='records')

def restaurant_format_transform(ti, place_json):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_tag = ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln")
    data_ophr = ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln")
    data_mchl = ti.xcom_pull(key="mcl_cleaned", task_ids="mcl_cln")

    df_place = pd.read_json(place_json, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    df_pinfo = pd.read_json(data_pif, orient='records')

    df_place['standard'] = df_p['standard'].copy()
    df_place['hit_score'] = df_p['hit_score'].copy()
    df_place['awards'] = df_p['awards'].copy()
    df_place['restaurant_types'] = df_pinfo['restaurant_types'].copy()
    df_place['cuisine_types'] = df_pinfo['cuisine_types'].copy()

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
    # temp = df_restaurant[~df_restaurant.michelins.isna()].to_dict('records')
    # print(json.dumps(temp[0], indent=2))

    return df_restaurant.to_json(orient='records')

def accommodation_format_transform(ti, place_json):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_room = ti.xcom_pull(key='room_cleaned', task_ids="room_cln")

    df_place = pd.read_json(place_json, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    df_pinfo = pd.read_json(data_pif, orient='records')
    # df_rinfo = df_pinfo[df_pinfo.category_code == 'RESTAURANT']

    df_place['standard'] = df_p['standard'].copy()
    df_place['hit_score'] = df_p['hit_score'].copy()
    df_place['awards'] = df_p['awards'].copy()

    df_place['register_license_id'] = df_pinfo['register_license_id'].copy()
    df_place['hotel_star'] = df_pinfo['hotel_star'].copy()
    df_place['display_checkin_time'] = df_pinfo['display_checkin_time'].copy()
    df_place['display_checkout_time'] = df_pinfo['display_checkout_time'].copy()
    df_place['number_of_rooms'] = df_pinfo['number_of_rooms'].copy()
    df_place['price_range'] = df_pinfo['price_range'].copy()
    df_place['accommodation_types'] = df_pinfo['accommodation_types'].copy()

    df_acm = df_place[df_place.category_code == 'ACCOMMODATION']

    df_room = pd.read_json(data_room, orient='records')
    grouped = df_room.groupby(['place_id']).apply(lambda x: x[['room_type', 'bed_type']].apply(row_to_dict, axis=1).tolist()).reset_index(name='accommodation_rooms')
    df_acm = pd.merge(df_acm, grouped, on='place_id', how='left')

    # temp = df_acm[~df_acm.accommodation_rooms.isna()]
    # temp['display_checkin_time'] = temp['display_checkin_time'].astype(str)
    # temp['display_checkout_time'] = temp['display_checkout_time'].astype(str)
    # print(json.dumps(temp.to_dict('records')[0], indent=2, cls=NpEncoder))

    return df_acm.to_json(orient='records')

def attraction_format_transform(ti, place_json):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_tag = ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln")
    data_ophr = ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln")
    data_fee = ti.xcom_pull(key="fee_cleaned", task_ids="inf_cln")

    df_place = pd.read_json(place_json, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    df_pinfo = pd.read_json(data_pif, orient='records')
    # df_atrinfo = df_pinfo[df_pinfo.category_code == 'ATTRACTION']

    df_place['hit_score'] = df_p['hit_score'].copy()
    df_place['attraction_types'] = df_pinfo['attraction_types'].copy()

    df_atr = df_place[df_place.category_code == 'ATTRACTION']

    df_tag = pd.read_json(data_tag, orient='records')
    df_tag = df_tag.groupby(['place_id']).agg({'description': list}).reset_index()
    df_atr = pd.merge(df_atr, df_tag, on='place_id', how='left')
    df_atr.rename(columns={'description': 'tags'}, inplace=True)

    df_fee = pd.read_json(data_fee, orient='records')
    df_atr = pd.merge(df_atr, df_fee, on='place_id', how='left')
    df_atr['fee'] = (df_atr[['thai_child', 'thai_adult', 'foreigner_child', 'foreigner_adult']]
                     .apply(lambda x: {'thai_child': x['thai_child'], 'thai_adult': x['thai_adult'],
                                       'foreigner_child': x['foreigner_child'], 'foreigner_adult': x['foreigner_adult']}, axis=1))
    df_atr.drop(columns=['thai_child', 'thai_adult', 'foreigner_child', 'foreigner_adult'], inplace=True)

    df_atr = pd.merge(df_atr, df_pinfo[['place_id', 'activities', 'targets']], on='place_id', how='left')

    df_ophr = pd.read_json(data_ophr, orient='records')
    grouped = df_ophr.groupby(['place_id']).apply(lambda x: x[['day', 'opening_time', 'closing_time']].apply(row_to_dict, axis=1).tolist()).reset_index(name='opening_hours')
    df_atr = pd.merge(df_atr, grouped, on='place_id', how='left')

    # temp = df_atr[~df_atr.activities.isna() & ~df_atr.targets.isna()].to_dict('records')
    # print(json.dumps(temp[0], indent=2, cls=NpEncoder))
    # print(df_pinfo.info())
    # print(df_atr.info())

    return df_atr.to_json(orient='records')

def shop_format_transform(ti, place_json):
    data_pif = ti.xcom_pull(key='info_cleaned', task_ids='inf_cln')
    data_p = ti.xcom_pull(key='data_places', task_ids='places_split')
    data_tag = ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln")
    data_ophr = ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln")

    df_place = pd.read_json(place_json, orient='records')
    df_p = pd.read_json(data_p, orient='records')
    df_pinfo = pd.read_json(data_pif, orient='records')

    df_place['standard'] = df_p['standard'].copy()
    df_place['shop_types'] = df_pinfo['shop_types'].copy()

    df_shop = df_place[df_place.category_code == 'SHOP']

    df_tag = pd.read_json(data_tag, orient='records')
    df_tag = df_tag.groupby(['place_id']).agg({'description': list}).reset_index()
    df_shop = pd.merge(df_shop, df_tag, on='place_id', how='left')
    df_shop.rename(columns={'description': 'tags'}, inplace=True)

    df_ophr = pd.read_json(data_ophr, orient='records')
    grouped = df_ophr.groupby(['place_id']).apply(lambda x: x[['day', 'opening_time', 'closing_time']].apply(row_to_dict, axis=1).tolist()).reset_index(name='opening_hours')
    df_shop = pd.merge(df_shop, grouped, on='place_id', how='left')

    # temp = df_shop[~df_shop.shop_types.isna()].to_dict('records')
    # print(json.dumps(temp[0], indent=2, cls=NpEncoder))
    # print(df_pinfo.info())
    # print(df_shop.info())

    return df_shop.to_json(orient='records')

def row_to_dict(row):
    return row.to_dict()