import pandas as pd
import math


task_id = "split_category"
null_val = {
    'introduction': '',
    'detail': '',
    'accommodation_types': [],
    'register_license_id': '',
    'hotel_star': '',
    'number_of_rooms': float('nan'),
    'price_range': '',
    'contact': [{'phones': None, 'mobiles': None, 'fax': None, 'emails': None, 'urls': None}],
    'thumbnail_url': '',
    'web_picture_urls': [],
    'mobile_picture_urls': [],
    'facilities': [],
    'services': [],
    'payment_methods': [],
    'map_code': '',
    'attraction_types': [],
    'activities': [],
    'targets': [],
    'shop_types': [],
    'restaurant_types': [],
    'cuisine_types': [],
    'michelins': [],
    'how_to_travel': [],
    'awards': [],
    'update_date': None,
    'rooms': [],
    'standard': '',
    'tags': [],
    'hit_score': float('nan'),
    'display_checkin_time': None,    
    'display_checkout_time': None,
    'latitude': float('nan'),
    'longtitude': float('nan'),
    'opening_hours': {"open_now": None, "periods": None, "weekday_text": {"day1": None, "day2": None, "day3": None, "day4": None, "day5": None, "day6": None, "day7": None}}
}

def remove_null_accommodation(ti):
    data = ti.xcom_pull(key="data_accommodation", task_ids=task_id)
    print(data)

def remove_null_attraction(ti):
    data = ti.xcom_pull(key="data_attraction", task_ids=task_id)
    print(data)

def remove_null_restaurant(ti):
    data = ti.xcom_pull(key="data_restaurant", task_ids=task_id)
    print(data)

def remove_null_shop(ti):
    data = ti.xcom_pull(key="data_shop", task_ids=task_id)
    print(data)

# def check_null(val, col):
#     if val == '' or val == [] or val == None or val == [''] or \
#         val == {"phones": None, "mobiles": None, "fax": None, "emails": None, "urls": None} or \
#             val == {"open_now": None, "periods": None, 
#             "weekday_text": {"day1": None, "day2": None, "day3": None, "day4": None, "day5": None, "day6": None, "day7": None}}:
#         return null_val[col]

#     elif type(i) == float:
#         if math.isnan(i):
#             return null_val[col]
#     return val