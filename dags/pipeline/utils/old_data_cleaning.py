import json
import pandas as pd
import math

f = open('categorized_EN.json')
json_array = json.load(f)

accom_f = open('./cleaned/accommodation_EN.json', 'w+')
attract_f = open('./cleaned/attraction_EN.json', 'w+')
shop_f = open('./cleaned/shop_EN.json', 'w+')
restaurant_f = open('./cleaned/restaurant_EN.json', 'w+')

accom_fm = open('./formodel/accommodation_EN.json', 'w+')
attract_fm = open('./formodel/attraction_EN.json', 'w+')
shop_fm = open('./formodel/shop_EN.json', 'w+')
restaurant_fm = open('./formodel/restaurant_EN.json', 'w+')


    

write_files = {
    'ACCOMMODATION': accom_f,
    'ATTRACTION': attract_f,
    'SHOP': shop_f,
    'RESTAURANT': restaurant_f
}

write_model_files = {
    'ACCOMMODATION': accom_fm,
    'ATTRACTION': attract_fm,
    'SHOP': shop_fm,
    'RESTAURANT': restaurant_fm
}

write_counts = {
    'ACCOMMODATION': [],
    'ATTRACTION': [],
    'SHOP': [],
    'RESTAURANT': []
}

for file in write_files.values():
    file.write('[')

for file in write_model_files.values():
    file.write('[')

delete_fields = {
    'ACCOMMODATION': ['tags', 'opening_hours', 'michelins', 'area', 'map_code', 'update_date'],
    'ATTRACTION': ['standard', 'awards', 'rooms', 'michelins', 'area', 'update_date'],
    'SHOP': ['tags', 'awards', 'rooms', 'hit_score', 'michelins', 'area', 'map_code', 'update_date'],
    'RESTAURANT': ['rooms', 'area', 'map_code', 'update_date']
}

too_many_null_fields = {
    'ACCOMMODATION': ['rooms', 'hit_score', 'standard', 'awards', 'how_to_travel'],
    'ATTRACTION': ['services', 'how_to_travel', 'tags', 'hit_score'],
    'SHOP': ['services', 'how_to_travel', 'tags', 'awards', 'standard', 'hit_score'],
    'RESTAURANT': ['standard', 'how_to_travel', 'services', 'hit_score', 'tags', 'awards']
}

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
def check_null(val, col):
    if val == '' or val == [] or val == None or val == [''] or \
        val == {"phones": None, "mobiles": None, "fax": None, "emails": None, "urls": None} or \
            val == {"open_now": None, "periods": None, 
            "weekday_text": {"day1": None, "day2": None, "day3": None, "day4": None, "day5": None, "day6": None, "day7": None}}:
        return null_val[col]

    elif type(i) == float:
        if math.isnan(i):
            return null_val[col]
    return val

# places_f.write('[')

for i in json_array:
    nolatlong = False
    temp = {}
    temp_fm = {}
    cat = i['category_code']
    for k, v in i.items():
        if (k == 'latitide' or k == 'longitude' )and v == None:
            # print(i['place_id'], i['place_name'])
            nolatlong = True
            break
        if k == 'place_information':
            for p_info_key, p_info_val in v.items():
                
                temp[p_info_key] = check_null(p_info_val, p_info_key)
                temp_fm[p_info_key] = check_null(p_info_val, p_info_key)
        else:
            if k not in delete_fields[cat] and k not in too_many_null_fields[cat]:
                temp_fm[k] = check_null(v, k)
            if k not in delete_fields[cat]:
                temp[k] = check_null(v, k)

    if not nolatlong and i['place_id'] not in write_counts[cat]:
        if len(write_counts[cat]) == 0:
            write_files[cat].write( json.dumps(temp))
            write_model_files[cat].write( json.dumps(temp_fm))

        else:
            write_files[cat].write( ',' + json.dumps(temp))
            write_model_files[cat].write(','+ json.dumps(temp_fm))

        write_counts[cat].append(i['place_id'])
    elif not nolatlong:
        print(cat, i['place_id'])

for file in write_files.values():
    file.write(']')
    
for file in write_model_files.values():
    file.write(']')
for k, v in write_counts.items():
    print(k, len(v))
# remove row without lat long
# no_lat_long = df.loc[(df['latitude'].isna()) | (df['longitude'].isna())]
# df = df[~df['place_id'].isin(no_lat_long['place_id'])]

# # accom = df.loc[df['category_code'] == 'ACCOMMODATION']
# # attract = df.loc[df['category_code'] == 'ATTRACTION']
# # shop = df.loc[df['category_code'] == 'SHOP']
# # rest = df.loc[df['category_code'] == 'RESTAURANT']
# # other = df.loc[df['category_code'] == 'OTHER']
# categorized = df.loc[df['category_code'] != 'OTHER'].T
# print(categorized)

# # categorized.to_json(r'categorized.json')
# # category = {'ACCOMMODATION': accom,  'ATTRACTION':attract, 'SHOP':shop, 'RESTAURANT':rest, 'OTHER':other, 'CATEGORIZED':categorized}
# # # # category = {'SHOP': shop}
# # # # for ind, row in df.iterrows():
# # # #     df['place_name'].iloc[ind] += ' ' + row['location']['sub_district']

# # # print('total ', len(df), '\n')
# # for cat, cdf in category.items():
# #     total = len(cdf)
# #     print(f'----------{cat}-----------\n{total}')
# #     for c in cdf.columns:
# #         if c != 'place_information':
# #             num_nan = 0
            

# #             # num_nan = cdf[c].isna().sum()
# #             for i in cdf[c]:
# #                 if i == '' or i == [] or i == None:
# #                     num_nan += 1
# #                 elif type(i) == float:
# #                     if math.isnan(i):
# #                        num_nan += 1

# #             if num_nan == 0:
# #                 print(c)
# #             elif total - num_nan != 0:
# #                 print(c, '\tNaN:' ,num_nan, '\tnot NaN:', total - num_nan, '\tnot Nan%', round((total - num_nan)/total, 4) * 100)
# #             else:
# #                 print('####',c )
# #     print('---------------------------\n')

# # # print(other)