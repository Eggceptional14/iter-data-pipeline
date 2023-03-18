import json


def split_category(ti):
    data = ti.xcom_pull(key="data", task_ids="get_detail")
    print(len(data))
    print(json.loads(data[0])['category_code'])
    category = ['RESTAURANT', 'ATTRACTION', 'ACCOMMODATION', 'SHOP']
    restaurant_list, attraction_list, accommodation_list, shop_list = [], [], [], []
    for place in data:
        place_obj = json.loads(place)

        if place_obj['category_code'] == category[0]:
            restaurant_list.append(place_obj)
        elif place_obj['category_code'] == category[1]:
            attraction_list.append(place_obj)
        elif place_obj['category_code'] == category[2]:
            accommodation_list.append(place_obj)
        elif place_obj['category_code'] == category[3]:
            shop_list.append(place_obj)

    # print(f'Number of restaurants:{len(restaurant_list)}')
    # print(f'Number of attractions:{len(attraction_list)}')
    # print(f'Number of accommodations:{len(accommodation_list)}')
    # print(f'Number of shops:{len(shop_list)}')
    
    ti.xcom_push(key="data_restaurant", value=restaurant_list)
    ti.xcom_push(key="data_attraction", value=attraction_list)
    ti.xcom_push(key="data_accommodation", value=accommodation_list)
    ti.xcom_push(key="data_shop", value=shop_list)

    return