import requests
import json
import os
from dotenv import load_dotenv


# def data_partition(ti):
#     data = ti.xcom_pull(key="raw_data", task_ids="get_raw_data")
#     ti.xcom_push(key="data", value=place_f)
    
def get_detail(ti):
    language = 'EN'
    load_dotenv('./dags/.env')
    key=os.getenv('TAT_API_KEY')
    base_url = "https://tatapi.tourismthailand.org/tatapi/v5/"
    place_f = []
    success_get_detail_cnt = 0
    data = ti.xcom_pull(key="raw_data", task_ids="get_raw_data")
    print(f'size of response:{len(data)}')

    for i in data:
        url = base_url + i['category_code'].lower() + '/' + i['place_id'] 
        try:
            detail_response = requests.get(url,
                        headers = {
                            "Accept": "*/*",
                            "Authorization": key,
                            "Accept-Language": language
                        })
            if detail_response.status_code == 200:
                success_get_detail_cnt += 1
                detail = detail_response.json()['result']
                detail['category_code'] = i['category_code']
                detail['category_description'] = i['category_description']
                place_f.append(json.dumps(detail))
                # print(type(detail))
                # id = detail['place_id']
                if (success_get_detail_cnt % 1000) == 0:
                    print(f'place id:{id}, total success: {success_get_detail_cnt}')

        except Exception as e:
            print('\n\n', e, '\n\n')
            print(i['category_code'], i['place_id'])
            print('\n', detail_response)
            print(url)
            
    ti.xcom_push(key="data", value=place_f)
    print(place_f[0])
    print(f'Get place detail success')

# def get_detail_partition1(ti):
#     pass
# def get_detail_partition2(ti):
#     pass
# def get_detail_partition3(ti):
#     pass
