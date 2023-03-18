import requests
import os
from dotenv import load_dotenv


def get_raw_data(ti):
    language = 'EN'
    output = []
    # places_f = open(f'./temp/places_{language}.json', 'w+')
    # missing_places = open(f'./temp/missing_places_{language}.json', 'w+')
    load_dotenv('./dags/.env')
    response = True
    pageNo = 1
    cnt = 0
    key = os.getenv('TAT_API_KEY')
    print(os.getcwd())

    while response:
        try:
            response = requests.get("https://tatapi.tourismthailand.org/tatapi/v5/places/search",
                            headers = {
                                "Accept": "*/*",
                                "Authorization": key,
                                "Accept-Language": language
                            },
                            params = {
                                'numberOfResult':100,
                                'pagenumber':pageNo
                            })
            pageNo += 1
            data = response.json()['result']
            output += data
            print(f'current page output:{len(data)}, current total output:{len(output)}')

        except Exception as e:
            # print(e, cnt)
            print(response)
            
    print(f'End of response, total op:{len(output)}')
    ti.xcom_push(key="raw_data", value=output)

if __name__ == '__main__':
    get_raw_data()
