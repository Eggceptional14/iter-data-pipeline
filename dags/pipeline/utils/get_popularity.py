import requests
import pandas as pd
import time
import atexit

from bs4 import BeautifulSoup


def get_popularity(place_name, destination):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"}
    place_name += " " + destination.lower()
    place_name = place_name.replace(" ", "+")
    URL     = "https://www.google.com/search?q="+place_name
    result = requests.get(URL, headers=headers)    

    soup = BeautifulSoup(result.content, 'html.parser')
    
    if soup is None:
        print('soup None')
        return None
    
    result_stats = soup.find("div", {"id": "result-stats"})
    if result_stats is None:
        print('result_stats None')
        return None
    
    total_results_text = result_stats.find(text=True, recursive=False)
    results_num = ''.join([num for num in total_results_text if num.isdigit()])
    return int(results_num)

def create_popularity(ti):
    popularity = []
    data = ti.xcom_pull(key="raw_data", task_ids="get_raw_data")
    df = pd.DataFrame(data)

    # popularity = df['popularity'].to_list()
    for ind, row in df.iterrows():
        time.sleep(0.2)
        pop = get_popularity(row['place_name'], row['destination'])
        if pop == None:
            popularity.append(0)
        else:
            # popularity[ind] = pop
            popularity.append(pop)
            
        # if ind == 499:
        #     break
    
    # df = df.head(500)
    df['popularity'] = popularity
    columns = ['place_id','place_name','destination','popularity']
    print(df[columns])
    ti.xcom_push(key="data_popularity", value=df[columns].to_json(orient='records'))