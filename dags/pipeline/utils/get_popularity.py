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

def exit_handler(df, pop, utf):
    df['popularity'] = pop
    temp_df = df[['place_id','place_name','destination','popularity']]
 
    temp_df.to_csv('temp/place_popularity4.csv', index=False)
    with open('temp/unable_to_find_pop3.txt', mode='wt') as file:
        for i in utf:
            file.write(str(i) + '\n')

def create_popularity(ti):
    popularity = []
    unable_to_find = []
    data = ti.xcom_pull(key="raw_data", task_ids="get_raw_data")
    df = pd.read_json(data, orient='records')

    # df = pd.read_csv('temp/place_popularity.csv')
    # df = pd.read_csv('temp/place.csv.gz', compression='gzip')
    # popularity = df['popularity'].to_list()
    for ind, row in df.iloc[11561:].iterrows():
        time.sleep(0.2)
        pop = get_popularity(row['place_name'], row['destination'])
        if pop == None:
            popularity.append(0)
            unable_to_find.append(ind)
        else:
            popularity[ind] = pop

    df['popularity'] = popularity
    columns = ['place_id','place_name','destination','popularity']
    # df.to_csv('temp/place_popularity.csv')
    ti.xcom_push(key="data_popularity", value=df[columns].to_json(orient='records'))
    # with open('temp/unable_to_find_pop.txt', mode='wt') as file:
    #     for i in unable_to_find:
    #         file.write(str(i) + '\n')