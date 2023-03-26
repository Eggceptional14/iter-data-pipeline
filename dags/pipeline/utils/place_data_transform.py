import pandas as pd


def tag_data_cleaning(ti):
    data = ti.xcom_pull(key='data_tags', task_ids='places_split')
    tag_df = pd.read_json(data)

    tag_df = tag_df[~tag_df.tags.isna()].explode('tags')
    tag_df.reset_index(drop=True, inplace=True)
    tag_df.rename(columns={'tags': 'description'}, inplace=True)
    
    print(tag_df.head())
    out_tag = tag_df.to_json(orient='records')
    ti.xcom_push(value=out_tag, key='tag_cleaned')

def info_data_cleaning(ti):
    data = ti.xcom_pull(key='data_info', task_ids='places_split')
    info_df = pd.read_json(data)

    # combine intro and detail into description
    info_df["description"] = info_df[["introduction", "detail"]].apply(" ".join, axis=1)
    info_df.drop(columns=['introduction', 'detail'], inplace=True)


    out_info = info_df.to_json(orient='records')
    ti.xcom_push(value=out_info, key='info_cleaned')

def michelin_data_cleaning(ti):
    data = ti.xcom_pull(key='data_michelin', task_ids='places_split')
    ml_df = pd.read_json(data)

    ml_df = ml_df[~ml_df.michelins.isna()].explode('michelins')
    ml_df.reset_index(drop=True, inplace=True)
    ml_df = pd.concat([ml_df, ml_df['michelins'].apply(pd.Series)], axis=1)
    ml_df.drop(columns=['michelins'], inplace=True)

    print(ml_df.head())
    out_ml = ml_df.to_json(orient='records')
    ti.xcom_push(value=out_ml, key='room_cleaned')

def ophr_data_cleaning(ti):
    data = ti.xcom_pull(key='data_ophr', task_ids='places_split')
    ophr_df = pd.read_json(data)
    # print(ophr_df.info())

    # remove unused column
    ophr_df.drop(columns=['open_now', 'periods', 'special_close_text', 
                  'weekday_text.day1', 'weekday_text.day2', 'weekday_text.day3',
                  'weekday_text.day4', 'weekday_text.day5', 'weekday_text.day6',
                  'weekday_text.day7',], inplace=True)
    
    # Add day column and remove old column
    ophr_df['day'] = "['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']"
    ophr_df.day = ophr_df.day.apply(eval)
    ophr_df.drop(columns=['weekday_text.day1.day', 'weekday_text.day2.day', 'weekday_text.day3.day',
                    'weekday_text.day4.day', 'weekday_text.day5.day', 'weekday_text.day6.day',
                    'weekday_text.day7.day'], inplace=True)
    
    # combine multiple time column
    ophr_df['time'] = ophr_df.drop(columns=['place_id', 'day']).values.tolist()
    ophr_df = ophr_df.loc[:, ['place_id', 'day', 'time']]
    ophr_df = ophr_df.explode(['day', 'time'])
    ophr_df.reset_index(drop=True, inplace=True)

    # split time column into open and close time
    new_cols = ophr_df['time'].str.split(" - ", n=1, expand=True)
    ophr_df['opening_time'] = new_cols[0]
    ophr_df['closing_time'] = new_cols[1]
    ophr_df.loc[ophr_df.time == 'Closed', 'closing_time'] = 'Closed'
    ophr_df.opening_time = ophr_df.opening_time.str.lower()
    ophr_df.closing_time = ophr_df.closing_time.str.lower()

    # fill missing value with unknown and remove time column
    ophr_df.fillna('unknown', inplace=True)
    ophr_df.drop(columns=['time'], inplace=True)

    print(ophr_df.head())
    out_ophr = ophr_df.to_json(orient='records')
    ti.xcom_push(value=out_ophr, key='ophr_cleaned')

def room_data_cleaning(ti):
    data = ti.xcom_pull(key='data_room', task_ids='places_split')
    room_df = pd.read_json(data)

    room_df = room_df[~room_df.rooms.isna()].explode('rooms')
    room_df.reset_index(drop=True, inplace=True)
    room_df = pd.concat([room_df, room_df['rooms'].apply(pd.Series)], axis=1)
    room_df.drop(columns=['rooms'], inplace=True)

    print(room_df.head())
    out_room = room_df.to_json(orient='records')
    ti.xcom_push(value=out_room, key='room_cleaned')