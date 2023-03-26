import json
import pandas as pd


def split_place_nested(ti):
    data = ti.xcom_pull(key="places_df", task_ids="places_cln")
    place_df = pd.read_json(data, orient='records')
    # print(place_df.info())
    # print(place_df[['tags','rooms', 'place_information', 'opening_hours', 'michelins']])
    
    # split tag column: tag is a list of string
    tag = place_df[['place_id', 'tags']]
    out_tag = tag.to_json(orient='records')

    # split room column: room column is a list of json object
    room = place_df[['place_id', 'rooms']]
    out_room = room.to_json(orient='records')

    info = pd.json_normalize(place_df.place_information)
    info['place_id'] = place_df['place_id']
    out_info = info.to_json(orient='records')

    ophr = pd.json_normalize(place_df.opening_hours)
    ophr['place_id'] = place_df['place_id']
    out_ophr = ophr.to_json(orient='records')

    # split michelin column: michelin column is a list of json object
    michelin = place_df[['place_id', 'michelins']]
    out_michelin = michelin.to_json(orient='records')

    place_df.drop(columns=['tags, rooms, place_information, opening_hours, michelins'], inplace=True)
    out_place = place_df.to_json(orient='records')

    ti.xcom_push(key='data_tags', value=out_tag)
    ti.xcom_push(key='data_info', value=out_info)
    ti.xcom_push(key='data_michelin', value=out_michelin)
    ti.xcom_push(key='data_ophr', value=out_ophr)
    ti.xcom_push(key='data_room', value=out_room)
    ti.xcom_push(key="data_places", value=out_place)