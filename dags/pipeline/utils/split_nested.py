import json
import pandas as pd


def split_nested(ti):
    data = ti.xcom_pull(key="data", task_ids="get_detail")
    
    data_casted = []
    for d in data:
        data_casted.append(json.loads(d))
    place_df = pd.DataFrame.from_records(data_casted)

    location = pd.json_normalize(place_df.location)
    location['place_id'] = place_df['place_id']
    out_location = location.to_json(orient='records')

    contact = pd.json_normalize(place_df.contact)
    contact['place_id'] = place_df['place_id']
    print(contact)
    out_contact = contact.to_json(orient='records')

    sha = pd.json_normalize(place_df.sha)
    sha['place_id'] = place_df['place_id']
    out_sha = sha.to_json(orient='records')

    facility = pd.json_normalize(place_df.facilities)
    facility['place_id'] = place_df['place_id']
    out_facility = facility.to_json(orient='records')

    service = pd.json_normalize(place_df.services)
    service['place_id'] = place_df['place_id']
    out_service = service.to_json(orient='records')

    place_df.drop(columns=['location', 'contact', 'sha', 'facilities', 'services'], inplace=True)
    out_place = place_df.to_json(orient='records')

    ti.xcom_push(key="data_contact", value=out_contact)
    ti.xcom_push(key="data_location", value=out_location)
    ti.xcom_push(key="data_sha", value=out_sha)
    ti.xcom_push(key="data_facility", value=out_facility)
    ti.xcom_push(key="data_service", value=out_service)
    ti.xcom_push(key="data_places", value=out_place)