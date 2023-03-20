import json


def split_nested(ti):
    data = ti.xcom_pull(key="data", task_ids="get_detail")
    contact, location, sha, facility, service, places = [], [], [], [], [], []
    for place in data:
        place_obj = json.loads(place)

        tmp_location = place_obj.pop('location')
        tmp_location['place_id'] = place_obj['place_id']
        location.append(tmp_location)

        tmp_sha = place_obj.pop('sha')
        tmp_sha['place_id'] = place_obj['place_id']
        sha.append(tmp_sha)

        tmp_contact = place_obj.pop('contact')
        tmp_contact['place_id'] = place_obj['place_id']
        contact.append(tmp_contact)

        tmp_facility = place_obj.pop('facilities')
        tmp_facility['place_id'] = place_obj['place_id']
        facility.append(tmp_facility)

        tmp_service = place_obj.pop('services')
        tmp_service['place_id'] = place_obj['place_id']
        service.append(tmp_service)

        places.append(place_obj)
    
    ti.xcom_push(key="data_contact", value=contact)
    ti.xcom_push(key="data_location", value=location)
    ti.xcom_push(key="data_sha", value=sha)
    ti.xcom_push(key="data_facility", value=facility)
    ti.xcom_push(key="data_service", value=service)
    ti.xcom_push(key="data_places", value=places)