import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def upsert_data(ti):
    try:
        conn_string = "postgresql://data:data@192.168.1.110:5433/data"
        db = create_engine(conn_string)
        conn = db.connect()

        conn.execute("DROP TABLE IF EXISTS place, accommodation, attraction, shop, restaurant CASCADE")

        df_to_sql(ti.xcom_pull(key="location_df", task_ids="location_cln"), 'location', conn)
        df_to_sql(ti.xcom_pull(key="sha_df", task_ids="sha_cln"), 'sha', conn)
        df_to_sql(ti.xcom_pull(key="contact_df", task_ids="contact_cln"), 'contact', conn)
        df_to_sql(ti.xcom_pull(key="facilities_df", task_ids="facilities_cln"), 'facility', conn)
        df_to_sql(ti.xcom_pull(key="services_df", task_ids="services_cln"), 'service', conn)

        df_to_sql(ti.xcom_pull(key="data_accom", task_ids="split_category"), 'accommodation', conn)
        df_to_sql(ti.xcom_pull(key="data_accom_type", task_ids="split_category"), 'accommodation_type', conn)
        df_to_sql(ti.xcom_pull(key="room_cleaned", task_ids="room_cln"), 'accommodation_room', conn)

        df_to_sql(ti.xcom_pull(key="data_attr", task_ids="split_category"), 'attraction', conn)
        df_to_sql(ti.xcom_pull(key="data_attr_type", task_ids="split_category"), 'attraction_type', conn)

        df_to_sql(ti.xcom_pull(key="data_res", task_ids="split_category"), 'restaurant', conn)
        df_to_sql(ti.xcom_pull(key="data_res_type", task_ids="split_category"), 'restaurant_type', conn)
        df_to_sql(ti.xcom_pull(key="data_cuisine_type", task_ids="split_category"), 'cuisine_type', conn)
        df_to_sql(ti.xcom_pull(key="mcl_cleaned", task_ids="mcl_cln"), 'michelin', conn)

        df_to_sql(ti.xcom_pull(key="data_shop", task_ids="split_category"), 'shop', conn)
        df_to_sql(ti.xcom_pull(key="data_shop_type", task_ids="split_category"), 'shop_type', conn)

        df_to_sql(ti.xcom_pull(key="data_target", task_ids="split_category"), 'target', conn)
        df_to_sql(ti.xcom_pull(key="data_activity", task_ids="split_category"), 'activity', conn)
        df_to_sql(ti.xcom_pull(key="fee_cleaned", task_ids="inf_cln"), 'fee', conn)
        df_to_sql(ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln"), 'tag', conn)
        df_to_sql(ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln"), 'opening_hour', conn)
        df_to_sql(ti.xcom_pull(key="data_place_fv", task_ids="split_category"), 'place', conn)

    except (Exception, psycopg2.Error) as error:
        print(error)

def df_to_sql(json_data, db_name, connection):
    df = pd.read_json(json_data, orient='records')
    df.to_sql(db_name, con=connection, if_exists='replace', index=False)
    print(f'successfully upsert data into:{db_name}')

