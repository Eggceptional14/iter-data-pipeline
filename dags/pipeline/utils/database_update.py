import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def upsert_data(ti):
    try:
        db = create_engine('postgresql://data:data@192.168.1.110:5433/data', client_encoding='utf8')
        conn = db.connect()

        conn.execute("DROP TABLE IF EXISTS place, accommodation, attraction, shop, restaurant CASCADE")

        df_to_sql(ti.xcom_pull(key="data_place_fv", task_ids="split_category"), 'place', conn)
        # remove duplicate (if exists)
        conn.execute("DELETE FROM place a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM place GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add primary key constraint
        conn.execute("ALTER TABLE place ADD PRIMARY KEY (place_id)")
        
        df_to_sql(ti.xcom_pull(key="location_df", task_ids="location_cln"), 'location', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM location a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM location GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE location ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="sha_df", task_ids="sha_cln"), 'sha', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM sha a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM sha GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE sha ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="contact_df", task_ids="contact_cln"), 'contact', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM contact a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM contact GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE contact ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="facilities_df", task_ids="facilities_cln"), 'facility', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM facility a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM facility GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE facility ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="services_df", task_ids="services_cln"), 'service', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM service a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM service GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE service ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")

        df_to_sql(ti.xcom_pull(key="data_accom", task_ids="split_category"), 'accommodation', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM accommodation a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM accommodation GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE accommodation ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_accom_type", task_ids="split_category"), 'accommodation_type', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM accommodation_type a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM accommodation_type GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE accommodation_type ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES accommodation(place_id)")
        
        df_to_sql(ti.xcom_pull(key="room_cleaned", task_ids="room_cln"), 'accommodation_room', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM accommodation_room a \
                     USING (SELECT MIN(ctid) as ctid, place_id, room_type, bed_type \
                     FROM accommodation_room GROUP BY place_id, room_type, bed_type HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.room_type = b.room_type AND a.bed_type = b.bed_type AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE accommodation_room ADD PRIMARY KEY (place_id, room_type, bed_type),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")

        df_to_sql(ti.xcom_pull(key="data_attr", task_ids="split_category"), 'attraction', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM attraction a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM attraction GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE attraction ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_attr_type", task_ids="split_category"), 'attraction_type', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM attraction_type a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM attraction_type GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE attraction_type ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES attraction(place_id)")

        df_to_sql(ti.xcom_pull(key="data_res", task_ids="split_category"), 'restaurant', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM restaurant a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM restaurant GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE restaurant ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_res_type", task_ids="split_category"), 'restaurant_type', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM restaurant_type a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM restaurant_type GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE restaurant_type ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES restaurant(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_cuisine_type", task_ids="split_category"), 'cuisine_type', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM cuisine_type a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM cuisine_type GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE cuisine_type ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES restaurant(place_id)")
        
        df_to_sql(ti.xcom_pull(key="mcl_cleaned", task_ids="mcl_cln"), 'michelin', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM michelin a \
                     USING (SELECT MIN(ctid) as ctid, place_id, name, year \
                     FROM michelin GROUP BY place_id, name, year HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.name = b.name AND a.year = b.year AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE michelin ADD PRIMARY KEY (place_id, name, year),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES restaurant(place_id)")

        df_to_sql(ti.xcom_pull(key="data_shop", task_ids="split_category"), 'shop', conn)
        # remove duplicate row (if exists)
        conn.execute("DELETE FROM shop a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM shop GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE shop ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_shop_type", task_ids="split_category"), 'shop_type', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM shop_type a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM shop_type GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE shop_type ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES shop(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_target", task_ids="split_category"), 'target', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM target a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM target GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE target ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="data_activity", task_ids="split_category"), 'activity', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM activity a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM activity GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE activity ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="fee_cleaned", task_ids="inf_cln"), 'fee', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM fee a \
                     USING (SELECT MIN(ctid) as ctid, place_id \
                     FROM fee GROUP BY place_id HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE fee ADD PRIMARY KEY (place_id),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="tag_cleaned", task_ids="tag_cln"), 'tag', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM tag a \
                     USING (SELECT MIN(ctid) as ctid, place_id, description \
                     FROM tag GROUP BY place_id, description HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.description = b.description AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE tag ADD PRIMARY KEY (place_id, description),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")
        
        df_to_sql(ti.xcom_pull(key="ophr_cleaned", task_ids="ophr_cln"), 'opening_hour', conn)
        # remove duplicate rows (if exists)
        conn.execute("DELETE FROM opening_hour a \
                     USING (SELECT MIN(ctid) as ctid, place_id, day \
                     FROM opening_hour GROUP BY place_id, day HAVING COUNT(*) > 1) \
                     b WHERE a.place_id = b.place_id AND a.day = b.day AND a.ctid <> b.ctid")
        # add constraints
        conn.execute("ALTER TABLE opening_hour ADD PRIMARY KEY (place_id, day),\
                      ADD CONSTRAINT fk_place_id FOREIGN KEY(place_id) REFERENCES place(place_id)")



    except (Exception, psycopg2.Error) as error:
        print(error)

def df_to_sql(json_data, db_name, connection):
    df = pd.read_json(json_data, orient='records')
    df.to_sql(db_name, con=connection, if_exists='replace', index=False)
    print(f'successfully upsert data into:{db_name}')

