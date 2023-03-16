import pandas as pd
import psycopg2
from loguru import logger
import hashlib
from datetime import datetime, timedelta


def insert_camera_data_local_db(design_code, record_times, record_values, bucket_times, table_name="camera_data"):
    logger.info("insert_local_db...")
    conn_local = psycopg2.connect(database="ArchMind_Canteen", user="archmind", password="pg123", host="127.0.0.1",
                                  port="5432")
    cur_local = conn_local.cursor()
    design_codes = [design_code] * len(record_times)
    indexes = []

    record_times_f, record_values_f, bucket_times_f, design_codes_f = [], [], [], []

    for i in range(len(record_times)):
        if record_times[i] is not None:
            design_codes_f.extend([design_codes[i]])
            record_times_f.extend([record_times[i]])
            record_values_f.extend([record_values[i]])
            bucket_times_f.extend([bucket_times[i]])
            index = hashlib.md5((design_codes[i] + record_times[i]).encode(encoding='utf-8')).hexdigest()
            indexes.extend([index])

    meters = zip(indexes, design_codes_f, record_times_f, record_values_f, bucket_times_f)
    meters_list = [list(meter) for meter in meters]

    print("Connect local database successfully")
    sql = "INSERT INTO {}(hash_index,design_code, record_time, record_value,bucket_time) VALUES(%s,%s,%s,%s," \
          "%s) ".format(table_name)
    try:
        cur_local.executemany(sql, meters_list)
    except Exception as e:
        print(e)

    conn_local.commit()
    cur_local.close()
    conn_local.close()


def select_camera_data_local_db(start_date=None, end_date=None):
    logger.info("select_camera_data_local_db...")
    conn = psycopg2.connect(database="ArchMind_Canteen", user="postgres", password="pg123", host="127.0.0.1",
                            port="5432")
    sql_get_all = "SELECT * FROM camera_data"
    df = pd.read_sql(sql=sql_get_all, con=conn)
    logger.info("select_camera_data_local_db Done !!!")
    return df


def sync_db_from_local_temp(days=10):
    logger.info("sync_db_from_local_temp...")

    today = datetime.now()
    offset = timedelta(days=-days)
    start_date = (today + offset).strftime('%Y-%m-%d')
    end_date = today.strftime("%Y-%m-%d")
    conn = psycopg2.connect(database="ArchMind_Canteen", user="postgres", password="pg123", host="127.0.0.1",
                            port="5432")
    sql_get_periods = "SELECT * FROM camera_data WHERE record_time > '{}'::timestamp AND record_time < '{}'::timestamp".format(
        start_date, end_date)
    df = pd.read_sql(sql=sql_get_periods, con=conn)
    conn.close()

    conn = psycopg2.connect(database="ArchMind_Canteen", user="postgres", password="pg123", host="127.0.0.1",
                            port="5432")
    cur = conn.cursor()
    print("Connect local database successfully")
    sql = "TRUNCATE TABLE {}".format("camera_data_temp")
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    conn.commit()
    cur.close()
    conn.close()

    indexes, design_codes_f, record_times_f, record_values_f, bucket_times_f = list(df["hash_index"]), list(
        df["design_code"]), list(df["record_time"]), list(df["record_value"]), list(df["bucket_time"])
    meters = zip(indexes, design_codes_f, record_times_f, record_values_f, bucket_times_f)
    meters_list = [list(meter) for meter in meters]

    conn = psycopg2.connect(database="ArchMind_Canteen", user="postgres", password="pg123", host="127.0.0.1",
                            port="5432")
    cur = conn.cursor()
    print("Connect local database successfully")
    sql = "INSERT INTO {}(hash_index,design_code, record_time, record_value,bucket_time) VALUES(%s,%s,%s,%s,%s) ".format(
        "camera_data_temp")
    try:
        cur.executemany(sql, meters_list)
    except Exception as e:
        print(e)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    sync_db_from_local_temp()
