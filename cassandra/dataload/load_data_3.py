import csv
import time
from cassandra.cluster import Cluster
from datetime import datetime
from cassandra.query import BatchStatement

cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
session = cluster.connect()

keyspace = "wholesale"
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '3' }};
""")

session.set_keyspace(keyspace)

table_creation_query = """
CREATE TABLE IF NOT EXISTS customer (
    C_W_ID INT,
    C_D_ID INT,
    C_ID INT,
    C_FIRST VARCHAR,
    C_MIDDLE VARCHAR,
    C_LAST VARCHAR,
    C_STREET_1 VARCHAR,
    C_STREET_2 VARCHAR,
    C_CITY VARCHAR,
    C_STATE VARCHAR,
    C_ZIP VARCHAR,
    C_PHONE VARCHAR,
    C_SINCE TIMESTAMP,
    C_CREDIT VARCHAR,
    C_CREDIT_LIM DECIMAL,
    C_DISCOUNT DECIMAL,
    C_BALANCE DECIMAL,
    C_YTD_PAYMENT FLOAT,
    C_PAYMENT_CNT INT,
    C_DELIVERY_CNT INT,
    C_DATA VARCHAR,
    PRIMARY KEY ((C_W_ID, C_D_ID), C_ID))
;
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/customer.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    batch = BatchStatement()
    counter=0
    start = time.time()
    for row in reader:
        insert_query = """
        INSERT INTO customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch.add(insert_query, (int(row[0]), int(row[1]), int(row[2]), row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], datetime.strptime(row[12], '%Y-%m-%d %H:%M:%S.%f'), row[13], float(row[14]), float(row[15]), float(row[16]), float(row[17]), int(row[18]), int(row[19]), row[20]))
        if len(batch) >= 1500:
            session.execute(batch)
            batch = BatchStatement()
            counter+=1
            if counter % 1000 == 0:
                print(f'3 - batch {counter} done; elapsed time: {time.time()-start}')
    if len(batch) > 0:
        session.execute(batch)

cluster.shutdown()

