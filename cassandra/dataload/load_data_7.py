import csv
import time
from cassandra.cluster import Cluster
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
CREATE TABLE IF NOT EXISTS stock (
    S_W_ID INT,
    S_I_ID INT,
    S_QUANTITY DECIMAL,
    S_YTD DECIMAL, 
    S_ORDER_CNT INT,
    S_REMOTE_CNT INT,
    S_DIST_01 VARCHAR,
    S_DIST_02 VARCHAR,
    S_DIST_03 VARCHAR,
    S_DIST_04 VARCHAR,
    S_DIST_05 VARCHAR,
    S_DIST_06 VARCHAR,
    S_DIST_07 VARCHAR,
    S_DIST_08 VARCHAR,
    S_DIST_09 VARCHAR,
    S_DIST_10 VARCHAR,
    S_DATA VARCHAR,
    PRIMARY KEY (S_W_ID, S_I_ID)
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/stock.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    batch = BatchStatement()
    counter=0
    start=time.time()
    for row in reader:
        insert_query = """
        INSERT INTO stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch.add(insert_query, (int(row[0]), int(row[1]), float(row[2]), float(row[3]), int(row[4]), int(row[5]), row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16]))
        if len(batch) >= 750:
            session.execute(batch)
            batch = BatchStatement()
            counter+=1
            if counter % 1000 == 0:
                print(f'7 - batch {counter} done; elapsed time: {time.time()-start}')
    if len(batch) > 0:
        session.execute(batch)

cluster.shutdown()

