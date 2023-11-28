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
CREATE TABLE IF NOT EXISTS orders (
    O_W_ID INT,
    O_D_ID INT,
    O_ID INT,
    O_C_ID INT,
    O_CARRIER_ID INT,
    O_OL_CNT DECIMAL,
    O_ALL_LOCAL DECIMAL,
    O_ENTRY_D TIMESTAMP,
    PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/order.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    batch = BatchStatement()
    counter=0
    start=time.time()
    for row in reader:
        row = [None if value == 'null' else value for value in row]
        insert_query = """
        INSERT INTO orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        o_carrier_id = None if row[4] is None else int(row[4])
        batch.add(insert_query, (int(row[0]), int(row[1]), int(row[2]), int(row[3]), o_carrier_id, float(row[5]), float(row[6]), datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S.%f')))
        if len(batch) >= 2500:
            session.execute(batch)
            batch = BatchStatement()
            counter+=1
            if counter % 1000 == 0:
                print(f'4 - batch {counter} done; elapsed time: {time.time()-start}')
    if len(batch) > 0:
        session.execute(batch)

cluster.shutdown()

