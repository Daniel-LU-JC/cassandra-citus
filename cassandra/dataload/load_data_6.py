import csv
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
CREATE TABLE IF NOT EXISTS orderline (
    OL_W_ID INT,
    OL_D_ID INT,
    OL_O_ID INT,
    OL_NUMBER INT,
    OL_I_ID INT,
    OL_DELIVERY_D TIMESTAMP,
    OL_AMOUNT DECIMAL,
    OL_SUPPLY_W_ID INT,
    OL_QUANTITY DECIMAL,
    OL_DIST_INFO VARCHAR,
    PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID), OL_NUMBER)
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/order-line.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    batch = BatchStatement()
    counter=0
    import time
    start=time.time()
    for row in reader:
        row = [None if value == 'null' else value for value in row]
        insert_query = """
        INSERT INTO orderline (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        ol_delivery_d = None if row[5] is None else datetime.strptime(row[5], '%Y-%m-%d %H:%M:%S.%f')
        batch.add(insert_query, (int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), ol_delivery_d, float(row[6]), int(row[7]), float(row[8]), row[9]))
        if len(batch) >= 2500:
            session.execute(batch)
            batch = BatchStatement()
            counter+=1
            #if counter % 1000 == 0:
            print(f'6 - batch {counter} done; elapsed time: {time.time()-start}')
    if len(batch) > 0:
        session.execute(batch)

cluster.shutdown()

