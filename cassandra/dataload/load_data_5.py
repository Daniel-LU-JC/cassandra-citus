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
CREATE TABLE IF NOT EXISTS item (
    I_ID INT PRIMARY KEY,
    I_NAME VARCHAR,
    I_PRICE DECIMAL,
    I_IM_ID INT,
    I_DATA VARCHAR
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/item.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    batch = BatchStatement()
    counter=0
    start=time.time()
    for row in reader:
        insert_query = """
        INSERT INTO item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA)
        VALUES (%s, %s, %s, %s, %s)
        """
        batch.add(insert_query, (int(row[0]), row[1], float(row[2]), int(row[3]), row[4]))
        if len(batch) >= 2500:
            session.execute(batch)
            batch = BatchStatement()
            counter+=1
            if counter % 1000 == 0:
                print(f'5 - batch {counter} done; elapsed time: {time.time()-start}')
    if len(batch) > 0:
        session.execute(batch)

cluster.shutdown()

