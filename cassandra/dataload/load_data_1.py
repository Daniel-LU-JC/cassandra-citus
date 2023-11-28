import csv
from cassandra.cluster import Cluster

cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
session = cluster.connect()

keyspace = "wholesale"
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '3' }};
""")

session.set_keyspace(keyspace)

table_creation_query = """
CREATE TABLE IF NOT EXISTS warehouse (
    W_ID INT PRIMARY KEY,
    W_NAME VARCHAR,
    W_STREET_1 VARCHAR,
    W_STREET_2 VARCHAR,
    W_CITY VARCHAR,
    W_STATE VARCHAR,
    W_ZIP VARCHAR,
    W_TAX DECIMAL,
    W_YTD DECIMAL
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/warehouse.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        insert_query = """
        INSERT INTO warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(insert_query, (int(row[0]), row[1], row[2], row[3], row[4], row[5], row[6], float(row[7]), float(row[8])))

cluster.shutdown()

