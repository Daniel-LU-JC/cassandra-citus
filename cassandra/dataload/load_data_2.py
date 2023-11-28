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
CREATE TABLE IF NOT EXISTS district (
    D_W_ID INT,
    D_ID INT,
    D_NAME VARCHAR,
    D_STREET_1 VARCHAR,
    D_STREET_2 VARCHAR,
    D_CITY VARCHAR,
    D_STATE VARCHAR,
    D_ZIP VARCHAR,
    D_TAX DECIMAL,
    D_YTD DECIMAL,
    D_NEXT_O_ID INT,
    PRIMARY KEY (D_W_ID, D_ID)
);
"""
session.execute(table_creation_query)

filename = '/home/stuproj/cs4224o/progs/data_files/district.csv'

with open(filename, mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        insert_query = """
        INSERT INTO district (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(insert_query, (int(row[0]), int(row[1]), row[2], row[3], row[4], row[5], row[6], row[7], float(row[8]), float(row[9]), int(row[10])))

cluster.shutdown()

