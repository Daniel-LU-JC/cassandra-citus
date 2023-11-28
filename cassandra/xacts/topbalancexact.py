import csv
import pandas as pd
from cassandra.cluster import Cluster

def topbalance():
    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()
    cql_query = f"select c_first, c_middle, c_last, c_balance, c_d_id, c_w_id from wholesale.customer;"
    result = pd.DataFrame(list(session.execute(cql_query)))
    result["c_balance"] = pd.to_numeric(result["c_balance"])
    result = result.sort_values(by="c_balance", ascending=False).head(10)
    for index, row in result.iterrows():
        cdid, cwid = row['c_d_id'], row['c_w_id']
        balance = row["c_balance"]
        cql_query = f"SELECT d_id, d_name from wholesale.district WHERE d_id = {cdid} AND d_w_id = {cwid};"
        result1 = pd.DataFrame(list(session.execute(cql_query)))
        did, dname = result1["d_id"].to_list()[0], result1["d_name"].to_list()[0]
        cql_query = f"SELECT w_name from wholesale.warehouse WHERE w_id = {did};"
        result2 = pd.DataFrame(list(session.execute(cql_query)))
        wname = result2["w_name"].to_list()[0]
        print("c_first: ",row['c_first'], "c_middle: ", row['c_middle'], "c_last: ",row['c_last'], "c_balance: ",row['c_balance'], "w_name: ", wname, "d_name: ", dname)
    cluster.shutdown()

# topbalance()

