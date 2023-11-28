import csv
import pandas as pd
from cassandra.cluster import Cluster

def stocklevel(w_id,d_id,t, l):
    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()
    district_query = f"select * from wholesale.district where d_w_id={w_id} and d_id={d_id};"
    district_result = pd.DataFrame(list(session.execute(district_query)))
    #print(district_result.head())
    n = district_result["d_next_o_id"].to_list()[0]
    s = 0
    for i in range(n-l, n):
        orderline_query = f"select ol_w_id,ol_d_id,ol_o_id,ol_i_id,ol_quantity from wholesale.orderline where ol_w_id={w_id} and ol_d_id={d_id} and ol_o_id={i};"
        orderline_result = pd.DataFrame(list(session.execute(orderline_query)))
        #print(orderline_result.head())
        threshold = orderline_result[orderline_result["ol_quantity"] < t]
        #print(threshold.head())
        #print(threshold.shape)
        s += threshold.shape[0]
    print(s)
    cluster.shutdown()

# stocklevel(1,1,10,5)

