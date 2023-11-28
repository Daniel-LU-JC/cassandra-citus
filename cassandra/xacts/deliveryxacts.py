import csv
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime

def delivery(w_id, carrier_id):
    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()
    null_query = f"select o_w_id,o_d_id,o_id,o_c_id,o_carrier_id from wholesale.orders;"
    result = pd.DataFrame(list(session.execute(null_query)))
    result = result[result["o_carrier_id"].isnull() & (result["o_w_id"]==w_id)]
    date = datetime.now()
    for district_number in range(1,11):
        n = result[(result["o_d_id"] == district_number)].sort_values(by="o_id", ascending=True).head(1)
        oid = n["o_id"].to_list()[0]
        ocid = n["o_c_id"].to_list()[0]
        update_query = f"update wholesale.orders set o_carrier_id = {carrier_id}, o_entry_d = '{date}' where o_w_id = {w_id} and o_d_id={district_number} and o_id={oid};"
        update_result=session.execute(update_query)
        orderline_query = f"select ol_w_id, ol_d_id, ol_o_id, ol_amount from wholesale.orderline where ol_w_id={w_id} and ol_o_id = {oid} and ol_d_id = {district_number};"
        orderline_result = pd.DataFrame(list(session.execute(orderline_query)))
        s = orderline_result["ol_amount"].sum()
        customer_query = f"select c_w_id,c_d_id,c_id,c_balance,c_delivery_cnt from wholesale.customer where c_w_id={w_id} and c_d_id={district_number} and c_id = {ocid};"
        customer_result = pd.DataFrame(list(session.execute(customer_query)))
        new_balance = customer_result["c_balance"].to_list()[0] + s
        new_delivery_cnt = customer_result["c_delivery_cnt"].to_list()[0] + 1
        customer_update_query = f"update wholesale.customer set c_balance={new_balance}, c_delivery_cnt={new_delivery_cnt} where c_w_id={w_id} and c_d_id={district_number} and c_id={ocid};"
        customer_update_query = session.execute(customer_update_query)
    cluster.shutdown()

# delivery(1,1)

