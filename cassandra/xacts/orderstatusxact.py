import csv
from cassandra.cluster import Cluster

def orderstatusxact(c_w_id, c_d_id, c_id):

    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()

    # find the customer first
    cql_query = f"select c_first,c_middle,c_last,c_balance from wholesale.customer\
            where c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id} allow filtering;"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())

    # find this customer's last order
    cql_query = f"select o_id,o_entry_d,o_carrier_id from wholesale.orders\
            where o_w_id={c_w_id} and o_d_id={c_d_id} and o_c_id={c_id} allow filtering;"
    result = session.execute(cql_query)
    sorted_result = sorted(result, key=lambda row: row.o_entry_d, reverse=True)
    last_row = sorted_result[0]
    print(last_row)

    # for each item in the customer's last order
    cql_query = f"select ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d from wholesale.orderline\
            where ol_w_id={c_w_id} and ol_d_id={c_d_id} and ol_o_id={last_row.o_id};"
    result = session.execute(cql_query)
    for row in result:
        print(row)

    cluster.shutdown()

# orderstatusxact(1, 2, 897);

