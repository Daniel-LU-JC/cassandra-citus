import csv
from cassandra.cluster import Cluster
from collections import defaultdict

def popularitemxact(w_id, d_id, l):

    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()

    multi_values_dict = defaultdict(list)

    # find the target district first
    cql_query = f"select d_w_id,d_id from wholesale.district where d_w_id={w_id} and d_id={d_id};"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())

    print(f"Number of last orders to be examined: {l}")

    # find the orders to be examined based on o_id
    cql_query = f"select o_id,o_entry_d,o_c_id from wholesale.orders where o_w_id={w_id} and o_d_id={d_id}"
    result = session.execute(cql_query)
    sorted_result = sorted(result, key=lambda row: row.o_id, reverse=True)
    for i in range(l):
        # order number & entry date and time & customer ID
        print(sorted_result[i])

        # name of the customer who placed this order
        query = f"select c_first,c_middle,c_last from wholesale.customer\
                where c_w_id={w_id} and c_d_id={d_id} and c_id={sorted_result[i].o_c_id};"
        person = session.execute(query)
        assert person.has_more_pages is False, "Expected one row in the result, but found more."
        print(person.one())

        # for each popular item in the order
        query = f"select ol_i_id,ol_quantity from wholesale.orderline\
                where ol_w_id={w_id} and ol_d_id={d_id} and ol_o_id={sorted_result[i].o_id};"
        items = session.execute(query)
        sorted_items = sorted(items, key=lambda row: row.ol_quantity, reverse=True)

        max_quantity = max(sorted_items, key=lambda row: row.ol_quantity).ol_quantity
        max_quantity_items = [item for item in sorted_items if item.ol_quantity == max_quantity]
        for item in max_quantity_items:
            print(item)
            query = f"select i_name from wholesale.item where i_id={item.ol_i_id};"
            itemitem = session.execute(query)
            assert itemitem.has_more_pages is False, "Expected one row in the result, but found more."
            print(itemitem.one())
            multi_values_dict[itemitem.one().i_name].append(sorted_result[i].o_id)
    for item_name, o_ids in multi_values_dict.items():
        print(f"Item Name: {item_name}, Orders: {o_ids}, percentage: {len(o_ids) / l * 100}%")

    cluster.shutdown()

# popularitemxact(1, 2, 35);

