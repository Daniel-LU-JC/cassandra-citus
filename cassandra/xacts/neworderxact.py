import csv
from cassandra.cluster import Cluster
from datetime import datetime, timezone

def neworderxact(c_id, w_id, d_id, num, items):

    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()

    assert len(items) == num
    
    # update d_next_o_id by 1
    cql_query = f"select * from wholesale.district\
            where d_w_id={w_id} and d_id={d_id};"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    D_TAX = result.one().d_tax
    cql_update = f"update wholesale.district set d_next_o_id={result.one().d_next_o_id+1}\
            where d_w_id={w_id} and d_id={d_id};"
    session.execute(cql_update)

    # create a new order
    data = {
        "o_id": result.one().d_next_o_id,
        "o_d_id": d_id,
        "o_w_id": w_id,
        "o_c_id": c_id,
        "o_entry_d": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "o_carrier_id": None,
        "o_ol_cnt": num,
        "o_all_local": 1
    }

    # is o_all_local?
    for item in items:
        assert len(item) == 3
        if item[1] != w_id:
            data["o_all_local"] = 0

    total_amount = 0

    # insert the new order
    insert_query = """
        INSERT INTO wholesale.orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
        VALUES (%(o_w_id)s, %(o_d_id)s, %(o_id)s, %(o_c_id)s, %(o_entry_d)s, %(o_carrier_id)s, %(o_ol_cnt)s, %(o_all_local)s)
    """
    print(data)
    session.execute(insert_query, data)

    i = 1
    # traverse each item in the order
    for item in items:
        get_stock = f"select * from wholesale.stock where s_w_id={item[1]} and s_i_id={item[0]};"
        result = session.execute(get_stock)
        assert result.has_more_pages is False, "Expected one row in the result, but found more."
        s_quantity = result.one().s_quantity
        adjusted_qty = s_quantity - item[2]
        if adjusted_qty < 10:
            adjusted_qty = adjusted_qty + 100
        
        # update the stock as follows
        cql_update = f"update wholesale.stock set s_quantity={adjusted_qty} where s_w_id={item[1]} and s_i_id={item[0]};"
        session.execute(cql_update)
        cql_update = f"update wholesale.stock set s_ytd={result.one().s_ytd + item[2]} where s_w_id={item[1]} and s_i_id={item[0]};"
        session.execute(cql_update)
        cql_update = f"update wholesale.stock set s_order_cnt={result.one().s_order_cnt+1} where s_w_id={item[1]} and s_i_id={item[0]};"
        session.execute(cql_update)
        if item[1] != w_id:  # increment s_remote_cnt by 1
            cql_update = f"update wholesale.stock set s_remote_cnt={result.one().s_remote_cnt+1} where s_w_id={item[1]} and s_i_id={item[0]};"
            session.execute(cql_update)

        # update the total amount
        get_price = f"select * from wholesale.item where i_id={item[0]};"
        result = session.execute(get_price)
        assert result.has_more_pages is False, "Expected one row in the result, but found more."
        item_amount = item[2] * result.one().i_price
        total_amount = total_amount + item_amount

        # create a new order-line
        line = {
            "ol_o_id": data["o_id"],
            "ol_d_id": data["o_d_id"],
            "ol_w_id": data["o_w_id"],
            "ol_number": i,
            "ol_i_id": item[0],
            "ol_supply_w_id": item[1],
            "ol_quantity": item[2],
            "ol_amount": item_amount,
            "ol_delivery_d": None,
        }

        # append OL_DIST_INFO
        get_stock = f"select * from wholesale.stock where s_w_id={item[1]} and s_i_id={item[0]};"
        result = session.execute(get_stock)
        assert result.has_more_pages is False, "Expected one row in the result, but found more."
        dist_info_attribute = f"s_dist_{data['o_d_id']:02}"
        line["ol_dist_info"] = getattr(result.one(), dist_info_attribute)

        insert_query = """
            INSERT INTO wholesale.orderline (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
            VALUES (%(ol_w_id)s, %(ol_d_id)s, %(ol_o_id)s, %(ol_number)s, %(ol_i_id)s, %(ol_delivery_d)s, %(ol_amount)s, %(ol_supply_w_id)s, %(ol_quantity)s, %(ol_dist_info)s)
        """
        print(line)
        session.execute(insert_query, line)

        i = i + 1

    # TOTAL_AMOUNT
    cql_query = f"select * from wholesale.warehouse where w_id={w_id};"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    W_TAX = result.one().w_tax
    cql_query = f"select * from wholesale.customer where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())
    C_DISCOUNT = result.one().c_discount
    print(f"W_TAX={W_TAX} D_TAX={D_TAX}")

    total_amount = total_amount * (1+D_TAX+W_TAX) * (1-C_DISCOUNT)
    print(f"NUM_ITEMS={num} TOTAL_AMOUNT={total_amount}")

    cluster.shutdown()

# neworderxact(2289, 1, 3, 7, [(2079, 1, 3), (6215, 1, 2), (38039, 1, 9), (40321, 1, 6), (40615, 1, 5), (47586, 1, 9), (84174, 1, 1)])

