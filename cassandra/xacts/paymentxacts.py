import csv
from cassandra.cluster import Cluster
from decimal import Decimal

def paymentxact(c_w_id, c_d_id, c_id, payment):

    cluster = Cluster(['192.168.51.33','192.168.51.34','192.168.51.35','192.168.51.36','192.168.51.37'])
    session = cluster.connect()

    print(f"payment amount: {payment}")

    # update the warehouse
    cql_query = f"select * from wholesale.warehouse where w_id={c_w_id};"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())
    payment_float = payment
    payment = Decimal(str(payment))
    cql_update = f"update wholesale.warehouse set w_ytd = {payment + result.one().w_ytd}\
            where w_id={c_w_id};"
    session.execute(cql_update)

    # update the district
    cql_query = f"select * from wholesale.district where d_w_id={c_w_id} and d_id={c_d_id}"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())
    cql_update = f"update wholesale.district set d_ytd = {payment + result.one().d_ytd}\
            where d_w_id={c_w_id} and d_id={c_d_id};"
    session.execute(cql_update)

    # update the customer
    cql_query = f"select * from wholesale.customer\
            where c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id}"
    result = session.execute(cql_query)
    assert result.has_more_pages is False, "Expected one row in the result, but found more."
    print(result.one())
    cql_update_1 = f"update wholesale.customer set c_balance={result.one().c_balance - payment}\
            where c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};"
    cql_update_2 = f"update wholesale.customer set c_ytd_payment={result.one().c_ytd_payment + payment_float}\
            where c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};"
    cql_update_3 = f"update wholesale.customer set c_payment_cnt={result.one().c_payment_cnt + payment/payment}\
            where c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};"
    session.execute(cql_update_1)
    session.execute(cql_update_2)
    session.execute(cql_update_3)

    cluster.shutdown()

# paymentxact(1, 5, 2817, 3849.58)

