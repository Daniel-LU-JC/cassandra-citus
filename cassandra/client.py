import sys
import time
import numpy as np

# import the transaction functions defined elsewhere
from xacts.neworderxact import neworderxact
from xacts.paymentxacts import paymentxact
# from xacts.deliveryxacts import delivery
from xacts.orderstatusxact import orderstatusxact
# from xacts.stocklevelxacts import stocklevel
from xacts.popularitemxact import popularitemxact
# from xacts.topbalancexact import topbalance

def new_order(c_id, w_id, d_id, m):

    order_details = []

    # Read the remaining M lines for item details
    for _ in range(m):
        ol_i_id, ol_supply_w_id, ol_quantity = map(int, input().strip().split(','))
        order_details.append((ol_i_id, ol_supply_w_id, ol_quantity))

    # start the autual logic
    neworderxact(c_id, w_id, d_id, m, order_details)

def process_payment(c_w_id, c_d_id, c_id, payment):
    paymentxact(c_w_id, c_d_id, c_id, payment)

def process_delivery(w_id, carrier_id):
    pass
    # delivery(w_id, carrier_id)

def get_order_status(c_w_id, c_d_id, c_id):
    orderstatusxact(c_w_id, c_d_id, c_id)

def examine_stock_level(w_id, d_id, t, l):
    # stocklevel(w_id, d_id, t, l)
    pass

def find_most_popular_items(w_id, d_id, l):
    popularitemxact(w_id, d_id, l)

def find_top_balance_customers():
    # topbalance()
    pass

def find_related_customer(c_w_id, c_d_id, c_id):
    # Placeholder for your actual logic
    pass

def main_driver():
    start_time = time.time()
    transactions = 0
    latencies = []

    try:
        while True:
            line = input().strip()
            if not line:
                break

            parts = line.split(',')
            command = parts[0]
            if command != 'P':
                arguments = list(map(int, parts[1:]))

            start_transaction = time.time()

            # Invoke the appropriate transaction function based on transaction_type
            if command == 'N':
                new_order(*arguments)
            if command == 'P':
                arguments = list(map(int, parts[1:-1])) + [float(parts[-1])]
                print()
                process_payment(*arguments)
            if command == 'D':
                process_delivery(*arguments)
            if command == 'O':
                get_order_status(*arguments)
            if command == 'S':
                examine_stock_level(*arguments)
            if command == 'I':
                find_most_popular_items(*arguments)
            if command == 'T':
                find_top_balance_customers()
            if command == 'R':
                find_related_customer(*arguments)
            

            end_transaction = time.time()
            transactions += 1
            latencies.append((end_transaction - start_transaction) * 1000)
    except EOFError as e:
        pass

    total_time = round(time.time() - start_time, 2)
    average_latency = round(np.mean(latencies), 2)
    median_latency = round(np.median(latencies), 2)
    percentile_95 = round(np.percentile(latencies, 95), 2)
    percentile_99 = round(np.percentile(latencies, 99), 2)

    # Output metrics to stderr
    # print(f'Total number of transactions processed: {transactions}', file=sys.stderr)
    # print(f'Total elapsed time (seconds): {total_time}', file=sys.stderr)
    # print(f'Transaction throughput (transactions per second): {transactions/total_time}', file=sys.stderr)
    # print(f'Average transaction latency (ms): {average_latency}', file=sys.stderr)
    # print(f'Median transaction latency (ms): {median_latency}', file=sys.stderr)
    # print(f'95th percentile transaction latency (ms): {percentile_95}', file=sys.stderr)
    # print(f'99th percentile transaction latency (ms): {percentile_99}', file=sys.stderr)
    print(f"{transactions},{total_time},{transactions/total_time},{average_latency},{median_latency},{percentile_95},{percentile_99}", file=sys.stderr)

if __name__ == '__main__':
    main_driver()
