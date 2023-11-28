import sys
import time
import numpy as np
import psycopg2
from datetime import datetime
from collections import defaultdict


def new_order(conn, C_ID, W_ID, D_ID, NUM_ITEMS):

    order_details = [[],[],[]]

    # Read the remaining M lines for item details
    for _ in range(NUM_ITEMS):
        ol_i_id, ol_supply_w_id, ol_quantity = map(int, input().strip().split(','))
        order_details[0].append(ol_i_id)
        order_details[1].append(ol_supply_w_id)
        order_details[2].append(ol_quantity)

    ITEM_NUM = order_details[0]
    SUPPLIER_WAREHOUSE = order_details[1]
    QUANTITY = order_details[2]

    try:
        cursor = conn.cursor()
        try:
            NEXT_O_ID = 0
            NEXT_O_ALL_LOCAL = 1
            TOTAL_AMOUNT = 0
            
            # Get the next order ID and update District 
            cursor.execute(""" 
                UPDATE District 
                SET D_NEXT_O_ID = D_NEXT_O_ID + 1 
                WHERE D_W_ID = %s AND D_ID = %s 
                RETURNING D_NEXT_O_ID; 
            """, (W_ID, D_ID)) 
            result = cursor.fetchone() 
            NEXT_O_ID = result[0] - 1 

            # Check if all items are from the same warehouse
            for i in range(NUM_ITEMS):
                if SUPPLIER_WAREHOUSE[i] != W_ID:
                    NEXT_O_ALL_LOCAL = 0
                    break

            # Insert a new order
            cursor.execute("INSERT INTO Orders(O_W_ID, O_D_ID, O_ID, O_C_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) "
                        "VALUES (%s, %s, %s, %s, 0, %s, NOW());",
                        (W_ID, D_ID, NEXT_O_ID, C_ID, NEXT_O_ALL_LOCAL))

            for i in range(NUM_ITEMS):
                # Get the S_QUANTITY
                cursor.execute("SELECT S_QUANTITY FROM Stock WHERE S_W_ID = %s AND S_I_ID = %s;",
                            (SUPPLIER_WAREHOUSE[i], ITEM_NUM[i]))
                result = cursor.fetchone()
                S_QUANTITY = result[0]

                # Calculate ADJUSTED_QUANTITY
                ADJUSTED_QUANTITY = S_QUANTITY - QUANTITY[i]
                if ADJUSTED_QUANTITY < 10:
                    ADJUSTED_QUANTITY += 100

                # Update Stock
                cursor.execute("UPDATE Stock SET S_QUANTITY = %s, S_YTD = S_YTD + %s, "
                            "S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = CASE WHEN %s != %s THEN S_REMOTE_CNT + 1 ELSE S_REMOTE_CNT END "
                            "WHERE S_W_ID = %s AND S_I_ID = %s;",
                            (ADJUSTED_QUANTITY, QUANTITY[i], SUPPLIER_WAREHOUSE[i], W_ID, SUPPLIER_WAREHOUSE[i], ITEM_NUM[i]))

                # Get I_PRICE
                cursor.execute("SELECT I_PRICE FROM Item WHERE I_ID = %s;", (ITEM_NUM[i],))
                result = cursor.fetchone()
                I_PRICE = result[0]

                # Calculate ITEM_AMOUNT
                ITEM_AMOUNT = I_PRICE * QUANTITY[i]
                TOTAL_AMOUNT += ITEM_AMOUNT

                # Insert into OrderLine
                cursor.execute("INSERT INTO OrderLine(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            (W_ID, D_ID, NEXT_O_ID, i, ITEM_NUM[i], ITEM_AMOUNT, SUPPLIER_WAREHOUSE[i], QUANTITY[i], 'S_DIST_' + str(D_ID)))

            result = f"Customer identifier: {W_ID}, {D_ID}, {C_ID}\n" \
                     f"Order number: {NEXT_O_ID}\n" \
                     f"Entry date: {datetime.now()}\n" \
                     f"Number of items: {NUM_ITEMS}\n" \
                     f"Total amount for order: {TOTAL_AMOUNT}\n"
            conn.commit()
            print(result)
        except Exception as e:
            print(f"error message: {e}")
            conn.rollback()
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")


def process_payment(conn, C_W_ID, C_D_ID, C_ID, PAYMENT):
    try:
        cursor = conn.cursor()

        try:
            # Update the Warehouse table
            cursor.execute(f"UPDATE Warehouse SET W_YTD = W_YTD + {PAYMENT} WHERE W_ID = {C_W_ID};")
            
            # Update the District table
            cursor.execute(f"UPDATE District SET D_YTD = D_YTD + {PAYMENT} WHERE D_W_ID = {C_W_ID} AND D_ID = {C_D_ID};")
            
            # Update the Customer table
            cursor.execute(f"UPDATE Customer SET C_BALANCE = C_BALANCE - {PAYMENT}, "
                           f"C_YTD_PAYMENT = C_YTD_PAYMENT + {PAYMENT}, "
                           f"C_PAYMENT_CNT = C_PAYMENT_CNT + 1 "
                           f"WHERE C_W_ID = {C_W_ID} AND C_D_ID = {C_D_ID} AND C_ID = {C_ID};")
            
            # Query the Customer table and get the customer information
            cursor.execute(f"SELECT 'Customer Identifier: (' || {C_W_ID} || ', ' || {C_D_ID} || ', ' || {C_ID} || ')', "
                           f"'Name: (' || C_FIRST || ' ' || C_MIDDLE || ' ' || C_LAST || ')', "
                           f"'Address: (' || C_STREET_1 || ', ' || C_STREET_2 || ', ' || C_CITY || ', ' || C_STATE || ', ' || C_ZIP || ')', "
                           f"'Phone: ' || C_PHONE, "
                           f"'Since: ' || C_SINCE, "
                           f"'Credit: ' || C_CREDIT, "
                           f"'Credit Limit: ' || C_CREDIT_LIM, "
                           f"'Discount: ' || C_DISCOUNT, "
                           f"'Balance: ' || C_BALANCE "
                           f"FROM Customer "
                           f"WHERE C_W_ID = {C_W_ID} AND C_D_ID = {C_D_ID} AND C_ID = {C_ID};")
            customer_info = cursor.fetchone()
            
            # Query the Warehouse table and get the warehouse information
            cursor.execute(f"SELECT 'Warehouse Address: (' || W_STREET_1 || ', ' || W_STREET_2 || ', ' || W_CITY || ', ' || W_STATE || ', ' || W_ZIP || ')'"
                           f"FROM Warehouse WHERE W_ID = {C_W_ID};")
            warehouse_info = cursor.fetchone()
            
            # Query the District table and get the district information
            cursor.execute(f"SELECT 'District Address: (' || D_STREET_1 || ', ' || D_STREET_2 || ', ' || D_CITY || ', ' || D_STATE || ', ' || D_ZIP || ')'"
                           f"FROM District WHERE D_W_ID = {C_W_ID} AND D_ID = {C_D_ID};")
            district_info = cursor.fetchone()
            
            # Create the result text
            result = "\n".join([str(info) for info in (customer_info, warehouse_info, district_info)])
            result += f"\nPayment amount: {PAYMENT}"    
            conn.commit()
            print(result)
        except Exception as e:
            print(f"error message: {e}")
            conn.rollback()
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")



def process_delivery(conn, W_ID, CARRIER_ID):
    # Create a connection to the database
    try:
        cursor = conn.cursor()
        try:
            MIN_N = 2000000000
            for DISTRICT_NO in range(1, 11):
                # Find the minimum O_ID for the specified warehouse, district, and carrier_id
                cursor.execute("SELECT min(O_ID) FROM Orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_CARRIER_ID IS NULL;", (W_ID, DISTRICT_NO,))
                result = cursor.fetchone()
                if result:
                    MIN_N = result[0]
                else:
                    continue

                # Update the O_CARRIER_ID for the found order
                cursor.execute("UPDATE Orders SET O_CARRIER_ID = %s WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s;", (CARRIER_ID, W_ID, DISTRICT_NO, MIN_N))

                # Update the OL_DELIVERY_D for the corresponding order lines
                cursor.execute("UPDATE OrderLine SET OL_DELIVERY_D = CURRENT_TIMESTAMP WHERE OL_O_ID = %s AND OL_W_ID = %s AND OL_D_ID = %s;", (MIN_N, W_ID, DISTRICT_NO))

                # Calculate the total amount for the order lines
                cursor.execute("SELECT sum(OL_AMOUNT) AS TOTAL FROM OrderLine WHERE OL_O_ID = %s AND OL_W_ID = %s AND OL_D_ID = %s;", (MIN_N, W_ID, DISTRICT_NO))
                result = cursor.fetchone()
                if result:
                    TOTAL = result[0]
                else:
                    TOTAL = 0

                # Find the customer ID associated with the order
                cursor.execute("SELECT O_C_ID FROM Orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s;", (W_ID, DISTRICT_NO, MIN_N))
                result = cursor.fetchone()
                if result:
                    CUSTOMER_ID = result[0]
                else:
                    CUSTOMER_ID = None

                # Update the customer's balance and delivery count
                if CUSTOMER_ID:
                    cursor.execute("UPDATE Customer SET C_BALANCE = C_BALANCE + %s, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;", (TOTAL, W_ID, DISTRICT_NO, CUSTOMER_ID))
            conn.commit()
        except Exception as e:
            print(f"error message: {e}")
            conn.rollback()
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")

def get_order_status(conn, C_W_ID, C_D_ID, C_ID):
    try:
        cursor = conn.cursor()
        try:

            # Query the Customer table and get the customer information
            cursor.execute(f"SELECT 'Customer name: (' || C_FIRST || ', ' || C_MIDDLE || ', ' || C_LAST || ') ' || 'Balance: ' || C_BALANCE "
                           f"FROM Customer "
                           f"WHERE C_W_ID = {C_W_ID} AND C_D_ID = {C_D_ID} AND C_ID = {C_ID};")
            customer_info = cursor.fetchone()
            
            # Query the Orders table to get the last order number
            cursor.execute(f"SELECT max(O_ID) FROM Orders WHERE O_W_ID = {C_W_ID} AND O_D_ID = {C_D_ID};")
            last_o_id = cursor.fetchone()[0]

            # Query the Orders table to get the last order information
            cursor.execute(f"SELECT 'Last order number: ' || O_ID || ' ' || 'Last order timestamp: ' || O_ENTRY_D ||"
                           f"' ' || 'Last order carrier id: ' || COALESCE(CAST(O_CARRIER_ID AS VARCHAR), 'None') "
                           f"FROM Orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s;", (C_W_ID, C_D_ID, last_o_id))
            last_order_info = cursor.fetchone()
            
            # Initialize a string to hold the items in the last order
            items_in_order = ''
            
            # Query the OrderLine table for items in the last order
            cursor.execute(f"SELECT * FROM OrderLine WHERE OL_W_ID = {C_W_ID} AND OL_D_ID = {C_D_ID} AND OL_O_ID = {last_o_id};")
            orderline_rows = cursor.fetchall()
            
            for orderline_row in orderline_rows:
                items_in_order += (f'Item number: {orderline_row[4]} '
                                   f'Supplying warehouse number: {orderline_row[7]} '
                                   f'Quantity ordered: {orderline_row[8]} '
                                   f'Total price: {orderline_row[6]} '
                                   f'Date and time of delivery: {orderline_row[5]}\n')
            
            # Create the result text
            result = f'{customer_info[0]}\n{last_order_info[0]}\n{items_in_order}'
            print(result)
        except Exception as e:
            print(f"error message: {e}")
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")

def examine_stock_level(conn, W_ID, D_ID, T, L):
    try:
        cursor = conn.cursor()
        try:
            # find the next available order number of the specified warehouse id and district id
            cursor.execute("""
            SELECT D_NEXT_O_ID
            FROM district
            WHERE D_W_ID = %s AND D_ID = %s;
            """, (W_ID, D_ID))

            N = cursor.fetchone()[0]

            # find the set of item id of the orderline for the specified warehouse id and district id and order id between N - L and N 
            cursor.execute("""
            SELECT OL_I_ID
            FROM orderline
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID BETWEEN %s - %s AND %s;
            """, (W_ID, D_ID, N, L, N))

            S = cursor.fetchall()

            below_threshold_count = 0
            # for each item in the set find the item with stock quantity less than the threshold and then sum them up 
            for item in S:
                    cursor.execute("""
                    SELECT COUNT(*)
                    FROM Stock
                    WHERE S_W_ID = %s AND S_I_ID = %s AND S_QUANTITY < %s;
                    """, (W_ID, item[0], T))
                    count = cursor.fetchone()[0]
                    below_threshold_count += count
            print(f"Number of items below the stock threshold ({T}): {below_threshold_count}")
        except Exception as e:
            print(f"error message: {e}")
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")

def find_most_popular_items(conn, W_ID, D_ID, L):
    try:
        cursor = conn.cursor()
        multi_values_dict = defaultdict(list)
        cursor.execute("""
            SELECT D_NEXT_O_ID
            FROM District
            WHERE D_W_ID = %s AND D_ID = %s;
        """, (W_ID, D_ID))
        N = cursor.fetchone()[0]
        print(f"1. District Identifier (W_ID={W_ID},D_ID={D_ID})")
        print(f"2. Number of last orders to be examined: {L}")

        # Find the orders to be examined based on o_id
        cursor.execute("""
            SELECT O_ID, O_ENTRY_D, O_C_ID
            FROM Orders
            WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID BETWEEN %s - %s AND %s;;
        """, (W_ID, D_ID, N, L, N))
        sorted_result = sorted(cursor.fetchall(), key=lambda row: row[0], reverse=True)
        print(f"3. For each order number x in S")
        for i in range(L):

            # Name of the customer who placed this order
            cursor.execute("""
                SELECT C_FIRST, C_MIDDLE, C_LAST
                FROM Customer
                WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;
            """, (W_ID, D_ID, sorted_result[i][2]))
            person = cursor.fetchone()
            order_number = sorted_result[i][0]
            order_entry_dt = sorted_result[i][1]
            print(f"----------------------------------------")
            print(f"Order Number O_ID: {order_number}")
            print(f"Order Entry Date Time: {order_entry_dt}")
            print(f"Name of Customer: {person}")
            # For each popular item in the order
            cursor.execute("""
                SELECT OL_I_ID, OL_QUANTITY
                FROM Orderline
                WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s;
            """, (W_ID, D_ID, sorted_result[i][0]))
            sorted_items = sorted(cursor.fetchall(), key=lambda row: row[1], reverse=True)
            max_quantity = max(sorted_items, key=lambda row: row[1])[1]
            max_quantity_items = [item for item in sorted_items if item[1] == max_quantity]
            for item in max_quantity_items:
                cursor.execute("""
                    SELECT I_NAME
                    FROM Item
                    WHERE I_ID = %s;
                """, (item[0],))
                item_name = cursor.fetchone()[0]
                print(f"Item Name: {item_name}, Quantity Ordered: {max_quantity}")
                multi_values_dict[item_name].append(sorted_result[i][0])
        print(f"---------------------------------------")
        print(f"4. The percentage of examined orders that contain each popular items")
        for item_name, o_ids in multi_values_dict.items():
            print(f"Item Name: {item_name},  percentage: {len(o_ids) / L * 100}%")
    
    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()



def find_top_balance_customers(conn):
    try:
        cursor = conn.cursor()
        try:
            result = ''
            # Create a temporary table TOP_TEN_W
            cursor.execute("""
                CREATE TEMP TABLE TOP_TEN AS
                SELECT
                    C_W_ID, C_D_ID, C_ID,
                    C_FIRST || ' ' || C_MIDDLE || ' ' || C_LAST AS Name,
                    C_BALANCE
                FROM
                    Customer
                ORDER BY
                    C_BALANCE DESC
                LIMIT 10;
            """)
            cursor.execute("""
                CREATE TEMP TABLE TOP_TEN_W AS
                SELECT
                    C.C_W_ID, C.C_D_ID, C.C_ID, C.Name, C_BALANCE, W.W_NAME
                FROM TOP_TEN C, Warehouse W
                WHERE C.C_W_ID = W.W_ID;
            """)

            # Create a temporary view TOP_TEN_W_D
            cursor.execute("""
                CREATE TEMP VIEW TOP_TEN_W_D AS
                SELECT
                    C.Name, C.C_BALANCE, C.W_NAME, D.D_NAME
                FROM TOP_TEN_W C, District D
                WHERE C.C_W_ID = D.D_W_ID AND C.C_D_ID = D.D_ID;
            """)

            # Fetch data from the temporary view and construct the result string
            cursor.execute("""
                SELECT * FROM TOP_TEN_W_D;
            """)
            rows = cursor.fetchall()
            for row in rows:
                result += f"Name: {row[0]}, Outstanding balance: {row[1]}, Warehouse name: {row[2]}, District name: {row[3]}\n"

            cursor.execute("DROP VIEW IF EXISTS TOP_TEN_W_D;")
            cursor.execute("DROP TABLE IF EXISTS TOP_TEN_W;")
            cursor.execute("DROP TABLE IF EXISTS TOP_TEN;")
            print(result)
        except Exception as e:
            print(f"error message: {e}")
        finally:
            cursor.close()
    except Exception as e:
        print(f"error message: {e}")



def find_related_customer(conn, C_W_ID, C_D_ID, C_ID):
    try:
        cursor = conn.cursor()

        try:
            RESULT = ''

            # Create a temporary table FILTERED_O
            cursor.execute("""
                CREATE TEMP TABLE FILTERED_O AS
                SELECT O.O_W_ID, O.O_D_ID, O.O_C_ID, O.O_ID
                FROM Orders O
                WHERE O.O_W_ID = %s AND O.O_D_ID = %s AND O.O_C_ID = %s
            """, (C_W_ID, C_D_ID, C_ID))

            # Create a temporary table FILTERED_N
            cursor.execute("""
                CREATE TEMP TABLE FILTERED_N AS
                SELECT O.O_W_ID, O.O_D_ID, O.O_C_ID, O.O_ID
                FROM Customer C, Orders O
                WHERE C.C_W_ID = O.O_W_ID AND C.C_D_ID = O.O_D_ID AND C.C_ID = O.O_C_ID AND C.C_W_ID <> %s
            """, (C_W_ID,))

            # Create temporary tables OL1 and OL2
            cursor.execute("""
                CREATE TEMP TABLE OL1 AS
                SELECT F1.O_W_ID, F1.O_D_ID, F1.O_C_ID, F1.O_ID, OL.OL_NUMBER, OL.OL_I_ID
                FROM OrderLine OL, FILTERED_O F1
                WHERE OL.OL_W_ID = F1.O_W_ID AND OL.OL_D_ID = F1.O_D_ID AND OL.OL_O_ID = F1.O_ID
            """)

            cursor.execute("""
                CREATE TEMP TABLE OL2 AS
                SELECT F1.O_W_ID, F1.O_D_ID, F1.O_C_ID, F1.O_ID, OL.OL_NUMBER, OL.OL_I_ID
                FROM OrderLine OL, FILTERED_N F1
                WHERE OL.OL_W_ID = F1.O_W_ID AND OL.OL_D_ID = F1.O_D_ID AND OL.OL_O_ID = F1.O_ID
            """)

            # Create a temporary table COMBINED
            cursor.execute("""
                CREATE TEMP TABLE COMBINED AS
                SELECT OL2.O_W_ID, OL2.O_D_ID, OL2.O_C_ID, OL2.O_ID, COUNT(*) AS NUM
                FROM OL1, OL2
                WHERE OL1.OL_I_ID = OL2.OL_I_ID
                GROUP BY OL2.O_W_ID, OL2.O_D_ID, OL2.O_C_ID, OL2.O_ID
            """)

            # Create a temporary table RELATED_CUSTOMERS_TABLE
            cursor.execute("""
                CREATE TEMP TABLE RELATED_CUSTOMERS_TABLE AS
                SELECT C.O_W_ID, C.O_D_ID, C.O_C_ID
                FROM COMBINED C
                WHERE NUM >= 2
            """)

            # Iterate over related customers and build the RESULT string
            cursor.execute("SELECT * FROM RELATED_CUSTOMERS_TABLE")
            for row in cursor.fetchall():
                RESULT += f'Customer identifier: ({row[0]}, {row[1]}, {row[2]})\n'


            print(RESULT)

        finally:
            # Drop the temporary tables
            cursor.execute("DROP TABLE IF EXISTS FILTERED_O")
            cursor.execute("DROP TABLE IF EXISTS FILTERED_N")
            cursor.execute("DROP TABLE IF EXISTS OL1")
            cursor.execute("DROP TABLE IF EXISTS OL2")
            cursor.execute("DROP TABLE IF EXISTS COMBINED")
            cursor.execute("DROP TABLE IF EXISTS RELATED_CUSTOMERS_TABLE")

            cursor.close()
    except Exception as e:
        print(f"error message: {e}")

def main_driver(host):
    start_time = time.time()
    transactions = 0
    latencies = []

    conn = psycopg2.connect(
        database="project",
        host= host,
        port="5111"
        )

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
                new_order(conn, *arguments)
            if command == 'P':
                arguments = list(map(int, parts[1:-1])) + [float(parts[-1])]
                print()
                process_payment(conn, *arguments)
            if command == 'D':
                process_delivery(conn, *arguments)
            if command == 'O':
                get_order_status(conn, *arguments)
            if command == 'S':
                examine_stock_level(conn, *arguments)
            if command == 'I':
                find_most_popular_items(conn, *arguments)
            if command == 'T':
                find_top_balance_customers(conn)
            if command == 'R':
                find_related_customer(conn, *arguments)
            

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
    main_driver(sys.argv[1])

