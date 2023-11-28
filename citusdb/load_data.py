import psycopg2
import csv
import sys

def convert_csv_null_to_db_null(value):
    if value.lower() == 'null':
        return None
    else:
        return value

def copy_data_from_csv_with_force_null(csv_file, table_name, columns, conn, cur):
    copy_command = f"""
        COPY {table_name} ({', '.join(columns)})
        FROM STDIN
        WITH (
            FORMAT CSV,
            DELIMITER ',',
            NULL 'null',
            FORCE_NULL ({', '.join(columns)})
        )
    """
    with open(csv_file, 'r') as file:
       cur.copy_expert(sql=copy_command, file=file)
       conn.commit()

def insert_data_from_csv_batch(csv_file, table_name, columns, conn, cur, batch_size=10000):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)

        rows = []
        for row in reader:
            # Convert 'null' strings to null values
            row = [convert_csv_null_to_db_null(value) for value in row]
            rows.append(row)

            if len(rows) >= batch_size:
                placeholders = ', '.join(['%s'] * len(columns))
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([placeholders])})"
                cur.executemany(insert_query, rows)
                rows = []

        # Insert any remaining rows
        if rows:
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([placeholders])})"
            cur.executemany(insert_query, rows)

def insert_data_from_csv(csv_file, table_name, columns, conn, cur):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)

        for row in reader:
            # Convert 'null' strings to null values
            row = [convert_csv_null_to_db_null(value) for value in row]
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            values = tuple(row)
            cur.execute(insert_query, values)



def insert_data_into_table(csv_name, host_ip):
    try:
        conn = psycopg2.connect(
            database="project",
            host=host_ip,
            port="5111"
        )

        cur = conn.cursor()


        warehouse_csv = '/home/stuproj/cs4224o/project_files/data_files/warehouse.csv'
        district_csv = '/home/stuproj/cs4224o/project_files/data_files/district.csv'
        customer_csv = '/home/stuproj/cs4224o/project_files/data_files/customer.csv'
        orders_csv = '/home/stuproj/cs4224o/project_files/data_files/order.csv'
        item_csv = '/home/stuproj/cs4224o/project_files/data_files/item.csv'
        orderline_csv = '/home/stuproj/cs4224o/project_files/data_files/order-line.csv'
        stock_csv = '/home/stuproj/cs4224o/project_files/data_files/stock.csv'

        # Define columns for tables
        warehouse_columns = ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD']
        district_columns = ['D_W_ID', 'D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID']
        customer_columns = ['C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2',
                            'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT',
                            'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DELIVERY_CNT', 'C_DATA']
        orders_columns = ['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID', 'O_CARRIER_ID', 'O_OL_CNT', 'O_ALL_LOCAL', 'O_ENTRY_D']
        item_columns = ['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA']
        orderline_columns = ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_DELIVERY_D', 'OL_AMOUNT', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'OL_DIST_INFO']
        stock_columns = ['S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DIST_01', 'S_DIST_02',
                        'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_DATA']


        # Insert data into table
        if csv_name == "1":
            copy_data_from_csv_with_force_null(warehouse_csv, 'Warehouse', warehouse_columns, conn, cur)
        elif csv_name == "2":
            copy_data_from_csv_with_force_null(district_csv, 'District', district_columns, conn, cur)
        elif csv_name == "3":
            copy_data_from_csv_with_force_null(customer_csv, 'Customer', customer_columns, conn, cur)
        elif csv_name == "4":
            copy_data_from_csv_with_force_null(orders_csv, 'Orders', orders_columns, conn, cur)
        elif csv_name == "5":
            copy_data_from_csv_with_force_null(item_csv, 'Item', item_columns, conn, cur)
        elif csv_name == "6":
            copy_data_from_csv_with_force_null(orderline_csv, 'OrderLine', orderline_columns, conn, cur)
        elif csv_name == "7":
            copy_data_from_csv_with_force_null(stock_csv, 'Stock', stock_columns, conn, cur)
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    insert_data_into_table(sys.argv[1], sys.argv[2])
