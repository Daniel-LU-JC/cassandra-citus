import psycopg2
import sys

def create_tables(host_ip):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            database="project",
            host=host_ip,
            port="5111"
        )

        cur = conn.cursor()

        # Define queries to create tables
        create_table_warehouse_query = '''
            CREATE TABLE IF NOT EXISTS Warehouse
            (
                W_ID int,
                W_NAME varchar(10),
                W_STREET_1 varchar(20),
                W_STREET_2 varchar(20),
                W_CITY varchar(20),
                W_STATE char(2),
                W_ZIP char(9),
                W_TAX decimal(4,4),
                W_YTD decimal(12,2),
                primary key(W_ID)
            )
        '''

        create_table_district_query = '''
            CREATE TABLE IF NOT EXISTS District (
                D_W_ID int,
                D_ID int,
                D_NAME varchar(10),
                D_STREET_1 varchar(20),
                D_STREET_2 varchar(20),
                D_CITY varchar(20),
                D_STATE char(2),
                D_ZIP char(9),
                D_TAX decimal(4,4),
                D_YTD decimal(12,2),
                D_NEXT_O_ID int,
                primary key(D_W_ID, D_ID)
            )
        '''

        create_table_customer_query = '''
            CREATE TABLE IF NOT EXISTS Customer (
                C_W_ID int,
                C_D_ID int,
                C_ID int,
                C_FIRST varchar(16),
                C_MIDDLE char(2),
                C_LAST varchar(16),
                C_STREET_1 varchar(20),
                C_STREET_2 varchar(20),
                C_CITY varchar(20),
                C_STATE char(2),
                C_ZIP char(9),
                C_PHONE char(16),
                C_SINCE timestamp,
                C_CREDIT char(2),
                C_CREDIT_LIM decimal(12,2),
                C_DISCOUNT decimal(5,4),
                C_BALANCE decimal(12,2),
                C_YTD_PAYMENT float,
                C_PAYMENT_CNT int,
                C_DELIVERY_CNT int,
                C_DATA varchar(500),
                primary key (C_W_ID, C_D_ID, C_ID)
            )
        '''

        create_table_orders_query = '''
            CREATE TABLE IF NOT EXISTS Orders (
                O_W_ID int,
                O_D_ID int,
                O_ID int,
                O_C_ID int,
                O_CARRIER_ID int,
                O_OL_CNT decimal(2,0),
                O_ALL_LOCAL decimal(1,0),
                O_ENTRY_D timestamp,
                primary key (O_W_ID, O_D_ID, O_ID)
            )
        '''

        create_table_item_query = '''
            CREATE TABLE IF NOT EXISTS Item (
                I_ID int,
                I_NAME varchar(24),
                I_PRICE decimal(5,2),
                I_IM_ID int,
                I_DATA varchar(50),
                primary key (I_ID)
            )
        '''

        create_table_order_line_query = '''
            CREATE TABLE IF NOT EXISTS OrderLine (
                OL_W_ID int,
                OL_D_ID int,
                OL_O_ID int,
                OL_NUMBER int,
                OL_I_ID int,
                OL_DELIVERY_D timestamp,
                OL_AMOUNT decimal(7,2),
                OL_SUPPLY_W_ID int,
                OL_QUANTITY decimal(2,0),
                OL_DIST_INFO char(24),
                primary key (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
            )
        '''

        create_table_stock_query = '''
            CREATE TABLE IF NOT EXISTS Stock (
                S_W_ID int,
                S_I_ID int,
                S_QUANTITY decimal(4,0),
                S_YTD decimal(8,2),
                S_ORDER_CNT int,
                S_REMOTE_CNT int,
                S_DIST_01 char(24),
                S_DIST_02 char(24),
                S_DIST_03 char(24),
                S_DIST_04 char(24),
                S_DIST_05 char(24),
                S_DIST_06 char(24),
                S_DIST_07 char(24),
                S_DIST_08 char(24),
                S_DIST_09 char(24),
                S_DIST_10 char(24),
                S_DATA varchar(50),
                primary key (S_W_ID, S_I_ID)
            )
        '''

        # Execute the queries
        cur.execute(create_table_warehouse_query)
        cur.execute(create_table_district_query)
        cur.execute(create_table_customer_query)
        cur.execute(create_table_orders_query)
        cur.execute(create_table_item_query)
        cur.execute(create_table_order_line_query)
        cur.execute(create_table_stock_query)
        '''
        # Add Foreign Key Constraints
        foreign_key_queries = [
                "ALTER TABLE District ADD CONSTRAINT fk_warehouse_district FOREIGN KEY (D_W_ID) REFERENCES Warehouse (W_ID);",
                "ALTER TABLE Customer ADD CONSTRAINT fk_customer_district FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES District (D_W_ID,D_ID);",
                "ALTER TABLE Orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES Customer (C_W_ID, C_D_ID, C_ID);",
                "ALTER TABLE OrderLine ADD CONSTRAINT fk_orderline_order FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES Orders (O_W_ID, O_D_ID, O_ID);",
                "ALTER TABLE OrderLine ADD CONSTRAINT fk_orderline_item FOREIGN KEY (OL_I_ID) REFERENCES Item (I_ID);",
                "ALTER TABLE Stock ADD CONSTRAINT fk_stock_item FOREIGN KEY (S_I_ID) REFERENCES Item (I_ID);",
                "ALTER TABLE Stock ADD CONSTRAINT fk_stock_warehouse FOREIGN KEY (S_W_ID) REFERENCES Warehouse (W_ID);"
        ]

        for query in foreign_key_queries:
                cur.execute(query)

       '''

        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_tables(sys.argv[1])
