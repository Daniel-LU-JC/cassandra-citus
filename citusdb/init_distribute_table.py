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

        # Create the distributed table across all nodes using W_ID as the distribution column
        create_distributed_table_warehouse_query = "SELECT create_distributed_table('Warehouse', 'w_id')"
        cur.execute(create_distributed_table_warehouse_query)

        # Create the metadata table
        #create_reference_table_warehouse_query = "SELECT create_reference_table('Warehouse', 'w_id')"
        #cur.execute(create_reference_table_warehouse_query)


        create_distributed_district_query = "SELECT create_distributed_table('District', 'd_w_id')"
        cur.execute(create_distributed_district_query)

        #create_reference_district_query = "SELECT create_reference_table('District', 'd_w_id')"
        #cur.execute(create_reference_district_query)


        create_distributed_customer_query = "SELECT create_distributed_table('Customer', 'c_w_id')"
        cur.execute(create_distributed_customer_query)
        #create_reference_customer_query = "SELECT create_reference_table('Customer', 'c_w_id')"
        #cur.execute(create_reference_customer_query)

        create_distributed_orders_query = "SELECT create_distributed_table('Orders', 'o_w_id')"
        cur.execute(create_distributed_orders_query)
        #create_reference_orders_query = "SELECT create_reference_table('Orders', 'o_w_id')"
        #cur.execute(create_reference_orders_query)

        create_distributed_item_query = "SELECT create_distributed_table('Item', 'i_id')"
        cur.execute(create_distributed_item_query)
        #create_reference_item_query = "SELECT create_reference_table('Item', 'i_id')"
        #cur.execute(create_reference_item_query)

        create_distributed_orderline_query = "SELECT create_distributed_table('OrderLine', 'ol_w_id')"
        cur.execute(create_distributed_orderline_query)
        #create_reference_orderline_query = "SELECT create_reference_table('OrderLine', 'ol_w_id')"
        #cur.execute(create_reference_orderline_query)

        create_distributed_stock_query = "SELECT create_distributed_table('Stock', 's_w_id')"
        cur.execute(create_distributed_stock_query)
        #create_reference_stock_query = "SELECT create_reference_table('Stock', 's_w_id')"
        #cur.execute(create_reference_stock_query)
        # Add Foreign Key Constraints

        # not able to create foreign key constraint for these  ol_i_id and s_i_id as citus only allows one key for distribution columns and if we create a foreign key constraint, it requires that foreign key to be added in that distribution column
        foreign_key_queries = [
                "ALTER TABLE District ADD CONSTRAINT fk_warehouse_district FOREIGN KEY (D_W_ID) REFERENCES Warehouse (W_ID);",
                "ALTER TABLE Customer ADD CONSTRAINT fk_customer_district FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES District (D_W_ID,D_ID);",
                "ALTER TABLE Orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES Customer (C_W_ID, C_D_ID, C_ID);",
                "ALTER TABLE OrderLine ADD CONSTRAINT fk_orderline_order FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES Orders (O_W_ID, O_D_ID, O_ID);",
                #"ALTER TABLE OrderLine ADD CONSTRAINT fk_orderline_item FOREIGN KEY (OL_I_ID) REFERENCES Item (I_ID);",
                #"ALTER TABLE Stock ADD CONSTRAINT fk_stock_item FOREIGN KEY (S_I_ID) REFERENCES Item (I_ID);",
                "ALTER TABLE Stock ADD CONSTRAINT fk_stock_warehouse FOREIGN KEY (S_W_ID) REFERENCES Warehouse (W_ID);"
        ]



        for query in foreign_key_queries:
                cur.execute(query)

        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_tables(sys.argv[1])