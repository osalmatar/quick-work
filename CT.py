import pandas as pd
import psycopg2
from datetime import datetime

ct = pd.read_csv("CT_Upload_Temp.csv")

placeholder_date = datetime(2000, 1, 1).date()
ct['Sell_Day'] = ct['Sell_Day'].fillna(placeholder_date)

# Connect to PostgreSQL using psycopg2
def read_and_upload_ct():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Create table with case-sensitive column names
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS ct (
        "Buy_DATE" TIMESTAMP,
        "Sell_Day" TIMESTAMP,
        "Call" VARCHAR(20),
        "Entry_Criteria" VARCHAR(20),
        "Stock" VARCHAR(20),
        "Buy_Qty" FLOAT,
        "Buy_Price" FLOAT,
        "Sell_Qty" FLOAT,
        "Sell_Price" FLOAT,
        "Stop_Loss" FLOAT,
        "Target_Price" FLOAT,
        "Duration" FLOAT,
        PRIMARY KEY ("Buy_DATE", "Stock")
    );
    '''
    cur.execute(create_table_query)

    # Insert or update data into the PostgreSQL table
    insert_query = '''
    INSERT INTO ct ("Buy_DATE", "Sell_Day", "Call", "Entry_Criteria", "Stock", "Buy_Qty", "Buy_Price", "Sell_Qty", 
    "Sell_Price", "Stop_Loss", "Target_Price", "Duration")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("Buy_DATE", "Stock")
    DO NOTHING
    '''

    # Insert or update each row
    for _, row in ct.iterrows():
        cur.execute(insert_query, tuple(row))

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

def main():
    read_and_upload_ct()

if __name__== "__main__":
    main()