import pandas as pd
import psycopg2

merged_table = pd.read_csv('tj_140125.csv')

def save_and_upload(merged_table):
    # Ensure correct data types
    merged_table['ISL/TSL'] = pd.to_numeric(merged_table['ISL/TSL'], errors='coerce')
    merged_table['Entry Price'] = pd.to_numeric(merged_table['Entry Price'], errors='coerce')
    merged_table['Target'] = pd.to_numeric(merged_table['Target'], errors='coerce')
    merged_table['CMP'] = pd.to_numeric(merged_table['CMP'], errors='coerce')
    merged_table['Buy Qty'] = pd.to_numeric(merged_table['Buy Qty'], errors='coerce')
    merged_table['Sell Qty'] = pd.to_numeric(merged_table['Sell Qty'], errors='coerce')
    merged_table['Sell Price'] = pd.to_numeric(merged_table['Sell Price'], errors='coerce')
    merged_table['Trade Day'] = pd.to_datetime(merged_table['Trade Day']).dt.date
    #merged_table['Last Alert Date'] = pd.to_datetime(merged_table['Last Alert Date']).dt.date
    merged_table['CMP_Date'] = pd.to_datetime(merged_table['CMP_Date']).dt.date
    merged_table['Commission'] = pd.to_numeric(merged_table['Commission'], errors='coerce')
    merged_table['Buy Qty'] = pd.to_numeric(merged_table['Buy Qty'], errors='coerce')
    
    # Convert NaT to Null to be received well by postgres
    merged_table['Trade Day'] = merged_table['Trade Day'].apply(lambda x: None if pd.isna(x) else x)
    merged_table['CMP_Date'] = merged_table['CMP_Date'].apply(lambda x: None if pd.isna(x) else x)


    # Ensure column order matches the SQL query
    merged_table = merged_table[['Trade Day', 'System', 'Trade Direction', 'Stock', 'Entry Price', 'ISL/TSL', 
                               'Buy Qty', 'Sell Qty', 'Sell Price', 'Exit Strategy', 'Commission', 'Trade Category',
                                 'Account', 'CMP', 'CMP_Date', 'Target']]

    # Save the final table to CSV
    merged_table.to_csv('tj.csv', index=False)

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Create Trade Journal table if it doesn't exist
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS tj (
        "Trade Day" DATE,
        "System" TEXT,
        "Trade Direction" TEXT,
        "Stock" TEXT,
        "Entry Price" FLOAT,
        "ISL/TSL" FLOAT,
        "Buy Qty" FLOAT,
        "Sell Qty" FLOAT,
        "Sell Price" FLOAT,
        "Exit Strategy" TEXT,
        "Commission" FLOAT,
        "Trade Category" TEXT,
        "Account" TEXT,
        "CMP" FLOAT, 
        "CMP_Date" DATE,
        "Target" FLOAT,
        PRIMARY KEY ("Stock", "Trade Day", "Account")
    );
    '''
    cur.execute(create_table_query)
    conn.commit()

    # Insert or update data into PostgreSQL
    for _, row in merged_table.iterrows():
        # Insert or update based on primary keys "Stock", "Buy Date"
        insert_query_composite = '''
        INSERT INTO tj ("Trade Day", "System", "Trade Direction", "Stock", "Entry Price", "ISL/TSL", "Buy Qty", 
        "Sell Qty", "Sell Price", "Exit Strategy", "Commission", "Trade Category", "Account", "CMP", "CMP_Date", "Target") 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Stock", "Trade Day", "Account")
        DO UPDATE SET 
            "System" = EXCLUDED."System",
            "Trade Direction" = EXCLUDED."Trade Direction",
            "Entry Price" = EXCLUDED."Entry Price",
            "ISL/TSL" = EXCLUDED."ISL/TSL",
            "Buy Qty" = EXCLUDED."Buy Qty",
            "Sell Qty" = EXCLUDED."Sell Qty",
            "Sell Price" = EXCLUDED."Sell Price",
            "Exit Strategy" = EXCLUDED."Exit Strategy",
            "Commission" = EXCLUDED."Commission",
            "Trade Category" = EXCLUDED."Trade Category",
            "CMP" = EXCLUDED."CMP",
            "CMP_Date" = EXCLUDED."CMP_Date",
            "Target" = EXCLUDED."Target";
        '''
        cur.execute(insert_query_composite, (
            row["Trade Day"], row["System"], row["Trade Direction"], row["Stock"], row["Entry Price"], row["ISL/TSL"], 
            row["Buy Qty"], row["Sell Qty"], row["Sell Price"], row["Exit Strategy"], 
            row["Commission"], row["Trade Category"], row["Account"], row["CMP"], row["CMP_Date"], row["Target"]
    ))

    # Insert or update for "CMP" and "CMP_Date" based on "Ticker" only
    update_query_cmp = '''
    UPDATE tj 
    SET
        "CMP" = y."CMP"
    FROM yfinance_data y
    WHERE tj."Stock" = y."Ticker"
    '''
    cur.execute(update_query_cmp)

    # Update Target from ew_conv_table
    update_query_target = '''
    UPDATE tj
    SET
        "Target" = ew."Target"
    FROM ew_conv_table ew
    WHERE tj."Stock" = ew."Ticker"
    '''
    cur.execute(update_query_target)
    
    # Apply logic for Call and Status adjustments
    def adjust_call_status(row):
        # Check if CMP < ll to hit ISL
        if pd.notnull(row['CMP']) and pd.notnull(row['ISL/TSL']) and row['CMP'] < row['ISL/TSL']:
            return 'Short', 'ISL Hit', round(row['ISL/TSL'], 2)
        # Check if CMP <= Target to hit Target from ew_conv_table
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] <= row['Target']:
            return 'Short', 'Target Hit', round(row['Target'], 2)
        # If conditions are not met, return original Call, Hit Type, and Sell Price as None
        return row['Trade Direction'], None, None
    
    merged_table = merged_table.copy()

    merged_table[['Trade Direction', 'Exit Strategy', 'Sell Price']] = merged_table.apply(
        lambda row: pd.Series(adjust_call_status(row)), axis=1).loc[:, [0,1,2]]
    

    conn.commit()
    cur.close()
    conn.close()

def main():
    save_and_upload(merged_table)

if __name__ == "__main__":
    main()