import pandas as pd
import psycopg2
from datetime import datetime

# Function to load and clean multiple CSV files and process all unique tickers
def load_and_merge_csv():
    # Load datasets
    A = pd.read_csv('ATR.csv')
    B = pd.read_csv('D_EW_B.csv')
    C = pd.read_csv('D_EW_S.csv')
    D = pd.read_csv('HHLL.csv')
    E = pd.read_csv('LR_Explore.csv')
    F = pd.read_csv('Pattern_Revv.csv')
    G = pd.read_csv('SCTR_Trial.csv')
    H = pd.read_csv('ZigZag.csv')

    # Process and clean datasets
    for df in [A, B, C, D, E, F, G, H]:
        df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')

    # Rename columns
    A.rename(columns={'Buy': 'Buy_ATR'}, inplace=True)
    A['Buy_ATR'] = A['Buy_ATR'].replace(1, 'ATR')

    B.rename(columns={'Mega Buy by Larger Wave ': 'Buy_EW'}, inplace=True)
    B['Buy_EW'] = 'EW'

    E.rename(columns={'myBuy ': 'Buy_LR'}, inplace=True)
    E['Buy_LR'] = E['Buy_LR'].replace(1, 'LR')

    F.rename(columns={'BullBreak': 'Buy_BB'}, inplace=True)
    F['Buy_BB'] = F['Buy_BB'].replace('bullish Breakout', 'BB')

    H.rename(columns={'Buy': 'Buy_ZZ'}, inplace=True)
    H['Buy_ZZ'] = H['Buy_ZZ'].replace(1, 'ZZ')

    # Map patterns
    pattern_mapping = {
        'Up Channel': '_UC',
        'Wedge': '_W',
        'Down Channel': '_DC',
        'Broadening Wedge': '_BW',
        'Ascending Triangle': '_AT',
        'Decending Triangle': '_DT'
    }
    F['Pattern'] = F['Pattern'].replace(pattern_mapping)

    # Merge tables and create final table
    tables = [A, B, C, D, E, F, G, H]
    final_table = pd.DataFrame()

    for table in tables:
        if 'Ticker' in table.columns:
            temp = pd.melt(table, id_vars=['Date/Time', 'Ticker'], var_name='Field', value_name='Value')
        else:
            temp = pd.melt(table, id_vars=['Date/Time'], var_name='Field', value_name='Value')
        final_table = pd.concat([final_table, temp], ignore_index=True)

    final_table = final_table.pivot_table(index=['Date/Time', 'Ticker'], columns='Field', values='Value', aggfunc='first').reset_index()

    # Fill NaNs and concatenate columns
    final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']] = final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']].fillna('')

    def concatenate_non_nan(row):
        values = [row['Buy_ATR'], row['Buy_EW'], row['Buy_BB'], row['Buy_LR'], row['Pattern']]
        non_nan_values = [str(v) for v in values if pd.notna(v)]
        return ''.join(non_nan_values)

    final_table['Buy Strat'] = final_table.apply(concatenate_non_nan, axis=1)

    # Exclude unnecessary patterns and rows with NaN Buy Strat
    exclude_values = ['_UC', '_W', '_DC', '_BW', '_AT', '_DT']
    final_table = final_table[~final_table['Buy Strat'].isin(exclude_values)]
    final_table = final_table[final_table['Buy Strat'].notna() & (final_table['Buy Strat'] != '')]

    # Filter to include only the rows for the most recent date
    max_date = final_table['Date/Time'].max()
    final_table = final_table[final_table['Date/Time'] == max_date]

    # Rename and keep only the necessary columns
    final_table = final_table[['Date/Time', 'Ticker', 'Buy Strat', 'll']]
    final_table = final_table.rename(columns={'Date/Time': 'Buy Date'})
    final_table.to_csv('final_table.csv', index=False)

    return final_table

# Fetch CMP from PostgreSQL yfinance table and ew_conv table, then merge with the tickers
def fetch_and_merge_from_postgresql(final_table):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Fetch CMP and CMP_Date from yfinance_data
    cmp_query = 'SELECT "Ticker", "CMP", "CMP_Date" FROM yfinance_data'
    cmp_df = pd.read_sql(cmp_query, conn)

    # Merge CMP data with final_table
    merged_table = pd.merge(final_table, cmp_df, on='Ticker', how='left')

    # Fetch ew_conv_table data with Target field
    ew_conv_query = '''
    SELECT "Ticker", "Target", "Call"
    FROM ew_conv_table
    '''
    ew_conv_df = pd.read_sql(ew_conv_query, conn)

    # Merge ew_conv data with the merged_table based on Ticker
    merged_table = pd.merge(merged_table, ew_conv_df, on='Ticker', how='left')

    # Fetch Close from merged_tickers
    merged_tickers_query = 'SELECT "Ticker", "Date/Time", "Close" FROM merged_tickers'
    merged_tickers_df = pd.read_sql(merged_tickers_query, conn)
    merged_tickers_df.rename(columns={'Date/Time':'Buy Date'}, inplace=True)
    merged_tickers_df['Buy Date'] = pd.to_datetime(merged_tickers_df['Buy Date'], errors='coerce')

    # Merge merged_tickers for Close (Buy Price) with merged_table based on ticker and date/time
    merged_table = pd.merge(merged_table, merged_tickers_df, on = ['Ticker', 'Buy Date'], how = 'left') # left to verify, but inner SHOULD give same results

    # Assign 'India' to the Account field after merging the tables
    merged_table['Account'] = 'KRBPT'

    # Ensure Date fields are in the correct date format
    merged_table['Buy Date'] = pd.to_datetime(merged_table['Buy Date'], errors='coerce').dt.date

    # Handle missing values in the date columns
    placeholder_date = datetime(2000, 1, 1).date()
    merged_table['Buy Date'] = merged_table['Buy Date'].fillna(placeholder_date)

    conn.close()

    # Calculate quantity safely and avoiding division by zero to avoid potential error division
    def calculate_quantity(row):
        if pd.notnull(row['Close']) and pd.notnull(row['ll']) and row['Close'] > row['ll']:
            result = 1000 / (row['Close'] - row['ll'])
            return round(result, 2) 
        return None  
    # Apply the function to calculate Quantity
    merged_table['Quantity'] = merged_table.apply(calculate_quantity, axis=1)

    merged_table['CMP'] = merged_table['CMP'].round(2)
    merged_table['ll'] = merged_table['ll'].round(2)
    merged_table['Close'] = merged_table['Close'].round(2)
    merged_table['Target'] = merged_table['Target'].round(2)

    # Rename the fields to match the new requirement
    merged_table = merged_table.rename(columns={'Buy Date':'Trade Day', 'Buy Strat': 'System', 'Call': 'Trade Direction',
                                                'Ticker':'Stock', 'Close':'Entry Price', 'll':'ISL/TSL','Quantity':'Buy Qty',
                                                'Hit Type':'Exit Strategy'})
    merged_table['Commission'] = pd.Series(dtype='int')
    merged_table['Trade Category'] = pd.Series(dtype='object')
    merged_table['Sell Qty'] = merged_table['Buy Qty']

    # Apply logic for Call and Status adjustments
    def adjust_call_status(row):
    # Check if CMP < ISL to hit ISL
        if pd.notnull(row['CMP']) and pd.notnull(row['ISL/TSL']) and row['CMP'] < row['ISL/TSL']:
            return 'Short', 'ISL Hit', round(row['ISL/TSL'], 2)
        # Check if CMP <= Target to hit Target from ew_conv_table
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] <= row['Target']:
            return 'Short', 'Target Hit', round(row['Target'], 2)
        # Set the default to 'long' to prevent unwanted entries sent to archived_trades table
        trade_direction = row['Trade Direction'] if pd.notnull(row['Trade Direction']) else 'Long'
        return 'Long', None, None

    # Copying the df to avoid warning from pandas
    merged_table = merged_table.copy()

    # Apply the function and assign values using .loc to avoid pandas warning
    merged_table[['Trade Direction', 'Exit Strategy', 'Sell Price']] = merged_table.apply(
        lambda row: pd.Series(adjust_call_status(row)), axis=1
    ).loc[:, [0, 1, 2]]

    merged_table['Sell Price'] = merged_table['Sell Price'].round(2)

    

    return merged_table

# Save final data to CSV and upload to PostgreSQL
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


def update_table_with_adjusted_status(table_name, conn):
    cur = conn.cursor()

    # Fetch all records from the specified table
    fetch_query = f'''
    SELECT 
        "Trade Day", "System", "Trade Direction", "Stock", "Entry Price", 
        "ISL/TSL", "Buy Qty", "Sell Qty", "Sell Price", "Exit Strategy",
        "Commission", "Trade Category", "Account", "CMP", "CMP_Date", "Target"
    FROM {table_name};
    '''
    records_df = pd.read_sql(fetch_query, conn)

    # Define the adjust_call_status logic with conditional logic for table_name
    def adjust_call_status(row):
        # Logic for tj
        if pd.notnull(row['CMP']) and pd.notnull(row['ISL/TSL']) and row['CMP'] < row['ISL/TSL']:
            return 'Short', 'ISL Hit', round(row['ISL/TSL'], 2)
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] <= row['Target']:
            return 'Short', 'Target Hit', round(row['Target'], 2)
        # If none of the conditions are met, return original trade direction and exit strat and sell price as none
        return row['Trade Direction'], None, None

    # Apply the adjust_call_status logic
    records_df[['Trade Direction', 'Exit Strategy', 'Sell Price']] = records_df.apply(
        lambda row: adjust_call_status(row), axis=1, result_type='expand'
    )
    records_df['Sell Price'] = records_df['Sell Price'].round(2)

    # Update the table with adjusted values
    for _, row in records_df.iterrows():
        update_query = f'''
        UPDATE {table_name}
        SET 
            "Trade Direction" = %s,
            "Exit Strategy" = %s,
            "Sell Price" = %s
        WHERE "Trade Day" = %s AND "Stock" = %s AND "Account" = %s;
        '''
        cur.execute(update_query, (
            row["Trade Direction"], row["Exit Strategy"], row["Sell Price"],
            row["Trade Day"], row["Stock"], row["Account"]
        ))
    conn.commit()
    cur.close()

def update_tj():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    update_table_with_adjusted_status('tj', conn)
    conn.close()

# Function to transfer Short stocks from tj to archived_trades
def transfer_closed():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Create the new table for archived records if it doesn't exist
    create_archive_table_query = '''
    CREATE TABLE IF NOT EXISTS archived_trades (
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
    cur.execute(create_archive_table_query)
    conn.commit()

    # Retrieve Close for each Ticker before deletion
    buy_price_query = '''
    SELECT "Stock", "Entry Price" 
    FROM tj
    WHERE "Entry Price" IS NOT NULL
    '''
    buy_price_df = pd.read_sql(buy_price_query, conn)

    # Convert buy_price_df to dictionary for easy lookup
    close_dict = buy_price_df.set_index('Stock')['Entry Price'].to_dict()

    # Fetch closed records to transfer
    closed_records_query = '''
    SELECT 
        "Trade Day", "System", "Trade Direction", "Stock", "Entry Price", 
        "ISL/TSL", "Buy Qty", "Sell Qty", "Sell Price", "Exit Strategy", "Commission", 
        "Trade Category", "Account", "CMP", "CMP_Date", "Target"
    FROM tj
    WHERE "Trade Direction" = 'Short'
    '''
    closed_records_df = pd.read_sql(closed_records_query, conn)

    # Fill missing Close values in closed_records_df
    closed_records_df['Entry Price'] = closed_records_df.apply(
        lambda row: close_dict.get(row['Stock'], row['Entry Price']) 
        if pd.isna(row['Entry Price']) else row['Entry Price'], axis=1
    )

    closed_records_df.to_csv('Records_Closed_Today.csv', index=False)

    # Insert updated closed records into archived_trades
    for _, row in closed_records_df.iterrows():
        insert_into_archived_query = '''
        INSERT INTO archived_trades (
            "Trade Day", "System", "Trade Direction", "Stock", "Entry Price", "ISL/TSL", 
            "Buy Qty", "Sell Qty", "Sell Price", "Exit Strategy", "Commission",
            "Trade Category", "Account", "CMP", "CMP_Date", "Target")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Stock", "Trade Day", "Account")
        DO NOTHING;
        '''
        cur.execute(insert_into_archived_query, tuple(row))
    conn.commit()

    # Delete the Closed records after transferring to archived_trades table
    delete_query = '''
    DELETE FROM tj
    WHERE "Trade Direction" = 'Short';
    '''
    cur.execute(delete_query)
    conn.commit()

    cur.close()
    conn.close()


# Main function to run the entire pipeline
def main():
    final_table = load_and_merge_csv()
    merged_table = fetch_and_merge_from_postgresql(final_table)
    save_and_upload(merged_table)
    update_tj()
    transfer_closed()

if __name__ == "__main__":
    main()