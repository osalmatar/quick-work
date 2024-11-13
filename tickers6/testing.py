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

# Fetch CMP from PostgreSQL yfinance table and ew_conv table (conditional selection of buy price / target), then merge with the tickers
def fetch_and_merge_cmp_and_ew_conv(final_table):
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

    # Fetch ew_conv_table data with conditional selection for Buy Price and Target
    ew_conv_query = '''
    SELECT 
        "Ticker", 
        "Last Alert Date",
        CASE 
            WHEN "Buy Price" > 1 THEN "Buy Price" 
            ELSE NULL 
        END AS "Buy Price",
        CASE 
            WHEN "Buy Price" <= 1 THEN "Target" 
            ELSE NULL 
        END AS "Target",
        "Call", 
        "Status"
    FROM ew_conv_table
    '''
    ew_conv_df = pd.read_sql(ew_conv_query, conn)

    # Merge ew_conv data with the merged_table based on Ticker
    merged_table = pd.merge(merged_table, ew_conv_df, on='Ticker', how='left')

    # Assign 'India' to the Broker field after merging the tables
    merged_table['Broker'] = 'India'

    # Ensure Date fields are in the correct date format
    merged_table['CMP_Date'] = pd.to_datetime(merged_table['CMP_Date'], errors='coerce').dt.date
    merged_table['Last Alert Date'] = pd.to_datetime(merged_table['Last Alert Date'], errors='coerce').dt.date
    merged_table['Buy Date'] = pd.to_datetime(merged_table['Buy Date'], errors='coerce').dt.date

    # Handle missing values in the date columns
    placeholder_date = datetime(2000, 1, 1).date()
    merged_table['CMP_Date'] = merged_table['CMP_Date'].fillna(placeholder_date)
    merged_table['Last Alert Date'] = merged_table['Last Alert Date'].fillna(placeholder_date)
    merged_table['Buy Date'] = merged_table['Buy Date'].fillna(placeholder_date)

    conn.close()

    # Calculate quantity and round floats to 2 decimal places
    merged_table['Quantity'] = (1000 / (merged_table['Buy Price'] - merged_table['ll'])).round(2)
    merged_table['CMP'] = merged_table['CMP'].round(2)
    merged_table['ll'] = merged_table['ll'].round(2)
    merged_table['Buy Price'] = merged_table['Buy Price'].round(2)
    merged_table['Target'] = merged_table['Target'].round(2)

    # Apply logic for Call and Status adjustments
    def adjust_call_status(row):
        # Check if CMP < ll to hit ISL
        if pd.notnull(row['CMP']) and pd.notnull(row['ll']) and row['CMP'] < row['ll']:
            return 'Short', 'Closed', 'ISL Hit', round(row['ll'], 2)
        # Check if CMP <= Target to hit Target from ew_conv_table
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] <= row['Target']:
            return 'Short', 'Closed', 'Target Hit', round(row['Target'], 2)
        # If conditions are not met, return original Call, Status, Hit Type, and Sell Price as None
        return row['Call'], row['Status'], None, None

    merged_table[['Call', 'Status', 'Hit Type', 'Sell Price']] = merged_table.apply(
        lambda row: adjust_call_status(row), axis=1, result_type='expand'
    )

    merged_table['Sell Price'] = merged_table['Sell Price'].round(2)

    return merged_table


# Save final data to CSV and upload to PostgreSQL
def save_and_upload(merged_table):
    # Ensure correct data types
    merged_table['ll'] = pd.to_numeric(merged_table['ll'], errors='coerce')
    merged_table['Buy Price'] = pd.to_numeric(merged_table['Buy Price'], errors='coerce')
    merged_table['Target'] = pd.to_numeric(merged_table['Target'], errors='coerce')
    merged_table['CMP'] = pd.to_numeric(merged_table['CMP'], errors='coerce')
    merged_table['Quantity'] = pd.to_numeric(merged_table['Quantity'], errors='coerce')
    merged_table['Sell Price'] = pd.to_numeric(merged_table['Sell Price'], errors='coerce')

    merged_table['Buy Date'] = pd.to_datetime(merged_table['Buy Date']).dt.date
    merged_table['Last Alert Date'] = pd.to_datetime(merged_table['Last Alert Date']).dt.date
    merged_table['CMP_Date'] = pd.to_datetime(merged_table['CMP_Date']).dt.date

    # Ensure column order matches the SQL query
    merged_table = merged_table[['Buy Date', 'Ticker', 'Buy Strat', 'Last Alert Date', 'll', 'Buy Price', 
                               'Target', 'Call', 'Status', 'CMP', 'CMP_Date', 'Quantity', 'Hit Type', 
                               'Sell Price', 'Broker']]

    # Save the final table to CSV
    merged_table.to_csv('final_trade_journal.csv', index=False)

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
    CREATE TABLE IF NOT EXISTS trade_journal (
        "Buy Date" DATE,
        "Ticker" TEXT,
        "Buy Strat" TEXT,
        "Last Alert Date" DATE,
        "ll" FLOAT,
        "Buy Price" FLOAT,
        "Target" FLOAT,
        "Call" TEXT,
        "Status" TEXT,
        "CMP" FLOAT,
        "CMP_Date" DATE,
        "Quantity" FLOAT,
        "Hit Type" TEXT,
        "Sell Price" FLOAT, 
        "Broker" TEXT,
        PRIMARY KEY ("Ticker", "Buy Date")
    );
    '''
    cur.execute(create_table_query)
    conn.commit()

    # Insert or update data into PostgreSQL
    for _, row in merged_table.iterrows():
        insert_query = '''
        INSERT INTO trade_journal ("Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Buy Price", "Target", 
        "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type", "Sell Price", "Broker") 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO UPDATE SET 
            "Buy Strat" = EXCLUDED."Buy Strat",
            "Last Alert Date" = EXCLUDED."Last Alert Date",
            "ll" = EXCLUDED."ll",
            "Buy Price" = EXCLUDED."Buy Price",
            "Target" = EXCLUDED."Target",
            "Call" = EXCLUDED."Call",
            "Status" = EXCLUDED."Status",
            "CMP" = EXCLUDED."CMP",
            "CMP_Date" = EXCLUDED."CMP_Date",
            "Quantity" = EXCLUDED."Quantity",
            "Hit Type" = EXCLUDED."Hit Type",
            "Sell Price" = EXCLUDED."Sell Price",
            "Broker" = EXCLUDED."Broker";
        '''
        cur.execute(insert_query, tuple(row))
    
    conn.commit()
    cur.close()
    conn.close()

# Function to transfer to archived_table
def transfer_records():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Create archived_trades if it doesn't exist
    create_archive_table_query = '''
    CREATE TABLE IF NOT EXISTS archived_trades (
        "Buy Date" DATE,
        "Ticker" TEXT,
        "Buy Strat" TEXT,
        "Last Alert Date" DATE,
        "ll" FLOAT,
        "Buy Price" FLOAT,
        "Target" FLOAT,
        "Call" TEXT,
        "Status" TEXT,
        "CMP" FLOAT,
        "CMP_Date" DATE,
        "Quantity" FLOAT,
        "Hit Type" TEXT,
        "Sell Price" FLOAT,
        "Broker" TEXT,
        PRIMARY KEY ("Ticker", "Buy Date")
    );
    '''
    cur.execute(create_archive_table_query)
    conn.commit()

    # Fetch closed records from trade_journal and join with ew_conv to get Target
    closed_records_query = '''
    SELECT 
        tj."Buy Date", tj."Ticker", tj."Buy Strat", tj."Last Alert Date", tj."ll", 
        tj."Buy Price", ew."Target", tj."Call", tj."Status", tj."CMP", tj."CMP_Date", 
        tj."Quantity", tj."Hit Type", tj."Sell Price", tj."Broker"
    FROM trade_journal tj
    LEFT JOIN ew_conv_table ew ON tj."Ticker" = ew."Ticker"
    WHERE tj."Status" = 'Closed'
    '''
    closed_records_df = pd.read_sql(closed_records_query, conn)
    
    # Debugging output: Check the first few rows to verify the join results
    print("Closed Records After Join:")
    print(closed_records_df[['Ticker', 'Buy Price', 'Target']].head(10))

    # Verify `Buy Price` and `Target` in closed_records_df before transferring to archived_trades
    if closed_records_df['Buy Price'].isnull().any():
        print("Warning: Missing Buy Price values in closed_records_df.")
    if closed_records_df['Target'].isnull().any():
        print("Warning: Missing Target values in closed_records_df.")

    # Insert updated closed records into archived_trades with ON CONFLICT DO UPDATE
    for _, row in closed_records_df.iterrows():
        insert_into_archived_query = '''
        INSERT INTO archived_trades (
            "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Buy Price", 
            "Target", "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type", 
            "Sell Price", "Broker")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO UPDATE SET 
            "Buy Price" = COALESCE(archived_trades."Buy Price", EXCLUDED."Buy Price"),
            "Target" = COALESCE(archived_trades."Target", EXCLUDED."Target");
        '''
        cur.execute(insert_into_archived_query, tuple(row))
    conn.commit()

    # Step 4: Delete the Closed records from trade_journal after transferring to archived_trades
    #delete_query = '''
    #DELETE FROM trade_journal
    #WHERE "Status" = 'Closed';
    #'''
    #cur.execute(delete_query)
    #conn.commit()

    #cur.close()
    #conn.close()




# Main function to run the entire pipeline
def main():
    final_table = load_and_merge_csv()
    merged_table = fetch_and_merge_cmp_and_ew_conv(final_table)
    save_and_upload(merged_table)
    transfer_records()

if __name__ == "__main__":
    main()
