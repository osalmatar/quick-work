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
    SELECT "Ticker", "Last Alert Date", "Target", "Call", "Status" 
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

    # Calculate quantity safely and avoiding division by zero
    # Define a safe calculation function
    def calculate_quantity(row):
        if pd.notnull(row['Close']) and pd.notnull(row['ll']) and row['Close'] > row['ll']:
            result = 1000 / (row['Close'] - row['ll'])
            return round(result, 2)  # Use round() instead of .round()
        return None  # Return None if the conditions are not met
    # Apply the safe function to calculate Quantity
    merged_table['Quantity'] = merged_table.apply(calculate_quantity, axis=1)

    merged_table['CMP'] = merged_table['CMP'].round(2)
    merged_table['ll'] = merged_table['ll'].round(2)
    merged_table['Close'] = merged_table['Close'].round(2)
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
    merged_table['Close'] = pd.to_numeric(merged_table['Close'], errors='coerce')
    merged_table['Target'] = pd.to_numeric(merged_table['Target'], errors='coerce')
    merged_table['CMP'] = pd.to_numeric(merged_table['CMP'], errors='coerce')
    merged_table['Quantity'] = pd.to_numeric(merged_table['Quantity'], errors='coerce')
    merged_table['Sell Price'] = pd.to_numeric(merged_table['Sell Price'], errors='coerce')
    merged_table['Buy Date'] = pd.to_datetime(merged_table['Buy Date']).dt.date
    merged_table['Last Alert Date'] = pd.to_datetime(merged_table['Last Alert Date']).dt.date
    merged_table['CMP_Date'] = pd.to_datetime(merged_table['CMP_Date']).dt.date

    # Ensure column order matches the SQL query
    merged_table = merged_table[['Buy Date', 'Ticker', 'Buy Strat', 'Last Alert Date', 'll', 'Close', 
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
        "Close" FLOAT,
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
        # Insert or update for composite key ("Buy Date", "Ticker")
        insert_query_composite = '''
        INSERT INTO trade_journal ("Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close", "Target", 
        "Call", "Status", "Quantity", "Hit Type", "Sell Price", "Broker") 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO UPDATE SET 
            "Buy Strat" = EXCLUDED."Buy Strat",
            "Last Alert Date" = EXCLUDED."Last Alert Date",
            "ll" = EXCLUDED."ll",
            "Close" = EXCLUDED."Close",
            "Target" = EXCLUDED."Target",
            "Call" = EXCLUDED."Call",
            "Status" = EXCLUDED."Status",
            "Quantity" = EXCLUDED."Quantity",
            "Hit Type" = EXCLUDED."Hit Type",
            "Sell Price" = EXCLUDED."Sell Price",
            "Broker" = EXCLUDED."Broker";
        '''
        cur.execute(insert_query_composite, (
            row["Buy Date"], row["Ticker"], row["Buy Strat"], row["Last Alert Date"], row["ll"], row["Close"], 
            row["Target"], row["Call"], row["Status"], row["Quantity"], 
            row["Hit Type"], row["Sell Price"], row["Broker"]
    ))

    # Insert or update for "CMP" and "CMP_Date" based on "Ticker" only
    update_query_cmp = '''
    UPDATE trade_journal 
    SET
        "CMP" = y."CMP",
        "CMP_Date" = y."CMP_Date"
    FROM yfinance_data y
    WHERE trade_journal."Ticker" = y."Ticker"
    '''
    cur.execute(update_query_cmp)
    
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

    conn.commit()
    cur.close()
    conn.close()


# Function to transfer specific records to a new table
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
        "Buy Date" DATE,
        "Ticker" TEXT,
        "Buy Strat" TEXT,
        "Last Alert Date" DATE,
        "ll" FLOAT,
        "Close" FLOAT,
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

    # Step 1: Retrieve Close for each Ticker before deletion
    buy_price_query = '''
    SELECT "Ticker", "Close" 
    FROM trade_journal
    WHERE "Close" IS NOT NULL
    '''
    buy_price_df = pd.read_sql(buy_price_query, conn)

    # Convert buy_price_df to dictionary for easy lookup
    close_dict = buy_price_df.set_index('Ticker')['Close'].to_dict()

    # Step 2: Fetch closed records to transfer
    closed_records_query = '''
    SELECT 
        "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", 
        "Close", "Target", "Call", "Status", "CMP", "CMP_Date", 
        "Quantity", "Hit Type", "Sell Price", "Broker"
    FROM trade_journal
    WHERE "Status" = 'Closed'
    '''
    closed_records_df = pd.read_sql(closed_records_query, conn)

    # Step 3: Fill missing Close values in closed_records_df using buy_price_dict
    closed_records_df['Close'] = closed_records_df.apply(
        lambda row: close_dict.get(row['Ticker'], row['Close']) 
        if pd.isna(row['Close']) else row['Close'], axis=1
    )

    # Step 4: Insert updated closed records into archived_trades
    for _, row in closed_records_df.iterrows():
        insert_into_archived_query = '''
        INSERT INTO archived_trades (
            "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close", 
            "Target", "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type", 
            "Sell Price", "Broker")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO NOTHING;
        '''
        cur.execute(insert_into_archived_query, tuple(row))
    conn.commit()

    # Step 5: Delete the Closed records after transferring to archived_trades table
    delete_query = '''
    DELETE FROM trade_journal
    WHERE "Status" = 'Closed';
    '''
    cur.execute(delete_query)
    conn.commit()

    cur.close()
    conn.close()

# Function to transfer specific records where the bought and sold tickers are on the same day
def transfer_reversed():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Create the new table for reversed records if it doesn't exist
    create_reversed_table_query = '''
    CREATE TABLE IF NOT EXISTS reversed_table (
        "Buy Date" DATE,
        "Ticker" TEXT,
        "Buy Strat" TEXT,
        "Last Alert Date" DATE,
        "ll" FLOAT,
        "Close" FLOAT,
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
    cur.execute(create_reversed_table_query)
    conn.commit()

    # Fetch all records from trade_journal for processing
    fetch_records_query = '''
    SELECT 
        "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", 
        "Close", "Target", "Call", "Status", "CMP", "CMP_Date",
        "Quantity", "Hit Type", "Sell Price", "Broker"
    FROM trade_journal
    WHERE "Buy Date" = "CMP_Date" AND "Status" = 'Closed';
    '''
    records_df = pd.read_sql(fetch_records_query, conn)

    records_df['Status'] = 'Open'
    records_df['Call'] = 'Long'

    # Recalculate Risk Amount and Target for all records
    records_df['Risk Amount'] = records_df['Close'] - records_df['ll']
    records_df['Target'] = (2 * records_df['Risk Amount']) + records_df['Close']
    records_df['Target'] = records_df['Target'].round(2)

    # Insert updated records into reversed_table
    for _, row in records_df.iterrows():
        insert_into_reversed_query = '''
        INSERT INTO reversed_table (
            "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close", 
            "Target", "Call", "Status", "Quantity", "Hit Type", "CMP", "CMP_Date",
            "Sell Price", "Broker")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO NOTHING;
        '''
        cur.execute(insert_into_reversed_query, (
            row["Buy Date"], row["Ticker"], row["Buy Strat"], row["Last Alert Date"],
            row["ll"], row["Close"], row["Target"], row["Call"], row["Status"],
            row["Quantity"], row["Hit Type"], row['CMP'], row['CMP_Date'], row["Sell Price"], row["Broker"]
        ))
    conn.commit()

    # Function to adjust Call, Status, Hit Type, and Sell Price using the updated Target
    def adjust_call_status(row):
        # Check if CMP < ll to hit ISL
        if pd.notnull(row['CMP']) and pd.notnull(row['ll']) and row['CMP'] < row['ll']:
            return 'Short', 'Closed', 'ISL Hit', round(row['ll'], 2)
        # Check if CMP <= Target to hit the adjusted Target
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] >= row['Target']:
            return 'Short', 'Closed', 'Target Hit', round(row['Target'], 2)
        # If conditions are not met, return original Call, Status, Hit Type, and Sell Price as None
        return row['Call'], row['Status'], None, None

    # Apply the logic to update the DataFrame
    records_df[['Call', 'Status', 'Hit Type', 'Sell Price']] = records_df.apply(
        lambda row: adjust_call_status(row), axis=1, result_type='expand'
    )

    # Round Sell Price to 2 decimal places
    records_df['Sell Price'] = records_df['Sell Price'].round(2)

    # Update reversed_table with adjusted values
    for _, row in records_df.iterrows():
        update_adjusted_values_query = '''
        UPDATE reversed_table
        SET 
            "Call" = %s,
            "Status" = %s,
            "Hit Type" = %s,
            "Sell Price" = %s
        WHERE "Buy Date" = %s AND "Ticker" = %s;
        '''
        cur.execute(update_adjusted_values_query, (
            row["Call"], row["Status"], row["Hit Type"], row["Sell Price"],
            row["Buy Date"], row["Ticker"]
        ))
    conn.commit()

    # Separate Closed records to transfer to archived_trades
    to_archive = records_df[records_df["Status"] == "Closed"]

    # Insert Closed records into archived_trades
    for _, row in to_archive.iterrows():
        insert_into_archive_query = '''
        INSERT INTO archived_trades (
            "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close", 
            "Target", "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type", 
            "Sell Price", "Broker")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO NOTHING;
        '''
        cur.execute(insert_into_archive_query, (
            row["Buy Date"], row["Ticker"], row["Buy Strat"], row["Last Alert Date"],
            row["ll"], row["Close"], row["Target"], row["Call"], row["Status"],
            row["CMP"], row["CMP_Date"], row["Quantity"], row["Hit Type"],
            row["Sell Price"], row["Broker"]
        ))
    conn.commit()

    # Delete the Closed records from trade_journal after archiving
    delete_query = '''
    DELETE FROM trade_journal
    WHERE "Buy Date" = "CMP_Date" AND "Status" = 'Closed';
    '''
    cur.execute(delete_query)
    conn.commit()

    # Delete the Closed records from reversed_table after archiving
    delete_query2 = '''
    DELETE FROM reversed_table
    WHERE "Status" = 'Closed';
    '''
    cur.execute(delete_query2)
    conn.commit()

    cur.close()
    conn.close()



# Create a new function that imports data from ew conv, trade journal, and reversed table, and outputs 2 excel files (buy and sell)
# Function finds the tickers in the last 2 days alert from ew conv that also exist in TJ or reversed table.
def fetch_and_export_to_excel():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Fetch Ticker from Trade_Journal
    tj_ticker_query = ''' SELECT "Ticker", "Broker" FROM trade_journal'''
    tj_ticker_df = pd.read_sql(tj_ticker_query, conn)

    # Fetch Tickers from reversed_table
    reversed_query = '''SELECT "Ticker", "Broker" from reversed_table'''
    reversed_ticker_df = pd.read_sql(reversed_query, conn)

    tj_reversed_ticker = pd.concat([tj_ticker_df, reversed_ticker_df]).drop_duplicates()

    # Fetch from EW_CONV
    # BUY
    ew_query_buy = '''SELECT "Ticker", "Last Alert Date", "Buy Price"
    FROM ew_conv_table WHERE "Target" = 'NaN' '''
    ew_conv_buy_df = pd.read_sql(ew_query_buy, conn)

    last_two_dates_buy = ew_conv_buy_df['Last Alert Date'].nlargest(2)
    ew_conv_buy_df = ew_conv_buy_df[ew_conv_buy_df['Last Alert Date'].isin(last_two_dates_buy)]
    merge_buy = pd.merge(ew_conv_buy_df, tj_reversed_ticker, on='Ticker', how='left')

    # SELL
    ew_query_sell = '''SELECT "Ticker", "Last Alert Date", "Target"
    FROM ew_conv_table WHERE "Buy Price" = 'NaN' '''
    ew_conv_sell_df = pd.read_sql(ew_query_sell, conn)

    last_two_dates_sell = ew_conv_sell_df['Last Alert Date'].nlargest(2)
    ew_conv_sell_df = ew_conv_sell_df[ew_conv_sell_df['Last Alert Date'].isin(last_two_dates_sell)]
    merge_sell = pd.merge(ew_conv_sell_df, tj_reversed_ticker, on='Ticker', how='left')

    # Write the sheets to excel
    with pd.ExcelWriter('ew_conv_buy_and_sell_today_and_yesterday.xlsx') as writer:
        merge_buy.to_excel(writer, sheet_name='ew_conv_buy', index=False)
        merge_sell.to_excel(writer, sheet_name='ew_conv_sell', index=False)


# General function to update a table using adjust_call_status
def update_table_with_adjusted_status(table_name, conn):
    cur = conn.cursor()

    # Fetch all records from the specified table
    fetch_query = f'''
    SELECT 
        "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", 
        "Close", "Target", "Call", "Status", "CMP", "CMP_Date",
        "Quantity", "Hit Type", "Sell Price", "Broker"
    FROM {table_name};
    '''
    records_df = pd.read_sql(fetch_query, conn)

    # Apply adjust_call_status logic
    def adjust_call_status(row):
        if pd.notnull(row['CMP']) and pd.notnull(row['ll']) and row['CMP'] < row['ll']:
            return 'Short', 'Closed', 'ISL Hit', round(row['ll'], 2)
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] >= row['Target']:
            return 'Short', 'Closed', 'Target Hit', round(row['Target'], 2)
        return row['Call'], row['Status'], None, None

    records_df[['Call', 'Status', 'Hit Type', 'Sell Price']] = records_df.apply(
        lambda row: adjust_call_status(row), axis=1, result_type='expand'
    )
    records_df['Sell Price'] = records_df['Sell Price'].round(2)

    # Update the table with adjusted values
    for _, row in records_df.iterrows():
        update_query = f'''
        UPDATE {table_name}
        SET 
            "Call" = %s,
            "Status" = %s,
            "Hit Type" = %s,
            "Sell Price" = %s
        WHERE "Buy Date" = %s AND "Ticker" = %s;
        '''
        cur.execute(update_query, (
            row["Call"], row["Status"], row["Hit Type"], row["Sell Price"],
            row["Buy Date"], row["Ticker"]
        ))
    conn.commit()
    cur.close()

# Functions for specific tables
def update_trade_journal():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    update_table_with_adjusted_status('trade_journal', conn)
    conn.close()

def update_reversed_table():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Step 1: Update CMP and CMP_Date in reversed_table from yfinance_data
    update_cmp_query = '''
    UPDATE reversed_table
    SET
        "CMP" = y."CMP",
        "CMP_Date" = y."CMP_Date"
    FROM yfinance_data y
    WHERE reversed_table."Ticker" = y."Ticker";
    '''
    cur.execute(update_cmp_query)
    conn.commit()
    print("Updated CMP and CMP_Date in reversed_table.")

    # Step 2: Fetch updated reversed_table data
    fetch_reversed_table_query = '''
    SELECT 
        "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close",
        "Target", "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type",
        "Sell Price", "Broker"
    FROM reversed_table;
    '''
    reversed_table_df = pd.read_sql(fetch_reversed_table_query, conn)

    # Step 3: Apply adjust_call_status logic
    def adjust_call_status(row):
        if pd.notnull(row['CMP']) and pd.notnull(row['ll']) and row['CMP'] < row['ll']:
            return 'Short', 'Closed', 'ISL Hit', round(row['ll'], 2)
        elif pd.notnull(row['CMP']) and pd.notnull(row['Target']) and row['CMP'] >= row['Target']:
            return 'Short', 'Closed', 'Target Hit', round(row['Target'], 2)
        return row['Call'], row['Status'], None, None

    reversed_table_df[['Call', 'Status', 'Hit Type', 'Sell Price']] = reversed_table_df.apply(
        lambda row: adjust_call_status(row), axis=1, result_type='expand'
    )
    reversed_table_df['Sell Price'] = reversed_table_df['Sell Price'].round(2)

    # Step 4: Update reversed_table with adjusted values
    for _, row in reversed_table_df.iterrows():
        update_adjusted_query = '''
        UPDATE reversed_table
        SET 
            "Call" = %s,
            "Status" = %s,
            "Hit Type" = %s,
            "Sell Price" = %s
        WHERE "Buy Date" = %s AND "Ticker" = %s;
        '''
        cur.execute(update_adjusted_query, (
            row["Call"], row["Status"], row["Hit Type"], row["Sell Price"],
            row["Buy Date"], row["Ticker"]
        ))
    conn.commit()
    print("Updated Call, Status, Hit Type, and Sell Price in reversed_table.")

    # Step 5: Archive Closed Records from reversed_table
    to_archive = reversed_table_df[reversed_table_df["Status"] == "Closed"]
    for _, row in to_archive.iterrows():
        insert_into_archive_query = '''
        INSERT INTO archived_trades (
            "Buy Date", "Ticker", "Buy Strat", "Last Alert Date", "ll", "Close", 
            "Target", "Call", "Status", "CMP", "CMP_Date", "Quantity", "Hit Type", 
            "Sell Price", "Broker"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("Buy Date", "Ticker")
        DO NOTHING;
        '''
        cur.execute(insert_into_archive_query, (
            row["Buy Date"], row["Ticker"], row["Buy Strat"], row["Last Alert Date"],
            row["ll"], row["Close"], row["Target"], row["Call"], row["Status"],
            row["CMP"], row["CMP_Date"], row["Quantity"], row["Hit Type"],
            row["Sell Price"], row["Broker"]
        ))
    conn.commit()
    print("Archived closed records from reversed_table.")

    # Step 6: Delete Closed Records from reversed_table
    delete_query = '''
    DELETE FROM reversed_table
    WHERE "Status" = 'Closed';
    '''
    cur.execute(delete_query)
    conn.commit()
    print("Deleted closed records from reversed_table.")

    cur.close()
    conn.close()



# Main function to run the entire pipeline
def main():
    final_table = load_and_merge_csv()
    merged_table = fetch_and_merge_from_postgresql(final_table)
    save_and_upload(merged_table)
    transfer_closed()
    update_trade_journal()
    transfer_reversed()
    update_reversed_table()
    fetch_and_export_to_excel()

if __name__ == "__main__":
    main()