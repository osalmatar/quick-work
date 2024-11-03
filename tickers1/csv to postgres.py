import pandas as pd
import psycopg2

# Load DataFrame from CSV and specify column types
def load_from_csv(filename):
    df = pd.read_csv(filename, low_memory=False)
    
    # Strip any leading/trailing whitespace from the column names
    df.columns = df.columns.str.strip()

    return df

# Prepare DataFrame for insertion by replacing NaN with 0 for integers/floats and None for other types
def prepare_data_for_insert(df):
    # Replace NaN with 0 for integer and float columns
    for col in df.select_dtypes(include=['float', 'int']).columns:
        df[col] = df[col].fillna(0)
    
    # Replace NaN with None for non-numeric columns (e.g., strings)
    df = df.where(pd.notna(df), None)

    return df

# Function to create the table if it doesn't exist
def create_table_if_not_exists(cur):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS joined_tickers (
        date_time TIMESTAMP,
        ticker VARCHAR,
        alert INTEGER,
        bearbreak VARCHAR,
        buy_atr VARCHAR,
        buy_bb VARCHAR,
        buy_ew VARCHAR,
        buy_lr VARCHAR,
        buy_zz VARCHAR,
        close FLOAT,
        dnbars INTEGER,
        downtrend_length INTEGER,
        fromhigh INTEGER,
        fromlow INTEGER,
        lr_lc FLOAT,
        lr_mc FLOAT,
        lr_uc FLOAT,
        mega_sellby_larger_wave FLOAT,
        numberoftickers INTEGER,
        pattern VARCHAR,
        percentilerank FLOAT,
        rank INTEGER,
        s1 FLOAT,
        sctr FLOAT,
        sl FLOAT,
        upbars INTEGER,
        uptrend_length INTEGER,
        hh FLOAT,
        ll FLOAT,
        myshort VARCHAR,
        buy VARCHAR,
        current_price FLOAT,
        quantity INTEGER,
        pl_percentage FLOAT,
        ticker_y VARCHAR,
        date_time_y TIMESTAMP,
        close_y FLOAT,
        sl_y FLOAT,
        mega_buy_by_larger_wave FLOAT,
        mega_sellby_larger_wave_y FLOAT,
        short_sl FLOAT,
        call VARCHAR,
        status VARCHAR
    );
    '''
    cur.execute(create_table_query)

# Function to check if processed_ew_conv data exists in the joined_tickers table
def check_ew_conv_exists(cur):
    # Check if any row in the joined_tickers table has a non-NULL ticker_y value
    cur.execute("SELECT EXISTS (SELECT 1 FROM joined_tickers WHERE ticker_y IS NOT NULL LIMIT 1);")
    return cur.fetchone()[0]

# Insert processed_ew_conv into PostgreSQL (9 fields)
def insert_to_postgres_ew_conv(df, conn):
    cur = conn.cursor()
    create_table_if_not_exists(cur)

    insert_query = '''
    INSERT INTO joined_tickers (ticker_y, date_time_y, close_y, sl_y, mega_buy_by_larger_wave, 
        mega_sellby_larger_wave_y, short_sl, call, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['Ticker'], row['Date/Time'], row['Close'], row['SL'], 
            row['Mega Buy by Larger Wave'],  # Ensure column name matches exactly
            row['Mega Sellby Larger Wave'], 
            row['Short_SL'], row['Call'], row['Status']
        ))
    
    conn.commit()
    cur.close()

# Insert cleaned_merged_tickers into PostgreSQL (43 fields)
def insert_to_postgres_cleaned(df, conn):
    cur = conn.cursor()

    insert_query = '''
    INSERT INTO joined_tickers (date_time, ticker, alert, bearbreak, buy_atr, buy_bb, buy_ew, buy_lr, buy_zz, close, 
        dnbars, downtrend_length, fromhigh, fromlow, lr_lc, lr_mc, lr_uc, mega_sellby_larger_wave, numberoftickers, 
        pattern, percentilerank, rank, s1, sctr, sl, upbars, uptrend_length, hh, ll, myshort, buy, current_price, 
        quantity, pl_percentage)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['Date/Time'], row['Ticker'], row['Alert'], row['BearBreak'], row['Buy_ATR'], 
            row['Buy_BB'], row['Buy_EW'], row['Buy_LR'], row['Buy_ZZ'], row['Close'], 
            row['DnBars'], row['Downtrend Length'], row['FromHigh'], row['FromLow'], row['LR_LC'], 
            row['LR_MC'], row['LR_UC'], row['Mega Sellby Larger Wave'], row['NumberOfTickers'], 
            row['Pattern'], row['PercentileRank'], row['Rank'], row['S1'], row['SCTR'], 
            row['SL'], row['UpBars'], row['Uptrend Length'], row['hh'], row['ll'], row['myShort'], 
            row['Buy'], row['Current_Price'], row['Quantity'], row['P/L Percentage']
        ))
    
    conn.commit()
    cur.close()

# Main function to handle data insertion
def handle_data_insertion():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="St1uv9ac29!",
            port="5432"
        )
        cur = conn.cursor()

        # Create the table if it doesn't exist
        create_table_if_not_exists(cur)

        # Check if ew_conv data already exists (static)
        ew_conv_exists = check_ew_conv_exists(cur)

        # Insert ew_conv data only once if it doesn't exist
        if not ew_conv_exists:
            print("Inserting static ew_conv data...")
            df_ew_conv = load_from_csv('processed_ew_conv.csv')
            df_ew_conv = prepare_data_for_insert(df_ew_conv)
            insert_to_postgres_ew_conv(df_ew_conv, conn)

        # Append cleaned_merged_tickers data each time
        print("Appending cleaned_merged_tickers data...")
        df_cleaned_merged = load_from_csv('cleaned_merged_tickers.csv')
        df_cleaned_merged = prepare_data_for_insert(df_cleaned_merged)
        insert_to_postgres_cleaned(df_cleaned_merged, conn)

        conn.close()

    except Exception as error:
        print(f"Error: {error}")

# Run the data insertion
handle_data_insertion()