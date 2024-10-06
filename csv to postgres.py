import pandas as pd
import psycopg2

# Load DataFrame from CSV and specify column types
def load_from_csv():
    return pd.read_csv(
        'merged_tickers.csv',
        dtype={
            'Alert': 'str',  # Alert will be initially loaded as a string
            'BearBreak': 'str', 
            'Buy_ATR': 'str',
            'Buy_BB': 'str', 
            'Buy_EW': 'str', 
            'Buy_LR': 'str', 
            'Buy_ZZ': 'str',
            'Pattern': 'str'
        },
        low_memory=False
    )

# Prepare DataFrame for insertion by converting Date/Time, replacing non-numeric values with None, and ensuring correct data types
def prepare_data_for_insert(df):
    # Convert 'Date/Time' to datetime format
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')

    # Handle the 'Alert' column: Treat non-numeric values as 0
    df['Alert'] = pd.to_numeric(df['Alert'], errors='coerce').fillna(0).astype(int)

    # Handle binary conversion for 'BearBreak' column
    df['BearBreak'] = df['BearBreak'].apply(lambda x: 1 if x == 'Bearish Breakout' else 0)

    # Replace NaN with None for numerical columns while keeping NaN for categorical columns
    df = df.where(pd.notna(df), None)

    # Filter out rows where 'Buy' is empty (None, NaN, or empty string)
    df = df[df['Buy'].notna() & (df['Buy'] != '')]

    return df

# Function to insert DataFrame into PostgreSQL
def insert_to_postgres(df):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",  # your host
            database="postgres",  # your database
            user="postgres",  # your username
            password="password",  # replace with your PostgreSQL password
            port="5432"  # default port for PostgreSQL
        )
        
        cur = conn.cursor()

        # Drop table if it already exists
        drop_table_query = '''
        DROP TABLE IF EXISTS joined_tickers;
        '''
        cur.execute(drop_table_query)
        conn.commit()

        # Create table in PostgreSQL with the correct structure and 'Buy' as the last column
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS joined_tickers (
            date_time TIMESTAMP,
            ticker TEXT,
            buy TEXT,
            alert NUMERIC,
            bearbreak NUMERIC,  -- Binary (1 for Bearish Breakout, 0 otherwise)
            buy_atr TEXT,
            buy_bb TEXT,
            buy_ew TEXT,
            buy_lr TEXT,
            buy_zz TEXT,
            close NUMERIC,
            dnbars NUMERIC,
            downtrend_length NUMERIC,
            fromhigh NUMERIC,
            fromlow NUMERIC,
            lr_lc NUMERIC,
            lr_mc NUMERIC,
            lr_uc NUMERIC,
            mega_sellby_larger_wave NUMERIC,
            numberoftickers NUMERIC,
            pattern TEXT,
            percentilerank NUMERIC,
            rank NUMERIC,
            s1 NUMERIC,
            sctr NUMERIC,
            sl NUMERIC,
            upbars NUMERIC,
            uptrend_length NUMERIC,
            hh NUMERIC,
            ll NUMERIC,
            myshort NUMERIC
        );
        '''
        cur.execute(create_table_query)
        conn.commit()

        # Insert data into PostgreSQL, with 'Buy' as the last field
        insert_query = '''
        INSERT INTO joined_tickers (date_time, ticker, buy, alert, bearbreak, buy_atr, buy_bb, buy_ew, buy_lr, buy_zz, close, 
            dnbars, downtrend_length, fromhigh, fromlow, lr_lc, lr_mc, lr_uc, mega_sellby_larger_wave, numberoftickers, 
            pattern, percentilerank, rank, s1, sctr, sl, upbars, uptrend_length, hh, ll, myshort)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # Iterate over DataFrame and insert each row
        for _, row in df.iterrows():
            # Prepare the row of values for insertion, with 'Buy' as the last value
            values = (
                row['Date/Time'], 
                row['Ticker'], 
                row['Buy'],  # Buy remains as text
                row.get('Alert', 0),  # Alert with non-numeric values treated as 0
                row.get('BearBreak', 0),
                row.get('Buy_ATR', None),
                row.get('Buy_BB', None),
                row.get('Buy_EW', None),
                row.get('Buy_LR', None),
                row.get('Buy_ZZ', None),
                row.get('Close', None),
                row.get('DnBars', None),
                row.get('Downtrend Length', None),
                row.get('FromHigh', None),
                row.get('FromLow', None),
                row.get('LR_LC', None),
                row.get('LR_MC', None),
                row.get('LR_UC', None),
                row.get('Mega Sellby Larger Wave', None),
                row.get('NumberOfTickers', None),
                row.get('Pattern', None),
                row.get('PercentileRank', None),
                row.get('Rank', None),
                row.get('S1', None),
                row.get('SCTR', None),
                row.get('SL', None),
                row.get('UpBars', None),
                row.get('Uptrend Length', None),
                row.get('hh', None),
                row.get('ll', None),
                row.get('myShort', None),  
            )

            cur.execute(insert_query, values)

        conn.commit()

        # Verify data insertion
        cur.execute("SELECT * FROM joined_tickers LIMIT 10")
        rows = cur.fetchall()
        for row in rows:
            print(row)

        # Close the cursor and connection
        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error: {error}")

# Load and prepare the data
df = load_from_csv()

df = prepare_data_for_insert(df)

# Insert the DataFrame into PostgreSQL
insert_to_postgres(df)
