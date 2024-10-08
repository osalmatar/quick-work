import pandas as pd
import psycopg2

# Load DataFrame from Tickers.csv and specify column types
def load_from_csv():
    return pd.read_csv(
        'Tickers.csv',
        dtype={
            'Alert': 'str',  # Alert will be initially loaded as a string
            'BearBreak': 'str', 
            'Buy_ATR': 'str',
            'Buy_BB': 'str', 
            'Buy_EW': 'str', 
            'Buy_LR': 'str', 
            'Buy_ZZ': 'str',
            'Pattern': 'str',
            'Call': 'str',  # Call will contain 'Long' or 'Short'
            'Status': 'str',  # Status will contain 'Open' or 'Close'
        },
        low_memory=False
    )

# Prepare DataFrame for insertion by converting Date/Time, replacing non-numeric values with None, and ensuring correct data types
def prepare_data_for_insert(df):
    # Convert 'Date/Time_x' and 'Date/Time_y' to datetime format
    df['Date/Time_x'] = pd.to_datetime(df['Date/Time_x'], errors='coerce')
    df['Date/Time_y'] = pd.to_datetime(df['Date/Time_y'], errors='coerce')

    # Handle the 'Alert' column: Treat non-numeric values as 0
    df['Alert'] = pd.to_numeric(df['Alert'], errors='coerce').fillna(0).astype(int)

    # Convert necessary fields to floats or integers where applicable
    df['Close_x'] = pd.to_numeric(df['Close_x'], errors='coerce')
    df['SL_x'] = pd.to_numeric(df['SL_x'], errors='coerce')
    df['Uptrend Length'] = pd.to_numeric(df['Uptrend Length'], errors='coerce').fillna(0).astype(int)
    df['UpBars'] = pd.to_numeric(df['UpBars'], errors='coerce').fillna(0).astype(int)
    df['Megabuy'] = pd.to_numeric(df['Mega Buy by Larger Wave '], errors='coerce')
    df['Megasell'] = pd.to_numeric(df['Mega Sellby Larger Wave_y'], errors='coerce')
    df['Short_SL'] = pd.to_numeric(df['Short_SL'], errors='coerce')

    # Replace NaN with None for numerical columns while keeping NaN for categorical columns
    df = df.where(pd.notna(df), None)

    return df

# Function to insert DataFrame into PostgreSQL without dropping the table (append)
def insert_to_postgres(df):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",  # your host
            database="postgres",  # your database
            user="postgres",  # your username
            password="St1uv9ac29!",  # replace with your PostgreSQL password
            port="5432"  # default port for PostgreSQL
        )
        
        cur = conn.cursor()

        # Insert data into PostgreSQL
        insert_query = '''
        INSERT INTO joined_tickers (date_time_x, ticker_x, alert, bearbreak, buy_atr, buy_bb, buy_ew, buy_lr, buy_zz, close_x, 
            dnbars, downtrend_length, fromhigh, fromlow, lr_lc, lr_mc, lr_uc, mega_sellby_larger_wave_x, numberoftickers, 
            pattern, percentilerank, rank, s1, sctr, sl_x, upbars, uptrend_length, hh, ll, myshort, buy, primary_key, 
            date_time_y, ticker_y, close_y, sl_y, megabuy, megasell, short_sl, call, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # Iterate over DataFrame and insert each row
        for _, row in df.iterrows():
            # Prepare the row of values for insertion
            values = (
                row['Date/Time_x'], 
                row['Ticker_x'], 
                row.get('Alert', 0),  
                row.get('BearBreak', 0),
                row.get('Buy_ATR', None),
                row.get('Buy_BB', None),
                row.get('Buy_EW', None),
                row.get('Buy_LR', None),
                row.get('Buy_ZZ', None),
                row.get('Close_x', None),
                row.get('DnBars', None),
                row.get('Downtrend Length', None),
                row.get('FromHigh', None),
                row.get('FromLow', None),
                row.get('LR_LC', None),
                row.get('LR_MC', None),
                row.get('LR_UC', None),
                row.get('Mega Sellby Larger Wave_x', None),
                row.get('NumberOfTickers', None),
                row.get('Pattern', None),
                row.get('PercentileRank', None),
                row.get('Rank', None),
                row.get('S1', None),
                row.get('SCTR', None),
                row.get('SL_x', None),
                row.get('UpBars', None),
                row.get('Uptrend Length', None),
                row.get('hh', None),
                row.get('ll', None),
                row.get('myShort', None),
                row.get('Buy', None),
                row.get('Primary_Key', None),
                row.get('Date/Time_y', None),
                row.get('Ticker_y', None),
                row.get('Close_y', None),
                row.get('SL_y', None),
                row.get('Megabuy', None),
                row.get('Megasell', None),
                row.get('Short_SL', None),
                row.get('Call', None),
                row.get('Status', None)
            )

            # Execute insert query
            cur.execute(insert_query, tuple(values))

        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error: {error}")

# Load and prepare the data
df = load_from_csv()
df = prepare_data_for_insert(df)

# Insert the DataFrame into PostgreSQL (Append)
insert_to_postgres(df)
