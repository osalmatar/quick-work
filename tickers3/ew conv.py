import pandas as pd
import psycopg2

# Function to process ticker data and add 'Call' and 'Status' columns
def process_ticker_data(df):
    # Add the 'Call' column based on 'Mega Buy by Larger Wave'
    df['Call'] = df['Mega Buy by Larger Wave '].apply(lambda x: 'Long' if pd.notna(x) and x > 1 else 'Short')
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')
    
    # Sort by 'Ticker' and 'Date/Time' to get the latest entry for each Ticker
    df = df.sort_values(by=['Ticker', 'Date/Time'])
    
    # Keep only the last entry for each ticker
    last_day_df = df.groupby('Ticker').agg(lambda x: x.iloc[-1]).reset_index()
    
    # Add the 'Status' column based on the 'Call' value
    last_day_df['Status'] = last_day_df['Call'].apply(lambda x: 'Open' if x == 'Long' else 'Close')
    
    return last_day_df

# Function to load, process ew_conv, and send to PostgreSQL using psycopg2
def load_and_process_ew_conv():
    # Load the CSV
    ew_conv = pd.read_csv('tickers2/EW_Conv.csv')
    ew_conv['Date/Time'] = pd.to_datetime(ew_conv['Date/Time'], errors='coerce')
    
    # Process the data
    processed_ew_conv = process_ticker_data(ew_conv)
    
    # Select and rename columns
    processed_ew_conv = processed_ew_conv[['Ticker', 'Date/Time', 'Mega Buy by Larger Wave ', 'Mega Sellby Larger Wave', 'Call', 'Status']]
    processed_ew_conv.rename(columns={
        'Date/Time': 'Last Alert Date',
        'Mega Buy by Larger Wave ': 'Buy Price',
        'Mega Sellby Larger Wave': 'Target'
    }, inplace=True)
    processed_ew_conv.to_csv('ew_conv_out.csv', index = False)
    
    # Connect to PostgreSQL using psycopg2
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
    cur = conn.cursor()

    # Drop the existing table, if it exists
    drop_table_query = '''
    DROP TABLE IF EXISTS ew_conv_table;
    '''
    cur.execute(drop_table_query)

    # Create table with case-sensitive column names
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS ew_conv_table (
        "Ticker" VARCHAR(50),
        "Last Alert Date" TIMESTAMP,
        "Buy Price" FLOAT,
        "Target" FLOAT,
        "Call" VARCHAR(10),
        "Status" VARCHAR(10),
        PRIMARY KEY ("Ticker", "Last Alert Date")
    );
    '''
    cur.execute(create_table_query)

    # Insert or update data into the PostgreSQL table
    insert_query = '''
    INSERT INTO ew_conv_table ("Ticker", "Last Alert Date", "Buy Price", "Target", "Call", "Status")
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT ("Ticker", "Last Alert Date")
    DO UPDATE SET
        "Buy Price" = EXCLUDED."Buy Price",
        "Target" = EXCLUDED."Target",
        "Call" = EXCLUDED."Call",
        "Status" = EXCLUDED."Status";
    '''

    # Insert or update each row
    for _, row in processed_ew_conv.iterrows():
        cur.execute(insert_query, tuple(row))

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

    # Return the processed data
    return processed_ew_conv

# Call the function to load, process, and update PostgreSQL
df_processed_ew_conv = load_and_process_ew_conv()
