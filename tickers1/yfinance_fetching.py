import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime

# Read the Excel file
file_path = 'yfinance.xlsx'
df = pd.read_excel(file_path)

# Limit the DataFrame to the top 50 rows for testing 
df = df.head(50)

# Create a new column 'CMP' to store the current prices and 'Sell Date' for the current date
df['CMP'] = None
df['Sell Date'] = datetime.today().strftime('%Y-%m-%d')  # Current date in 'YYYY-MM-DD' format

# Loop through the 'Ticker' column and fetch current price for each ticker
for index, row in df.iterrows():
    stock_symbol = row['Ticker']
    stock = yf.Ticker(stock_symbol)
    
    try:
        current_price = stock.history(period='1d')['Close'].iloc[0]
        df.at[index, 'CMP'] = current_price
    except (IndexError, KeyError):
        print(f"Could not fetch price for {stock_symbol}")

# Save the updated DataFrame back to Excel
output_file = 'yfinance_cmp.xlsx'
df.to_excel(output_file, index=False)

# PostgreSQL database connection setup
conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="St1uv9ac29!",
        port="5432"
    )
cur = conn.cursor()

# Create table if it doesn't exist with quoted column names for case sensitivity
create_table_query = '''
CREATE TABLE IF NOT EXISTS yfinance_data (
    "Ticker" TEXT,   
    "CMP" FLOAT,
    "Sell_Date" DATE
);
'''
cur.execute(create_table_query)
conn.commit()

# Insert data into the PostgreSQL table with quoted column names
for index, row in df.iterrows():
    insert_query = '''
    INSERT INTO yfinance_data ("Ticker", "CMP", "Sell_Date") 
    VALUES (%s, %s, %s);
    '''
    cur.execute(insert_query, (row['Ticker'], row['CMP'], row['Sell Date']))

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()

print(f"Data saved to {output_file} and sent to PostgreSQL.")
