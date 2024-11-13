import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime   

# Read stock tickers from Excel
def read_stock_list(file_path):
    try:
        stock_df = pd.read_excel(file_path)
        return stock_df['Ticker'].tolist()
    except Exception as e:
        print(f"Error reading stock list: {e}")
        return []

# Fetch the latest market prices for individual tickers
def fetch_latest_market_prices(tickers):
    current_prices = []
    timestamps = []
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            stock_data = stock.history(period='1d')  # Fetching daily data
            
            # Get the last available closing price and timestamp
            last_price = stock_data['Close'].iloc[-1]
            last_timestamp = stock_data.index[-1]  # Get the last timestamp
            
            current_prices.append({'Ticker': ticker, 'Current Price': last_price})
            timestamps.append(last_timestamp)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    
    return current_prices, timestamps


# Save data to PostgreSQL
def save_to_postgres(df, conn):
    cur = conn.cursor()

    # Drop the existing table if it exists
    drop_table_query = '''
    DROP TABLE IF EXISTS yfinance_data;
    '''
    cur.execute(drop_table_query)
    conn.commit()

    # Create table with quoted column names for case sensitivity
    create_table_query = '''
    CREATE TABLE yfinance_data (
        "Ticker" TEXT,   
        "CMP" FLOAT,
        "CMP_Date" TIMESTAMP
    );
    '''
    cur.execute(create_table_query)
    conn.commit()

    # Insert data into the PostgreSQL table
    for index, row in df.iterrows():
        insert_query = '''
        INSERT INTO yfinance_data ("Ticker", "CMP", "CMP_Date") 
        VALUES (%s, %s, %s);
        '''
        cur.execute(insert_query, (row['Ticker'], row['Current Price'], row['CMP Date']))
    
    conn.commit()

# Main function
def main():
    file_path = 'yfinance.xlsx'
    
    # Read stock tickers from the Excel file
    stock_list = read_stock_list(file_path)

    # Limit to 20 records for testing purposes
    #stock_list = stock_list[:20]

    if stock_list:
        # Fetch current prices and their timestamps
        current_prices, timestamps = fetch_latest_market_prices(stock_list)

        if current_prices:
            # Convert to DataFrame for better formatting
            prices_df = pd.DataFrame(current_prices)
            
            # Add CMP Date using the last timestamp for each ticker
            prices_df['CMP Date'] = [timestamp.strftime('%Y-%m-%d %H:%M:%S') for timestamp in timestamps]

            # Save to Excel
            output_file = 'current_stock_prices.xlsx'
            prices_df.to_excel(output_file, index=False)
            print(f"Current prices saved to '{output_file}'.")

            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="St1uv9ac29!",
                port="5432"
            )
            
            # Save the data to PostgreSQL
            save_to_postgres(prices_df, conn)
            conn.close()

            print("Data also saved to PostgreSQL.")
        else:
            print("No data to save.")
    else:
        print("Stock list is empty.")

if __name__ == "__main__":
    main()
