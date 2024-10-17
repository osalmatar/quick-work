import pandas as pd
import yfinance as yf

# Read the Excel file
df = pd.read_excel('yfinance.xlsx')  

# Ensure there are no duplicates in the Tickers field
tickers = df['Ticker']

# Create an empty list to store the current prices
current_prices = []

# Loop through each ticker in the Tickers field
for stock_symbol in tickers:
    stock = yf.Ticker(stock_symbol)

    try:
        # Get the current price (latest close price)
        current_price = stock.history(period='1d')['Close'].iloc[0]
    except IndexError:
        current_price = None  # Handle cases where price data is unavailable
    
    # Append the current price to the list
    current_prices.append(current_price)

# Add the 'Current Price' column to the DataFrame
df['CMP'] = current_prices

# Save the updated DataFrame back to Excel
df.to_excel('yfinance_with_current_prices.xlsx', index=False)

print("Current prices added and file saved as 'yfinance_with_current_prices.xlsx'.")
