import yfinance as yf
# Replace 'RELIANCE.NS' with the stock ticker symbol
stock_symbol = 'RELIANCE.NS'
stock = yf.Ticker(stock_symbol)

# Get the current price
current_price = stock.history(period='1d')['Close'][0]
print(f"Current price of {stock_symbol}: {current_price}")