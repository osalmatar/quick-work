import pandas as pd
import yfinance as yf

# Function to process ticker data and add 'Call' and 'Status' columns
def process_ticker_data(df):
    # Step 1: Create the 'Call' column based on the 'Mega Buy by Larger Wave ' column
    df['Call'] = df['Mega Buy by Larger Wave '].apply(lambda x: 'Long' if pd.notna(x) and x > 1 else 'Short')

    # Step 2: Convert 'Date/Time' to datetime and sort by 'Ticker' and 'Date/Time'
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')  # Convert 'Date/Time' safely
    df = df.sort_values(by=['Ticker', 'Date/Time'])

    # Step 3: Extract the last row for each Ticker without filling NaN values using agg
    last_day_df = df.groupby('Ticker').agg(lambda x: x.iloc[-1]).reset_index()

    # Step 4: Create the 'Status' column based on the 'Call' value in the last record for each Ticker
    last_day_df['Status'] = last_day_df['Call'].apply(lambda x: 'Open' if x == 'Long' else 'Close')

    # Step 5: Filter and remove rows where Call field is Short
    last_day_df = last_day_df[last_day_df['Call'] == 'Long']

    return last_day_df

# Function to load and clean multiple CSV files
def load_and_merge_csv():
    A = pd.read_csv('ATR.csv')
    B = pd.read_csv('D_EW_B.csv')
    C = pd.read_csv('D_EW_S.csv')
    D = pd.read_csv('HHLL.csv')
    E = pd.read_csv('LR_Explore.csv')
    F = pd.read_csv('Pattern_Revv.csv')
    G = pd.read_csv('SCTR_Trial.csv')
    H = pd.read_csv('ZigZag.csv')

    # Ensure Date/Time is properly parsed as datetime
    for df in [A, B, C, D, E, F, G, H]:
        df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')

    # Renaming columns and performing necessary transformations
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

    # Map values in the 'Pattern' column to new values
    pattern_mapping = {
        'Up Channel': '_UC',
        'Wedge': '_W',
        'Down Channel': '_DC',
        'Broadening Wedge': '_BW',
        'Ascending Triangle': '_AT',
        'Decending Triangle': '_DT'
    }
    F['Pattern'] = F['Pattern'].replace(pattern_mapping)

    # Merge tables
    tables = [A, B, C, D, E, F, G, H]
    final_table = pd.DataFrame()

    for table in tables:
        if 'Ticker' in table.columns:
            temp = pd.melt(table, id_vars=['Date/Time', 'Ticker'], var_name='Field', value_name='Value')
        else:
            temp = pd.melt(table, id_vars=['Date/Time'], var_name='Field', value_name='Value')
        final_table = pd.concat([final_table, temp], ignore_index=True)

    # Pivot the table to the final structure
    final_table = final_table.pivot_table(index=['Date/Time', 'Ticker'], columns='Field', values='Value', aggfunc='first').reset_index()

    # Replace NaN values with empty strings in columns needed for concatenation
    final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']] = final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']].fillna('')

    # Function to concatenate non-NaN values into the Buy column
    def concatenate_non_nan(row):
        values = [row['Buy_ATR'], row['Buy_EW'], row['Buy_BB'], row['Buy_LR'], row['Pattern']]
        non_nan_values = [str(v) for v in values if pd.notna(v)]  # Filter out NaN values
        return ''.join(non_nan_values)  # Concatenate the non-NaN values

    # Apply the function row-by-row to create the Buy column
    final_table['Buy'] = final_table.apply(concatenate_non_nan, axis=1)

    # Filter out rows where 'Buy' matches certain patterns
    exclude_values = ['_UC', '_W', '_DC', '_BW', '_AT', '_DT']
    final_table = final_table[~final_table['Buy'].isin(exclude_values)]

    # Filter out rows where 'Buy' is empty (None, NaN, or empty string)
    final_table = final_table[final_table['Buy'].notna() & (final_table['Buy'] != '')]

    # Find the latest Date/Time
    latest_date = final_table['Date/Time'].max()

    # Filter the DataFrame to include only rows with the latest Date/Time
    latest_records = final_table[final_table['Date/Time'] == latest_date].copy()  # Use .copy() to avoid SettingWithCopyWarning

    # Create a new column for current stock price using .loc
    latest_records.loc[:, 'Current_Price'] = None

    for index, row in latest_records.iterrows():
        stock_symbol = row['Ticker']  # Set stock symbol from each row in 'Ticker'
        stock = yf.Ticker(stock_symbol)

        # Get the current price using iloc to avoid FutureWarning
        try:
            current_price = stock.history(period='1d')['Close'].iloc[0]
            latest_records.loc[index, 'Current_Price'] = current_price
        except IndexError:
            latest_records.loc[index, 'Current_Price'] = None  # Handle cases where price data is unavailable

    # Replace NaN or infinite values in 'Close' and 'll' before division to avoid ZeroDivisionError
    # Explicitly convert to float after filling NaN
    # Convert object columns to their inferred types before applying .fillna()
    latest_records['Close'] = latest_records['Close'].infer_objects().fillna(0).astype(float)
    latest_records['ll'] = latest_records['ll'].infer_objects().fillna(0).astype(float)



    # Avoid division by zero by replacing zero values in 'Close - ll' with a small non-zero number
    latest_records['Quantity'] = 1000 / latest_records.apply(lambda row: (row['Close'] - row['ll']) if (row['Close'] - row['ll']) != 0 else 1e-6, axis=1)

    # Calculate 'P/L Percentage'
    latest_records['P/L Percentage'] = (latest_records['Current_Price'] - latest_records['Close']) / latest_records['Close']

    # Round 'Current_Price', 'Close', and 'P/L Percentage' to 2 decimal digits
    latest_records['Current_Price'] = latest_records['Current_Price'].round(2)
    latest_records['Close'] = latest_records['Close'].round(2)
    latest_records['P/L Percentage'] = latest_records['P/L Percentage'].round(2)
    latest_records['Quantity'] = latest_records['Quantity'].round(0)

    # Save the final filtered DataFrame to a CSV
    latest_records.to_csv('cleaned_merged_tickers.csv', index=False)

    return latest_records

# Function to load and process ew_conv, process it, and output separately
def load_and_process_ew_conv():
    # Load EW_Conv and ensure 'Date/Time' is datetime
    ew_conv = pd.read_csv('EW_Conv.csv')
    ew_conv['Date/Time'] = pd.to_datetime(ew_conv['Date/Time'], errors='coerce')

    # Process the ew_conv dataset using the process_ticker_data function
    processed_ew_conv = process_ticker_data(ew_conv)

    # Save the processed ew_conv DataFrame to a CSV file
    processed_ew_conv.to_csv('processed_ew_conv.csv', index=False)

    return processed_ew_conv

# Call the function to load and process the datasets separately
df_cleaned_merged_tickers = load_and_merge_csv()
df_processed_ew_conv = load_and_process_ew_conv()

# The two datasets are saved as 'cleaned_merged_tickers.csv' and 'processed_ew_conv.csv'
