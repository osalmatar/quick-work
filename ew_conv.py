import pandas as pd

def process_ticker_data(csv_file):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Step 1: Create the 'Call' column based on the 'Mega Buy by Larger Wave ' column
    df['Call'] = df['Mega Buy by Larger Wave '].apply(lambda x: 'Long' if x > 1 else 'Short')

    # Step 2: Convert 'Date/Time' to datetime and sort by 'Ticker' and 'Date/Time'
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    df = df.sort_values(by=['Ticker', 'Date/Time'])

    # Step 3: Extract the last row for each Ticker (latest 'Date/Time' record)
    last_day_df = df.groupby('Ticker', as_index=False).last()

    # Step 4: Create the 'Status' column based on the 'Call' value in the last record for each Ticker
    last_day_df['Status'] = last_day_df['Call'].apply(lambda x: 'Open' if x == 'Long' else 'Close')

    return last_day_df

# Execute the function 
output_df = process_ticker_data('EW_Conv.csv')


# Save the result to a new CSV file
output_df.to_csv('last_day_ticker_output.csv', index=False)
