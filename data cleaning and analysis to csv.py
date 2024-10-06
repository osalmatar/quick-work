import pandas as pd

# Function to load CSV files and return a merged DataFrame
def load_and_merge_csv():
    A = pd.read_csv('ATR.csv')
    B = pd.read_csv('D_EW_B.csv')
    C = pd.read_csv('D_EW_S.csv')
    D = pd.read_csv('HHLL.csv')
    E = pd.read_csv('LR_Explore.csv')
    F = pd.read_csv('Pattern_Revv.csv')
    G = pd.read_csv('SCTR_Trial.csv')
    H = pd.read_csv('ZigZag.csv')

    # Renaming columns and performing necessary transformations
    A.rename(columns={'Buy': 'Buy_ATR'}, inplace=True)
    A['Buy_ATR'] = A['Buy_ATR'].replace(1, 'ATR')

    B.rename(columns={'Mega Buy by Larger Wave ': 'Buy_EW'}, inplace=True)
    B['Buy_EW'] = 'EW'

    E.rename(columns={'myBuy ': 'Buy_LR'}, inplace=True)
    E['Buy_LR'] = E['Buy_LR'].replace(1, 'LR')

    F.rename(columns={'BullBreak': 'Buy_BB'}, inplace=True)
    F['Buy_BB'] = F['Buy_BB'].replace('bullish Breakout', 'BB')

    H.rename(columns={'Buy':'Buy_ZZ'}, inplace=True)
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

    # Apply the pattern mapping to column 'Pattern'
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

    # Save the final table to a CSV (optional, for manual review)
    final_table.to_csv('merged_tickers.csv', index=False)

    # Return the final DataFrame
    return final_table

# Call the function to load and merge CSV files
df = load_and_merge_csv()

print(df[['Ticker', 'Date/Time', 'Buy', 'Pattern']].tail())  # Print to verify the DataFrame output
