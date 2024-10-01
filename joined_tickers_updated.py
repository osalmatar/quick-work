import pandas as pd

A = pd.read_csv('ATR.csv')
B = pd.read_csv('D_EW_B.csv')
C = pd.read_csv('D_EW_S.csv')
D = pd.read_csv('HHLL.csv')
E = pd.read_csv('LR_Explore.csv')
F = pd.read_csv('Pattern_Revv.csv')
G = pd.read_csv('SCTR_Trial.csv')
H = pd.read_csv('ZigZag.csv')

A.rename(columns={'Buy': 'Buy_ATR'}, inplace=True)
A['Buy_ATR'] = A['Buy_ATR'].replace(1, 'ATR')
A.head

B.rename(columns={'Mega Buy by Larger Wave ': 'Buy_EW'}, inplace=True)
B['Buy_EW'] = 'EW'
B['Buy_EW'].unique()

E.rename(columns={'myBuy ': 'Buy_LR'}, inplace=True)
E['Buy_LR'] = E['Buy_LR'].replace(1, 'LR')
E.head()

F.rename(columns={'BullBreak': 'Buy_BB'}, inplace=True)
F['Buy_BB'] = F['Buy_BB'].replace('bullish Breakout', 'BB')
F['Pattern'].unique()

# Create the function to change the values 
def change_status(value):
    if value =='Wedge':
        return 'W'
    elif value == 'Up Channel':
        return 'UC'
    elif value == 'Down Channel':
        return 'DC'
    elif value == 'Broadening Wedge':
        return 'BW'
    elif value == 'Ascending Triangle':
        return 'AT'
    elif value == 'Decending Triangle':
        return 'DT'
    else:
        return value
    

#Apply the function on pattern from dataset F
F['Pattern'] = F['Pattern'].apply(change_status)
F['Pattern'].unique()

H.rename(columns={'Buy':'Buy_ZZ'}, inplace=True)
H['Buy_ZZ'] = H['Buy_ZZ'].replace(1, 'ZZ')
H['Buy_ZZ'].unique()


# Main function

# List of dataframes to merge
tables = [A, B, C, D, E, F, G, H]

# Start with an empty DataFrame for the final result
final_table = pd.DataFrame()

# Perform full outer join on Date/Time and create new rows for each Ticker
for table in tables:
    if 'Ticker' in table.columns:
        # Include 'Ticker' and 'Date/Time' in the melt operation
        temp = pd.melt(table, id_vars=['Date/Time', 'Ticker'], var_name='Field', value_name='Value')
    else:
        # If 'Ticker' is not present, use only 'Date/Time'
        temp = pd.melt(table, id_vars=['Date/Time'], var_name='Field', value_name='Value')

    final_table = pd.concat([final_table, temp], ignore_index=True)

# Pivot the final table to get back the desired structure
final_table = final_table.pivot_table(index=['Date/Time', 'Ticker'], columns='Field', values='Value', aggfunc='first').reset_index()

# Re-arrange columns to have 'Ticker' as the first column
cols = ['Ticker', 'Date/Time'] + [col for col in final_table.columns if col not in ['Ticker', 'Date/Time']]
final_table = final_table[cols]

# Fill NaN values with empty strings for the relevant columns
final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']] = final_table[['Buy_ATR', 'Buy_EW', 'Buy_BB', 'Buy_LR', 'Pattern']].fillna('')

# Create the 'Buy' column by concatenating the values from relevant columns
final_table['Buy'] = final_table['Buy_ATR'].astype(str) + final_table['Buy_EW'].astype(str) + final_table['Buy_BB'].astype(str) + final_table['Buy_LR'].astype(str) + final_table['Pattern'].astype(str)

# Re-arrange columns to place 'Buy' in the desired order
cols = ['Ticker', 'Date/Time', 'Buy'] + [col for col in final_table.columns if col not in ['Ticker', 'Date/Time', 'Buy']]
final_table = final_table[cols]

# Save the result to a CSV file
final_table.to_csv('joined_tickers.csv', index=False)
