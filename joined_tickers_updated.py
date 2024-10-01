import pandas as pd

A = pd.read_csv('ATR.csv')
B = pd.read_csv('D_EW_B.csv')
C = pd.read_csv('D_EW_S.csv')
D = pd.read_csv('HHLL.csv')
E = pd.read_csv('LR_Explore.csv')
F = pd.read_csv('Pattern_Revv.csv')
G = pd.read_csv('SCTR_Trial.csv')
H = pd.read_csv('ZigZag.csv')

#Data cleaning to rename each value of Buy into the name of the tinker instead of 1

#ATR
A['Buy'] = A['Buy'].replace(1, 'ATR')

#D_EW
B.rename(columns={'Mega Buy by Larger Wave ': 'Buy'}, inplace=True)
B['Buy'] = 'EW'

#LR_Explore
E.rename(columns={'myBuy ': 'Buy'}, inplace=True)
E['Buy'] = E['Buy'].replace(1, 'LR')

#Pattern_Revv
F.rename(columns={'BullBreak': 'Buy'}, inplace=True)
F['Buy'] = F['Buy'].replace('bullish Breakout', 'BB')

#Zigzag
H['Buy'] = H['Buy'].replace(1, 'ZZ')


# Main function starts here
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

# Save the result to a CSV file
final_table.to_csv('joined_tickers.csv', index=False)

