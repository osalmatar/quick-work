import pandas as pd

ew_conv = pd.read_csv('ew_conv_out.csv')
trade_journal = pd.read_csv('final_trade_journal.csv')

merge = pd.merge(ew_conv, trade_journal, on = 'Ticker', how = 'inner')

# Write the merged dataframe to a CSV file
merge.to_csv('merged_data.csv', index=False)
