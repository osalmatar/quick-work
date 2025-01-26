import pandas as pd
import psycopg2

# Load the CSV file
df = pd.read_csv('hhll.csv')  

# Calculate the required 'll' and 'hh' for each Ticker
result = (
    df.groupby('Ticker', as_index=False)
    .agg({'ll': 'min', 'hh': 'max'})
)

# Quick look at the output
print(result.head())

# PostgreSQL connection details
conn = psycopg2.connect(
    host="localhost",  
    database="",  
    user="",  
    password="",  
    port="5432"  
)
cur = conn.cursor()

# Create the table
create_table_query = '''
DROP TABLE IF EXISTS hhll;
CREATE TABLE hhll (
    Ticker TEXT PRIMARY KEY,
    ll FLOAT,
    hh FLOAT
);
'''
cur.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = '''
INSERT INTO hhll (Ticker, ll, hh) VALUES (%s, %s, %s)
ON CONFLICT (Ticker)
DO UPDATE SET ll = EXCLUDED.ll, hh = EXCLUDED.hh;
'''
for _, row in result.iterrows():
    cur.execute(insert_query, (row['Ticker'], row['ll'], row['hh']))

# Commit and close the connection
conn.commit()
cur.close()
conn.close()

print("Data successfully saved to PostgreSQL table 'hhll'.")
