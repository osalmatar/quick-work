import os
import pandas as pd
import psycopg2
import io
from datetime import datetime

# Define the folder path and database connection details
folder_path = "C:/Users/ASUS/OneDrive/Documents/GitHub/AutomatedExcelReports/quick-work/150125/Download"  # Update path 

# PostgreSQL connection parameters
conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # Update with your database name
    user="postgres",
    password="St1uv9ac29!",  # Update with your password
    port="5432"  # Default port; adjust if necessary
)
cursor = conn.cursor()

# PostgreSQL table name
table_name = "yfinance_data"

# Function to extract Ticker name from the first line of the file
def extract_ticker(file_path):
    with open(file_path, 'r') as file:
        first_line = file.readline().strip()
    return first_line.split(" ")[-1]  # Extracts the Ticker after $NAME

# Function to validate and process a single file
def process_file(file_path, ticker):
    try:
        # Read the remaining lines after the first line
        with open(file_path, 'r') as file:
            lines = file.readlines()[1:]  # Skip the first line containing $NAME

        # Load data into a DataFrame
        df = pd.read_csv(
            io.StringIO("\n".join(lines)),
            header=None,
            names=["CMP_Date", "Open", "High", "Low", "CMP", "Volume"]
        )

        # Ensure the number of columns is correct
        df = df.dropna(subset=["CMP_Date", "CMP"])  # Remove rows with missing crucial fields

        # Specify a fixed date format to ensure consistency
        df["CMP_Date"] = pd.to_datetime(df["CMP_Date"], format="%Y-%m-%d", errors="coerce")

        # Drop rows with invalid dates
        df = df.dropna(subset=["CMP_Date"])

        # Ensure CMP is numeric
        df["CMP"] = pd.to_numeric(df["CMP"], errors="coerce").round(2)

        # Drop rows with invalid CMP values
        df = df.dropna(subset=["CMP"])

        # Add the Ticker column
        df["Ticker"] = ticker

        # Extract the row with the latest date
        latest_row = df.sort_values("CMP_Date", ascending=False).head(1)[["Ticker", "CMP_Date", "CMP"]]
        return latest_row
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None



# Drop the existing table if it exists
drop_table_query = '''
DROP TABLE IF EXISTS yfinance_data;
    '''
cursor.execute(drop_table_query)
conn.commit()

# Create table query
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    "Ticker" TEXT,
    "CMP_Date" TIMESTAMP,
    "CMP" FLOAT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Process each file in the folder
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    # Extract Ticker name from the first line
    ticker = extract_ticker(file_path)

    # Process the file and extract the latest row
    latest_data = process_file(file_path, ticker)
    if latest_data is not None and not latest_data.empty:
        for _, row in latest_data.iterrows():
            try:
                # Insert the data into PostgreSQL
                insert_query = f"""
                INSERT INTO {table_name} ("Ticker", "CMP_Date", "CMP")
                VALUES (%s, %s, %s);
                """
                cursor.execute(insert_query, (row["Ticker"], row["CMP_Date"], row["CMP"]))
                conn.commit()
            except Exception as e:
                print(f"Error inserting data for {ticker}: {e}")
                conn.rollback()  # Rollback transaction to prevent blocking

# Close the connection
cursor.close()
conn.close()

print("All files have been processed and uploaded to the PostgreSQL database.")
