import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# Function to connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="St1uv9ac29!",
            port="5432"
        )
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        raise

# Function to check if a table exists
def table_exists(conn, table_name):
    query = sql.SQL("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """)
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone()[0]

# Function to get table schema from PostgreSQL
def get_table_schema(conn, table_name):
    query = sql.SQL("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """)
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchall()

# Function to enforce data types based on table schema
def enforce_data_types(data, schema):
    type_map = {
        'integer': 'Int64',  # Pandas nullable integer
        'bigint': 'Int64',
        'smallint': 'Int64',
        'real': 'float',
        'double precision': 'float',
        'text': 'string',
        'character varying': 'string',
        'boolean': 'boolean',
        'date': 'datetime64[ns]',
        'timestamp without time zone': 'datetime64[ns]',
        'timestamp with time zone': 'datetime64[ns]'
    }
    for col, dtype in schema:
        if col in data.columns:
            pandas_dtype = type_map.get(dtype, 'object')  # Default to object for unsupported types
            data[col] = data[col].astype(pandas_dtype, errors='ignore')
    return data

# Function to append data without duplicates
def append_data(conn, table_name, data):
    # Create SQLAlchemy engine from psycopg2 connection details
    engine = create_engine('postgresql+psycopg2://postgres:St1uv9ac29!@localhost:5432/postgres')

    # Load existing data from the table using SQLAlchemy engine
    existing_data = pd.read_sql_table(table_name, con=engine)

    # Combine and drop duplicates
    combined_data = pd.concat([existing_data, data]).drop_duplicates()

    # Replace the table with combined data
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM {table}").format(table=sql.Identifier(table_name)))
        conn.commit()

    # Insert combined data back into the table
    combined_data.to_csv(f"{table_name}_temp.csv", index=False, header=False)
    with open(f"{table_name}_temp.csv", "r") as f:
        with conn.cursor() as cur:
            cur.copy_expert(
                sql.SQL("COPY {table} FROM STDIN WITH CSV").format(table=sql.Identifier(table_name)),
                f
            )
        conn.commit()

# Function to create a table based on the DataFrame structure
def create_table(conn, table_name, data):
    # Generate CREATE TABLE SQL with quoted column names
    columns = ", ".join(
        f'"{col}" TEXT' for col in data.columns  # Quote each column name with double quotes
    )
    create_query = sql.SQL("CREATE TABLE {table} ({columns})").format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(columns)
    )
    with conn.cursor() as cur:
        cur.execute(create_query)
        conn.commit()

    # Insert data into the newly created table
    data.to_csv(f"{table_name}_temp.csv", index=False, header=False)
    with open(f"{table_name}_temp.csv", "r") as f:
        with conn.cursor() as cur:
            cur.copy_expert(
                sql.SQL("COPY {table} FROM STDIN WITH CSV").format(table=sql.Identifier(table_name)),
                f
            )
        conn.commit()

# Main function
def main():
    # Specify file and table name directly
    file_path = "ATR.csv"  # Replace with your actual file path
    table_name = "ATR"  # Replace with your desired table name

    # Load data
    try:
        data = pd.read_csv(file_path)
    except Exception as e:
        print("Error reading the file:", e)
        return

    # Connect to the database
    conn = connect_to_db()

    try:
        # Check if table exists
        if table_exists(conn, table_name):
            print(f"Table '{table_name}' exists. Enforcing data types and appending data...")
            schema = get_table_schema(conn, table_name)  # Get existing table schema
            data = enforce_data_types(data, schema)  # Coerce data types
            append_data(conn, table_name, data)
        else:
            print(f"Table '{table_name}' does not exist. Creating table...")
            create_table(conn, table_name, data)
            print(f"Table '{table_name}' created and data inserted.")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
