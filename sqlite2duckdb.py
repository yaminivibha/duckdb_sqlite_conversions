import sqlite3
import pandas as pd
import duckdb
import os

# SQLite to CSV
sqlite_conn = sqlite3.connect('makeorig.sqlite.db')
cursor = sqlite_conn.cursor()
print(cursor)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"tables:{tables}")
table_csv_map = {}

for table in tables:
    print(table)
    df = pd.read_sql_query("SELECT * from %s" % table[0], sqlite_conn)

    # Convert boolean columns to str before exporting
    for col in df.select_dtypes(include=['bool']).columns:
        df[col] = df[col].apply(lambda x: 'TRUE' if x else 'FALSE')
    df.fillna("", inplace=True)
    csv_name = table[0] + '.csv'
    df.to_csv(csv_name, index=False)
    table_csv_map[table[0]] = csv_name

sqlite_conn.close()

# CSV to DuckDB
duckdb_conn = duckdb.connect('tryingagain.db')

for table, csv_file in table_csv_map.items():
    df = pd.read_csv(csv_file)

    # Convert object columns to the correct type if necessary
    for col in df.select_dtypes(include=['object']).columns:
        if (df[col].astype(str).str.lower() == 'true').all() or (df[col].astype(str).str.lower() == 'false').all():
            df[col] = df[col].astype(bool)

    print(f"Data types in DataFrame for table {table}:")
    print(df.dtypes)

    # If the table already exists in DuckDB, drop it
    if duckdb_conn.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{table}'").fetchall():
        duckdb_conn.execute(f"DROP TABLE {table}")

    duckdb_conn.register(table, df)

    try:
        duckdb_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM {table}")
    except Exception as e:
        print(f"Error occurred while creating table {table} in DuckDB: {e}")

duckdb_conn.close()


# DELETE ALL THE CSVs

directory = '.'  # Set the directory path

for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        os.remove(file_path)
