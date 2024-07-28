# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 20:29:51 2024

@author: Temidayo
"""

import os
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from sqlalchemy import create_engine, inspect
import io
from datetime import datetime
from dotenv import load_dotenv, dotenv_values
import psycopg2

# Specify the directory where your .env file is located
env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'

# Load environment variables from .env file in the specified directory
env_values = dotenv_values(os.path.join(env_dir, '.env'))

# Azure Data Lake Gen 2 details
account_name = env_values.get("account_name")
container_name = env_values.get("container_name")

# Azure AD credentials
client_id = env_values.get("client_id")
client_secret = env_values.get("client_secret")
tenant_id = env_values.get("tenant_id")

# PostgreSQL Database details
pg_server = env_values.get("pg_server")
pg_database = env_values.get("pg_database")
pg_username = env_values.get("pg_username")
pg_password = env_values.get("pg_password")

# Create a connection to PostgreSQL
pg_conn = psycopg2.connect(
    dbname=pg_database, 
    user=pg_username, 
    password=pg_password, 
    host=pg_server
)
pg_cursor = pg_conn.cursor()

# Authenticate to Azure Data Lake Gen 2
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)

# Get the file system client (container)
file_system_client = service_client.get_file_system_client(file_system=container_name)

# Function to get max ID from a DataFrame
def get_max_id(df, id_column):
    return df[id_column].max() if not df.empty else None

# Function to check if a directory exists in Azure Data Lake
def directory_exists(directory_client):
    try:
        directory_client.get_directory_properties()
        return True
    except:
        return False

# List all tables in the PostgreSQL database, excluding system tables
pg_cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name NOT IN ('pg_buffercache', 'pg_stat_statements')
""")
tables = pg_cursor.fetchall()

# Loop through each table in PostgreSQL
for table in tables:
    table_name = table[0]
    
    # Get the first column (ID column) name
    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position LIMIT 1")
    id_column = pg_cursor.fetchone()[0]

    # Get max ID from PostgreSQL table
    pg_cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
    max_id_pg = pg_cursor.fetchone()[0]

    # Directory client for the table
    directory_client = file_system_client.get_directory_client(f"{table_name}")

    # Check if the directory exists in Azure Data Lake Gen 2, create if not
    if not directory_exists(directory_client):
        directory_client.create_directory()

    # List the files in the specified directory and get the latest file
    paths = list(file_system_client.get_paths(path=f"{table_name}"))
    if paths:
        latest_file = max(paths, key=lambda x: x.last_modified).name

        # Download the latest file into memory
        file_client = file_system_client.get_file_client(latest_file)
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        # Load the CSV data into a pandas DataFrame
        csv_data = downloaded_bytes.decode('utf-8')
        df_existing = pd.read_csv(io.StringIO(csv_data))

        # Get max ID from the existing data in Azure Data Lake Gen 2
        max_id_adls = get_max_id(df_existing, id_column)
    else:
        max_id_adls = None

    # Check if there are new rows in the PostgreSQL table
    if max_id_pg and (max_id_adls is None or max_id_pg > max_id_adls):
        # SQL query to read new data
        query_new_data = f"SELECT * FROM {table_name} WHERE {id_column} > {max_id_adls}" if max_id_adls else f"SELECT * FROM {table_name}"
        
        # Read new data from PostgreSQL into a pandas DataFrame
        df_new = pd.read_sql(query_new_data, pg_conn)

        # Generate the file name with the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        incremental_file_name = f"{table_name}_{timestamp}.csv"

        # Convert the DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df_new.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Upload the CSV file to Azure Data Lake Gen 2
        file_client = directory_client.get_file_client(incremental_file_name)
        file_client.upload_data(csv_data, overwrite=True)

        print(f"Incremental file {incremental_file_name} for table {table_name} uploaded to Azure Data Lake Gen 2 successfully.")
    else:
        print(f"No new data found for table {table_name}. No file was created.")

# Close the PostgreSQL connection
pg_cursor.close()
pg_conn.close()

print("All tables have been processed and new data has been uploaded to Azure Data Lake Gen 2.")
