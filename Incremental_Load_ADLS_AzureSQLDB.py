# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 19:52:40 2024

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

# Specify the directory where your .env file is located
env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'
env_values = dotenv_values(os.path.join(env_dir, '.env'))

# Azure Data Lake Gen 2 details
account_name = env_values.get("account_name")
container_name = env_values.get("container_name")

# Azure AD credentials
client_id = env_values.get("client_id")
client_secret = env_values.get("client_secret")
tenant_id = env_values.get("tenant_id")

# Azure SQL Database details
sql_server = env_values.get("sql_server")
sql_database = env_values.get("sql_database")
sql_username = env_values.get("sql_username")
sql_password = env_values.get("sql_password")
sql_driver = 'ODBC Driver 17 for SQL Server'

# Authenticate to Azure Data Lake Gen 2
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)

# Get the file system client (container)
file_system_client = service_client.get_file_system_client(file_system=container_name)

# Create a SQLAlchemy engine for Azure SQL Database
connection_string = f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver}"
engine = create_engine(connection_string)

# Function to get max ID from a table in Azure SQL Database
def get_max_id_sql(table_name):
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        id_column = columns[0]['name']  # Assume the first column is the ID column
        query = f"SELECT MAX({id_column}) as max_id FROM {table_name}"
        max_id = pd.read_sql(query, engine).iloc[0]['max_id']
        return max_id if max_id is not None else 0
    except Exception as e:
        print(f"Error getting max ID for table {table_name}: {e}")
        return 0

# Function to get the latest file and max ID from Azure Data Lake
def get_max_id_blob(directory_name):
    paths = list(file_system_client.get_paths(path=directory_name))
    if not paths:
        return None, 0

    latest_file = max(paths, key=lambda p: p.last_modified).name
    file_client = file_system_client.get_file_client(latest_file)
    download = file_client.download_file()
    downloaded_bytes = download.readall()

    df = pd.read_csv(io.StringIO(downloaded_bytes.decode('utf-8')))
    id_column = df.columns[0]  # Assume the first column is the ID column
    max_id = df[id_column].max()
    return latest_file, max_id

# List all folders (directories) in the Azure Data Lake container
directories = [path.name for path in file_system_client.get_paths() if path.is_directory]

# Process each directory (table)
for directory_name in directories:
    table_name = directory_name
    max_id_sql = get_max_id_sql(table_name)
    latest_file_name, max_id_blob = get_max_id_blob(directory_name)
    
    if latest_file_name and max_id_blob > max_id_sql:
        file_client = file_system_client.get_file_client(latest_file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        df_new = pd.read_csv(io.StringIO(downloaded_bytes.decode('utf-8')))
        new_data = df_new[df_new[df_new.columns[0]] > max_id_sql]  # Filter new data

        # Add a new column 'Ingestion_Time' with the current UTC time
        new_data['Ingestion_Time'] = datetime.utcnow()

        # Append the new data to the Azure SQL Database table
        new_data.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"New data successfully appended to the table {table_name}.")
    else:
        print(f"No new data found for table {table_name}.")

print("All tables have been processed.")
