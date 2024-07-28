# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 18:40:18 2024

@author: Temidayo
"""


# Import all Necessary Libraries
import pandas as pd
from sqlalchemy import create_engine, inspect
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os
import io
from dotenv import load_dotenv, dotenv_values

# Specify the directory where your .env file is located
env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'
env_values = dotenv_values(os.path.join(env_dir, '.env'))

# PostgreSQL connection details
pg_server = env_values.get("pg_server")
pg_database = env_values.get("pg_database")
pg_username = env_values.get("pg_username")
pg_password = env_values.get("pg_password")

# Create a SQLAlchemy engine for PostgreSQL
pg_connection_string = f"postgresql://{pg_username}:{pg_password}@{pg_server}/{pg_database}"
pg_engine = create_engine(pg_connection_string)

# Azure Data Lake details
account_name = env_values.get("account_name")
container_name = env_values.get("container_name")

# Authenticate to Azure Data Lake Gen 2
client_id = env_values.get("client_id")
client_secret = env_values.get("client_secret")
tenant_id = env_values.get("tenant_id")

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)

# Get the file system client (container)
file_system_client = service_client.get_file_system_client(file_system=container_name)

# Function to upload data to Azure Data Lake
def upload_to_adls(data, folder_name, file_name):
    directory_client = file_system_client.get_directory_client(folder_name)
    file_client = directory_client.get_file_client(file_name)
    file_client.upload_data(data, overwrite=True)

# Get the list of tables in the PostgreSQL database
inspector = inspect(pg_engine)
tables = inspector.get_table_names()

# Iterate over each table and upload its data to Azure Data Lake
for table in tables:
    print(f"Processing table: {table}")
    
    # Read data from PostgreSQL table into a pandas DataFrame
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, pg_engine)
    
    # Save DataFrame as a CSV file in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload the CSV file content to Azure Data Lake Gen 2
    upload_to_adls(csv_buffer.getvalue(), folder_name=table, file_name=f"{table}.csv")
    
    print(f"Table {table} uploaded to Azure Data Lake Gen 2 successfully.")

print("All tables have been processed and uploaded to Azure Data Lake Gen 2.")

