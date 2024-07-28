# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 19:52:40 2024

@author: Temidayo
"""

import os
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io
from datetime import datetime
from dotenv import load_dotenv, dotenv_values

# Specify the directory where your .env file is located
env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'
env_values = dotenv_values(os.path.join(env_dir, '.env'))

# Azure Blob Storage details
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

# Authenticate to Azure Blob Storage
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=credential)

# Get the container client
container_client = blob_service_client.get_container_client(container_name)

# Get the list of blobs in the container
blobs = container_client.list_blobs()

# Create a SQLAlchemy engine for Azure SQL Database
connection_string = f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver}"
engine = create_engine(connection_string)

# Process each blob in the container
for blob in blobs:
    blob_client = container_client.get_blob_client(blob)
    
    # Download the blob content into memory
    download_stream = blob_client.download_blob()
    downloaded_bytes = download_stream.readall()
    
    # Load the CSV data into a pandas DataFrame
    csv_data = downloaded_bytes.decode('utf-8')
    try:
        df = pd.read_csv(io.StringIO(csv_data))
    except pd.errors.EmptyDataError:
        print(f"Skipping empty file: {blob.name}")
        continue
    
    # Get the current UTC time
    utc_now = datetime.utcnow()
    
    # Add a new column 'Ingestion_Time' with the current UTC time
    df['Ingestion_Time'] = utc_now
    
    # Derive table name from the blob name, correcting for redundant parts
    table_name_parts = os.path.splitext(blob.name)[0].split('/')
    table_name = table_name_parts[-1]  # Use only the last part of the blob name
    
    # Write the DataFrame to the Azure SQL Database table, overwriting the data
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Blob {blob.name} uploaded to Azure SQL Database table {table_name} successfully.")

print("All blobs have been processed and uploaded to Azure SQL Database.")
