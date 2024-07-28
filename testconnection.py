# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 09:46:39 2024

@author: Temidayo
"""

import psycopg2
import os
from dotenv import load_dotenv, dotenv_values

# Load environment variables from .env file
load_dotenv()

pg_server = os.getenv("pg_server")
pg_database = os.getenv("pg_database")
pg_username = os.getenv("pg_username")
pg_password = os.getenv("pg_password")

try:
    pg_conn = psycopg2.connect(
        dbname=pg_database, 
        user=pg_username, 
        password=pg_password, 
        host=pg_server, 
        port=5432  # Specify port if different
    )
    print("Connection successful!")
    pg_conn.close()
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
