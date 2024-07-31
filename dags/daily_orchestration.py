from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from sqlalchemy import create_engine, inspect
import io
import psycopg2
from dotenv import load_dotenv, dotenv_values

# Load environment variables from .env file
load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['yourmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'datalake_sql_sync',
    default_args=default_args,
    description='A DAG to run scripts for syncing data between Azure Data Lake and Azure SQL Database',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2024, 7, 28, 6, 0),  # Start on 28-07-2024 at 6 AM UTC+1
    catchup=False,
)

def run_script_1():
    # #env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'
    # env_values = dotenv_values(os.path.join(env_dir, '.env'))

    account_name = os.getenv("account_name")
    container_name = os.getenv("container_name")

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    tenant_id = os.getenv("tenant_id")

    pg_server = os.getenv("pg_server")
    pg_database = os.getenv("pg_database")
    pg_username = os.getenv("pg_username")
    pg_password = os.getenv("pg_password")

    pg_conn = psycopg2.connect(
        dbname=pg_database, 
        user=pg_username, 
        password=pg_password, 
        host=pg_server,
        port=5432  # Ensure you specify the port if different
    )
    pg_cursor = pg_conn.cursor()

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    def get_max_id(df, id_column):
        return df[id_column].max() if not df.empty else None

    def directory_exists(directory_client):
        try:
            directory_client.get_directory_properties()
            return True
        except:
            return False

    pg_cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name NOT IN ('pg_buffercache', 'pg_stat_statements')
    """)
    tables = pg_cursor.fetchall()

    for table in tables:
        table_name = table[0]
        
        pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position LIMIT 1")
        id_column = pg_cursor.fetchone()[0]

        pg_cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
        max_id_pg = pg_cursor.fetchone()[0]

        directory_client = file_system_client.get_directory_client(f"{table_name}")

        if not directory_exists(directory_client):
            directory_client.create_directory()

        paths = list(file_system_client.get_paths(path=f"{table_name}"))
        if paths:
            latest_file = max(paths, key=lambda x: x.last_modified).name
            file_client = file_system_client.get_file_client(latest_file)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            csv_data = downloaded_bytes.decode('utf-8')
            df_existing = pd.read_csv(io.StringIO(csv_data))
            max_id_adls = get_max_id(df_existing, id_column)
        else:
            max_id_adls = None

        if max_id_pg and (max_id_adls is None or max_id_pg > max_id_adls):
            query_new_data = f"SELECT * FROM {table_name} WHERE {id_column} > {max_id_adls}" if max_id_adls else f"SELECT * FROM {table_name}"
            df_new = pd.read_sql(query_new_data, pg_conn)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            incremental_file_name = f"{table_name}_{timestamp}.csv"
            csv_buffer = io.StringIO()
            df_new.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            file_client = directory_client.get_file_client(incremental_file_name)
            file_client.upload_data(csv_data, overwrite=True)
            print(f"Incremental file {incremental_file_name} for table {table_name} uploaded to Azure Data Lake Gen 2 successfully.")
        else:
            print(f"No new data found for table {table_name}. No file was created.")

    pg_cursor.close()
    pg_conn.close()
    print("All tables have been processed and new data has been uploaded to Azure Data Lake Gen 2.")

def run_script_2():
    # env_dir = r'C:\Users\Temidayo\Documents\Docker_Airflow_Project'
    # env_values = dotenv_values(os.path.join(env_dir, '.env'))

    account_name = os.getenv("account_name")
    container_name = os.getenv("container_name")

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    tenant_id = os.getenv("tenant_id")

    sql_server = os.getenv("sql_server")
    sql_database = os.getenv("sql_database")
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")
    sql_driver = 'ODBC Driver 17 for SQL Server'

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    connection_string = f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver}"
    engine = create_engine(connection_string)

    def get_max_id_sql(table_name):
        try:
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            id_column = columns[0]['name']
            query = f"SELECT MAX({id_column}) as max_id FROM {table_name}"
            max_id = pd.read_sql(query, engine).iloc[0]['max_id']
            return max_id if max_id is not None else 0
        except Exception as e:
            print(f"Error getting max ID for table {table_name}: {e}")
            return 0

    def get_max_id_blob(directory_name):
        paths = list(file_system_client.get_paths(path=directory_name))
        if not paths:
            return None, 0

        latest_file = max(paths, key=lambda p: p.last_modified).name
        file_client = file_system_client.get_file_client(latest_file)
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        df = pd.read_csv(io.StringIO(downloaded_bytes.decode('utf-8')))
        id_column = df.columns[0]
        max_id = df[id_column].max()
        return latest_file, max_id

    directories = [path.name for path in file_system_client.get_paths() if path.is_directory]

    for directory_name in directories:
        table_name = directory_name
        max_id_sql = get_max_id_sql(table_name)
        latest_file_name, max_id_blob = get_max_id_blob(directory_name)
        
        if latest_file_name and max_id_blob > max_id_sql:
            file_client = file_system_client.get_file_client(latest_file_name)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            df_new = pd.read_csv(io.StringIO(downloaded_bytes.decode('utf-8')))
            new_data = df_new[df_new[df_new.columns[0]] > max_id_sql]

            new_data['Ingestion_Time'] = datetime.utcnow()
            new_data.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"New data successfully appended to the table {table_name}.")
        else:
            print(f"No new data found for table {table_name}.")

    print("All tables have been processed.")

run_script_1_task = PythonOperator(
    task_id='run_script_1',
    python_callable=run_script_1,
    dag=dag,
)

run_script_2_task = PythonOperator(
    task_id='run_script_2',
    python_callable=run_script_2,
    dag=dag,
)

notify_failure = EmailOperator(
    task_id='notify_failure',
    to='your_email@example.com',
    subject='Airflow DAG Failure: datalake_sql_sync',
    html_content="""<h3>Failure in DAG datalake_sql_sync</h3>""",
    trigger_rule='one_failed',
    dag=dag,
)

run_script_1_task >> run_script_2_task
[run_script_1_task, run_script_2_task] >> notify_failure
