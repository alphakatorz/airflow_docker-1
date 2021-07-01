import os
import sys
from datetime import datetime
import requests as rq
from sqlalchemy import create_engine
import logging 
import unicodedata
import re
import json
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd


# ----- CONVENIENCE FUNCTIONS -----

def process_columns(string_to_process, replace_string='_'):
    """
    Process a column name by :
    - processing special aplphabetical characters
    - replacing special characters by value set in 'replace_string' 

    #Parameters
    :param string_to_process (str): String to process (column name)
    :param replace_string (str, default : '_'): Value to set for replaced characters
        (ex : "/" -> "", where "" is the replace_string)

    #Return
    return out (str): Processed string
    """
    BIGQUERY_COLUMN_NAME_RE = "[^0-9a-zA-Z_]+"
    # Get encoding of column
    str_bytes = str.encode(string_to_process)
    encoding = 'utf-8'

    # Process any special alphabetical character
    temp_out = unicodedata\
        .normalize('NFKD', string_to_process)\
        .encode('ASCII', 'ignore')\
        .decode(encoding)

    out = re.sub(BIGQUERY_COLUMN_NAME_RE, replace_string, temp_out)

    return out

# ----- PYTHON CALLABLES -----

def transform_csv_to_df_to_table(
    data_path: str, 
    csv_name: str, 
    postgres_url: str,
    dwh_schema: str,
    table_name: str
) :
    """
    #### Extract task
    A simple Extract task to get data ready for the rest of the data
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    # DATA_PATH = "/Data_cm",
    # csv_path = f"{DATA_PATH}/{csv_name}"
    logging.info(f"chargement depuis {csv_name} ..")
    df = pd.read_csv(csv_name)
    logging.info(f"chargement depuis {csv_name} reussi")
    logging.info("dimensions du df")
    logging.info(df.shape)
    ls_col = []
    for column in df.columns :
        column_clean = process_columns(column)
        ls_col.append(column_clean)
    df.columns = ls_col
    logging.info(f"nettoyage du df des colonnes reussi")

    # creer la connexion a la base de donées
    engine = create_engine(postgres_url)
    logging.info(f"engine crée")

    with engine.connect() as connection:
    
        # creer le schema du DWH, engine.execute c'est tjr du SQL
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {dwh_schema}") 
        logging.info(f"schema {dwh_schema} reussi")

        # charger le df dans une table de la base de données
        df.to_sql(name=table_name,con=connection,schema=dwh_schema,if_exists="replace",chunksize=500)
        logging.info(f"table {table_name} crée")

# ----- DAG FUNCTION -----

def create_dag(
    dag_id,
    default_args,
    schedule=None
):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    ## TODO : CONSTANTES A METTRE DANS UNE VARIABLE AIRFLOW
    DATA_PATH = "/Data_cm",
    POSTGRES_URL = "postgres://postgres:password4db@postgres_dwh:5432/dwhdb"

    with dag:
        start = DummyOperator(
            task_id='start',
            dag=dag
        )

        end = DummyOperator(
            task_id='end',
            dag=dag
        )

        ## TODO : créer mécanisme avec Variables Airflow
        ls_tables = [
            {
                "file_name": "/Data_cm/sorties.csv",
                "table_name": "sorties",
                "dwh_schema": "stg"

            },
            {
                "file_name": "/Data_cm/entree.csv",
                "table_name": "entree",
                "dwh_schema": "stg"
                
            }
        ]

        for table in ls_tables:
            file_name = table['file_name']
            table_name = table['table_name']
            dwh_schema = table['dwh_schema']
            run_this = PythonOperator(
                dag=dag,
                task_id=f'csv_load_{table_name}',
                python_callable=transform_csv_to_df_to_table,
                op_kwargs = {
                    "data_path":DATA_PATH, 
                    "csv_name": file_name, 
                    "postgres_url": POSTGRES_URL,
                    "dwh_schema": dwh_schema,
                    "table_name": table_name
                }
            )
            start >> run_this >> end

        return dag





SCHEDULE = None
DAG_ID = 'import_entrees_sorties'

ARGS = {
    'owner': 'YKA',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1,
    'max_active_runs': 1
}
globals()[DAG_ID] = create_dag(DAG_ID, ARGS, SCHEDULE)