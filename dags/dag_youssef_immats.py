from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

from utils_immats import (get_minio_client, get_files_from_minio, build_postgres_url_from_creds,
    get_immats_fromdwh, get_files_from_minio, download_files_from_minio, process_month_name,
    clean_code_departement, remove_empty_months, clean_df, process_file, load_into_dwh)


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'YKA',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def ETL_IMMATS(
    dwh_username="postgres",
    dwh_password="password4db", 
    dwh_host="postgres_dwh",
    dwh_port="5432",
    dwh_db="dwhdb",
    dwh_schema="stg",
    bucket_name="landing-bucket",
    file_prefix="immats/",
    table_name="immats"
        
):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """


    @task()
    def extract(
        dwh_username="postgres",
        dwh_password="password4db", 
        dwh_host="postgres_dwh",
        dwh_port="5432",
        dwh_db="dwhdb",
        bucket_name="landing-bucket",
        file_prefix="immats/"
    ):
        postgres_url = build_postgres_url_from_creds(dwh_username, dwh_password, dwh_host, dwh_port, dwh_db)
        engine = create_engine(postgres_url)
        with engine.connect() as connection:
            dc_years = get_immats_fromdwh(
                dwh_schema = "stg",
                table_name = "immats",
                connection = connection
                )
        return dc_years



    @task()
    def transform(
        bucket_name,
        file_prefix,
        dc_years
    ):
        ls_files_to_download = get_files_from_minio(
            bucket_name=bucket_name,
            file_prefix=file_prefix,
            dc_years=dc_years
        )
        ls_downloaded_files = download_files_from_minio(
            bucket_name=bucket_name,
            file_prefix=file_prefix,
            ls_files_to_download=ls_files_to_download
        )
        return ls_downloaded_files
    

    @task()
    def load(
        dc_years,
        ls_downloaded_files,
        table_name,
        dwh_username="postgres",
        dwh_password="password4db", 
        dwh_host="postgres_dwh",
        dwh_port="5432",
        dwh_db="dwhdb",
        dwh_schema="stg",
    ):
        postgres_url = build_postgres_url_from_creds(dwh_username, dwh_password, dwh_host, dwh_port, dwh_db)
        for downloaded_file in ls_downloaded_files:
            df = process_file(downloaded_file, dc_years)
            load_into_dwh(df, postgres_url, dwh_schema, table_name)

    
    dc_years = extract(
        dwh_username=dwh_username,
        dwh_password=dwh_password,
        dwh_host=dwh_host,
        dwh_port=dwh_port,
        dwh_db=dwh_db,
        bucket_name=bucket_name,
        file_prefix=file_prefix
    )

    ls_downloaded_files = transform(
        bucket_name=bucket_name,
        file_prefix=file_prefix,
        dc_years=dc_years
    )

    load(
        dc_years=dc_years,
        ls_downloaded_files=ls_downloaded_files,
        table_name=table_name,
        dwh_username=dwh_username,
        dwh_password=dwh_password,
        dwh_host=dwh_host,
        dwh_port=dwh_port,
        dwh_db=dwh_db,
        dwh_schema=dwh_schema
    )


etl_immats = ETL_IMMATS()