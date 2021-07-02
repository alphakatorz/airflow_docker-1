import unicodedata
import re
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import logging
import tempfile
import shutil
import os
import numpy as np
from datetime import datetime


## -----| TOOLBOX |------
def get_minio_client(
    host="minio", 
    port="9000", 
    access_key="minio", 
    secret_key="minio123", 
    secure=False
    ):
    """
    Get minIO client with credentials provided as arg
    
    #Parameters
    :param host (str, default to 'minio'): Host of the minIO Server
    :param port (str, default to '9000'): Port of the minIO Server
    :param access_key (str, default to 'minio'): Access key to connect to the minIO server
    :param secret_key (str, default to 'minio123'): Secret key to connect to the minIO server
    :param secure (bool, default to True): HTTPS (True) or HTTP (False)
    
    
    #Return
    return: minio_client (minio.Minio): the minIO client object
    
    """
    return Minio(
        endpoint=f"{host}:{port}",
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
        )

def get_file_fromMinio(bucket_name, minio_object_path, local_object_path):
    """
    Download a file from minIO
    
    #Parameters
    :param bucket_name (str): Name of the minIO bucket
    :param minio_object_path (str): source path of the object in the minIO Server
    :param local_object_path (str, default to 'minio'): destination path of the object in the local environment
    
    """
    minio_client = get_minio_client()
    minio_client.fget_object(
        bucket_name=bucket_name, 
        object_name=minio_object_path,
        file_path=local_object_path
        )

def build_postgres_url_from_creds(
    dwh_username="postgres",
    dwh_password="password4db", 
    dwh_host="postgres_dwh",
    dwh_port="5432",
    dwh_db="dwhdb"
    ):
    
    return f"postgresql+psycopg2://{dwh_username}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_db}"


def get_immats_fromdwh(dwh_schema, table_name, connection, date_col="date_immat"):
    """
    Get a list of years not fully loaded in the datawarehouse 
    
    #Parameters
    :param dwh_schema (str): Schema of the table in the DWH
    :param table_name (str): Name of the table in the DWH
    :param connection (SQLAlchemy.connection): Open SQLAlchemy connection to the DWH
    :param date_col (str, default to 'date_immat'): name of the date column to use
    
    #Return
    return: dc_years (dict): dict of years with date loaded in the DWH
    """
    
    query = f"SELECT distinct date AS date_immat FROM {dwh_schema}.{table_name}"
    df = pd.read_sql(query,con=connection) 
    df["year_immat"] = df[date_col].str[-4:]
    dc_years = dict()
    for year, nb_mois in df.year_immat.value_counts().iteritems():
        if nb_mois < 12:
            key_name = f"immats/immats_{year}.csv"
            dc_years[key_name] = df[df["year_immat"]==year][date_col].tolist()
    return dc_years

def get_files_from_minio(bucket_name, file_prefix, dc_years=None):
    """
    Get a list of files to download from minIO.
    Those files contain data that is not yet in the DWH
    
    #Parameters
    :param bucket_name (str): Schema of the table in the DWH
    :param file_prefix (str): Object path in the minIO Server
    :param dc_years (dict, default to None): dict of years with date loaded in the DWH
    
    """
    minio_client = get_minio_client()
    ls_minio_files = list(minio_client.list_objects(bucket_name, file_prefix))
    ls_files_to_download = []
    for obj in (ls_minio_files):
        # On charge le fichier seulement si
        # - dc_years est renseigné ET que l'objet est dedans
        # - OU que dc_years n'est pas renseigné
        if (
            (
                dc_years is not None and 
                obj.object_name in dc_years
                ) or
                dc_years is None
                ):
                ls_files_to_download.append(obj.object_name)
                # get_file_fromMinio(bucket_name, minio_object_path, local_object_path)
    return ls_files_to_download

def download_files_from_minio(bucket_name, file_prefix, ls_files_to_download):
    """
    Get a list of years not fully loaded in the datawarehouse 
    #Parameters
    :param bucket_name (str): Schema of the table in the DWH
    :param file_prefix (str): Object path in the minIO Server
    :param ls_years_not_in_dwh (list, default to None): Open SQLAlchemy connection to the DWH
    
    """
    minio_client = get_minio_client()
    ls_minio_files = list(minio_client.list_objects(bucket_name, file_prefix))
    ls_downloaded_files = []
    local_object_dir = tempfile.mkdtemp()
    for minio_object_path in ls_files_to_download:
        local_object_name = minio_object_path.replace(file_prefix, "")
        local_object_path = os.path.join(local_object_dir,local_object_name) 
        get_file_fromMinio(bucket_name, minio_object_path, local_object_path)
        ls_downloaded_files.append(local_object_path)
    return ls_downloaded_files


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
    out = out.lower()
    return out


def process_month_name(raw_date):
    month_short = raw_date.split('@')[0]
    if month_short.endswith('.'):
        clean_month = month_short
    else:
        clean_month = month_short[:3] + "."
        clean_date = clean_month + raw_date.split('@')[1]
    return clean_date


def clean_code_departement(code):
    code = str(code)
    if code.startswith("0"):
        code = code[1:]
    return code


def remove_empty_months(df, month_col_name="DATE"):
    ls_empty = []
    for month in df[month_col_name]:
        print(month)
        df_month = df[df[month]]
        if df_month.shape[0] < 2:
            ls_empty.append(month)
            
    df_clean = df[~df[month_col_name].str.isin(ls_empty)]
    return df_clean


def clean_df(df):
    ls_col = []
    for column in df:
        column_clean = process_columns(column)
        ls_col.append(column_clean)
        df.columns = ls_col
        df["DATE"] = df["DATE"].apply(process_month_name)
        df['code_departement'] = df['code_departement'].apply(clean_code_departement)
    return df


def process_file(
    downloaded_file,
    dc_years
    ):
    
    #1. on read le fichier dans un df 
    print(f"loading df from {downloaded_file}")
    sheets = pd.ExcelFile(downloaded_file)
    df = pd.read_excel(sheets,sheet_name=sheets.sheet_names[0])
    #2. On récupère les lignes de tous les mois qui ne sont pas dans le DWH
    ls_date_in_dwh_for_that_year = dc_years[downloaded_file]
    date_filter = (df["DATE"].str.isin(ls_date_in_dwh_for_that_year))
    df_immats_filtred = df[date_filter]
    #3. On enlève les mois vides
    df_clean = remove_empty_months(df_clean)
    #4. On clean le df
    df_clean = clean_df(df_immats_filtred)
    return df_clean


def load_into_dwh(df, postgres_url, dwh_schema, table_name):
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        df_clean.to_sql(name=table_name,con=connection,schema=dwh_schema,if_exists="append")
        print('Successfully loaded dataframe into DWH')