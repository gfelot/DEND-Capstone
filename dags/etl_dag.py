from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators import (StageToRedshiftOperator)
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from numpy import uint32, float32
from pandas import UInt32Dtype
# from helpers import SqlQueries

default_args = {
    'owner': 'gil',
    'start_date': datetime(2019, 8, 7),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False
}

datatype = {"Invoice/Item Number": str,
            "Date": str,
            "Store Number": uint32,
            "Store Name": str,
            "Address": str,
            "City": str,
            "Zip Code": str,
            "Store Location": str,
            "County Number": UInt32Dtype(),
            "County": str,
            "Category": str,
            "Category Name": str,
            "Vendor Number": UInt32Dtype(),
            "Vendor Name": str,
            "Item Number": uint32,
            "Item Description": str,
            "Pack": uint32,
            "Bottle Volume (ml)": uint32,
            "State Bottle Cost": str,
            "State Bottle Retail": str,
            "Bottles Sold": uint32,
            "Sale (Dollars)": str,
            "Volume Sold (Liters)": float32,
            "Volume Sold (Gallons)": float32}

cols = list(datatype.keys())

def list_key():
    # Get the value from the admin -> connection panel
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    logging.info(f'Listing S3 {bucket}')
    keys = hook.list_keys(bucket)
    for k in keys:
        logging.info(f'- s3://{bucket}/{k}')

# Using the context manager allows me not to duplicate the dag parameter in each operator
with DAG('__e-commerce_etl',
         default_args=default_args,
         description='Load and transform data in a local Postgres Database with AirFlow',
         catchup=False,
         schedule_interval='* 0 * * *'
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_to_redshift = PythonOperator(
        task_id='list_S3_keys',
        python_callable=list_key
    )

    # stage_events_to_redshift = StageToRedshiftOperator(
    #     task_id='Stage_csv_to_redshift',
    #     table='staging',
    #     redshift_conn_id='redshift',
    #     aws_credentials_id='aws_credentials',
    #     s3_bucket='ucapstone-dend',
    #     s3_key='dataset',
    #     sql=SqlQueries.stage_to_redshift
    # )

    # dump_file = DumpCsvFileToPostgres(
    #     task_id='Dump_file',
    #     file_path='/root/airflow/dataset/dataset.csv',
    #     datatype=datatype,
    #     colsname=cols,
    #     db_connect='postgresql+psycopg2://airflow:airflow@postgres:5432',
    #     table_name='yolo'
    # )

    load_sales_fact_table = DummyOperator(task_id='Load_sales_fact_table')

    load_products_dim_table = DummyOperator(task_id='Load_products_dim_table')
    load_categories_dim_table = DummyOperator(task_id='Load_categories_dim_table')
    load_stores_dim_table = DummyOperator(task_id='Load_stores_dim_table')
    load_counties_dim_table = DummyOperator(task_id='Load_counties_dim_table')
    load_vendors_dim_table = DummyOperator(task_id='Load_vendors_dim_table')
    load_calendar_dim_table = DummyOperator(task_id='Load_calendar_dim_table')

    geocoding_sellers = DummyOperator(task_id='Geocoding_sellers')

    quality_check_1 = DummyOperator(task_id='quality_check_1')

    quality_check_2 = DummyOperator(task_id='quality_check_2')

    end_operator = DummyOperator(task_id='Stop_execution')

# Make graph

dim_tables = [
    load_products_dim_table,
    load_categories_dim_table,
    load_stores_dim_table,
    load_counties_dim_table,
    load_vendors_dim_table,
    load_calendar_dim_table
]

start_operator >> stage_to_redshift
stage_to_redshift >> dim_tables >> load_sales_fact_table
load_sales_fact_table >> geocoding_sellers
geocoding_sellers >> quality_check_1 >> quality_check_2 >> end_operator
