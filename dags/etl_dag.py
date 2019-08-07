from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (DumpCsvFileToPostgres)
from numpy import uint32, float32
from pandas import UInt32Dtype

default_args = {
    'owner': 'gil',
    'start_date': datetime(2016, 9, 4),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
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

# Using the context manager allows me not to duplicate the dag parameter in each operator
with DAG('__e-commerce_etl',
         default_args=default_args,
         description='Load and transform data in a local Postgres Database with AirFlow',
         catchup=False,
         schedule_interval='0 * * * *'
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    dump_file = DumpCsvFileToPostgres(
        task_id='Dump_file',
        file_path='/root/airflow/dataset/dataset.csv',
        datatype=datatype,
        colsname=cols,
        db_connect='postgresql+psycopg2://airflow:airflow@postgres:5432',
        table_name='yolo'
    )

    stage_csv = DummyOperator(task_id='Stage_csv')

    geocoding_sellers = DummyOperator(task_id='Geocoding_sellers')

    load_sales_fact_table = DummyOperator(task_id='Load_sales_fact_table')

    load_products_dim_table = DummyOperator(task_id='Load_products_dim_table')
    load_categories_dim_table = DummyOperator(task_id='Load_categories_dim_table')
    load_stores_dim_table = DummyOperator(task_id='Load_stores_dim_table')
    load_counties_dim_table = DummyOperator(task_id='Load_counties_dim_table')
    load_vendors_dim_table = DummyOperator(task_id='Load_vendors_dim_table')
    load_calendar_dim_table = DummyOperator(task_id='Load_calendar_dim_table')

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

start_operator >> dump_file >> stage_csv
stage_csv >> dim_tables >> load_sales_fact_table
load_sales_fact_table >> geocoding_sellers
geocoding_sellers >> quality_check_1 >> quality_check_2 >> end_operator
