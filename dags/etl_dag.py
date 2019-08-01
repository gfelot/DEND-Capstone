from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'gil',
    'start_date': datetime(2016, 9, 4),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'email_on_retry':  False
}

# Using the context manager allows me not to duplicate the dag parameter in each operator
with DAG('__e-commerce_etl',
         default_args=default_args,
         description='Load and transforme data in a local Postgres Database with AirFlow',
         catchup=False,
         schedule_interval='0 * * * *'
         ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_orders_dataset = DummyOperator(task_id='Stage_orders_dataset')
    stage_order_items = DummyOperator(task_id='Stage_order_items')
    stage_sellers_dataset = DummyOperator(task_id='Stage_sellers_dataset')
    stage_products_dataset = DummyOperator(task_id='Stage_products_dataset')
    stage_payments_dataset = DummyOperator(task_id='Stage_payments_dataset')

    geocoding_sellers = DummyOperator(task_id='Geocoding_sellers')

    load_sell_fact_table = DummyOperator(task_id='Load_sell_fact_table')

    load_orders_dim_table = DummyOperator(task_id='Load_orders_dim_table')
    load_sellers_dim_table = DummyOperator(task_id='Load_sellers_dim_table')
    load_products_dim_table = DummyOperator(task_id='Load_products_dim_table')
    load_payments_dim_table = DummyOperator(task_id='Load_payments_dim_table')
    load_calendar_dim_table = DummyOperator(task_id='Load_calendar_dim_table')

    quality_check_1 = DummyOperator(task_id='quality_check_1')
    quality_check_2 = DummyOperator(task_id='quality_check_2')

    end_operator = DummyOperator(task_id='Stop_execution')



# Make graph

start_operator >> [stage_orders_dataset, stage_order_items, stage_sellers_dataset, stage_products_dataset, stage_payments_dataset]
stage_sellers_dataset >> geocoding_sellers
[stage_orders_dataset, stage_order_items, geocoding_sellers, stage_products_dataset, stage_payments_dataset] >> load_sell_fact_table
load_sell_fact_table >> [load_orders_dim_table, load_sellers_dim_table, load_products_dim_table, load_payments_dim_table, load_calendar_dim_table]
[load_orders_dim_table, load_sellers_dim_table, load_products_dim_table, load_payments_dim_table, load_calendar_dim_table] >> quality_check_1
quality_check_1 >> quality_check_2
quality_check_2 >> end_operator
