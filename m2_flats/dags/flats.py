import pendulum
import parser
import etl
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 9, tz="UTC"),
    catchup=False,
    template_searchpath='data/sql/',
    tags=["example"]
)
def flats():

    clear_data_task = PythonOperator(
        task_id="clear_data",
        python_callable=etl.clear_data
    )

    # Создание таблиц 

    create_dims_task = PostgresOperator(
        task_id="create_dims_table",
        postgres_conn_id='postgres_conn',
        sql=etl.dims_sql
    )

    create_fact_table = PostgresOperator(
        task_id="create_fact_table",
        postgres_conn_id='postgres_conn',
        sql=etl.fact_table_sql
    )

    # Получение данных из источников

    get_links_task = PythonOperator(
        task_id="get_links",
        python_callable=parser.get_links
    )

    get_flats_task = PythonOperator(
        task_id="get_flats",
        python_callable=parser.get_flats,
        execution_timeout=timedelta(hours=1)
    )

    get_coords_task = PythonOperator(
        task_id='get_coords',
        python_callable=etl.enrich_coords
    )

    # Трансформация

    get_urls_task =  PythonOperator(
        task_id='get_urls',
        python_callable=etl.get_urls,
    )

    transform_to_dims_task = PythonOperator(
        task_id='transform_to_dims',
        python_callable=etl.create_flat_dims
    )

    transform_to_facts_task = PythonOperator(
        task_id='transform_to_facts',
        python_callable=etl.create_flat_facts

    )

    # Загрузка

    load_dims_task = PostgresOperator(
        task_id="create_dims",
        postgres_conn_id='postgres_conn',
        sql='dims.sql'
    )

    load_fact_table = PostgresOperator(
        task_id="create_facts",
        postgres_conn_id='postgres_conn',
        sql='facts.sql'
    )
    
    clear_data_task >> get_links_task >> [get_flats_task, create_dims_task, create_fact_table] >> get_coords_task >> get_urls_task >> [transform_to_dims_task, transform_to_facts_task] >> load_dims_task >> load_fact_table


flats()