import actions
import pendulum
from datetime import timedelta
import requests

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 9, tz="UTC"),
    catchup=False,
    template_searchpath='data/sql/',
    tags=["example"],
    on_failure_callback=actions.send_fail,
    on_success_callback=actions.send_success
)
def flats_avito_test():

    # def start():
    #     pass

    # create_dim_metro = PostgresOperator(
    #     task_id="create_dim_metro",
    #     postgres_conn_id='postgres_conn',
    #     sql=actions.create_dim_metro
    # )

    # create_dim_flat_params = PostgresOperator(
    #     task_id="create_dim_flat_params",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_dim_flat_params
    # )

    # create_dim_house = PostgresOperator(
    #     task_id="create_dim_house",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_dim_house
    # )

    # create_dim_geo = PostgresOperator(
    #     task_id="create_dim_geo",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_dim_geo
    # )

    # start_task = PythonOperator(
    #     task_id="start",
    #     python_callable=start
    # )

    # create_fact_flat = PostgresOperator(
    #     task_id="create_fact_flat",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_fact_table
    # )

    # get_links_task = PythonOperator(
    #     task_id="get_links",
    #     python_callable=actions.get_links,
    #     execution_timeout=timedelta(minutes=10)
    # )

    # extract_flats_task = PythonOperator(
    #     task_id="extract_flats_task",
    #     python_callable=actions.extract_new_flats,
    #     execution_timeout=timedelta(minutes=15)
    # )

    # transform_flats_task = PythonOperator(
    #     task_id="transform_flats_task",
    #     python_callable=actions.transform_new_flats,
    # )

    # save_db_task = PythonOperator(
    #     task_id="save_db_task",
    #     python_callable=actions.save_db
    # )

    # load_metro_task = PostgresOperator(
    #     task_id="load_metro_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='metro.sql'
    # )

    # load_flat_params = PostgresOperator(
    #     task_id="load_flat_params",
    #     postgres_conn_id='postgres_conn',
    #     sql='flat_params.sql'
    # )

    # load_house_task = PostgresOperator(
    #     task_id="load_house_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='house.sql'
    # )

    # load_fact_task = PostgresOperator(
    #     task_id="load_flat_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='flat.sql'
    # )

    # clear_orphans_task = PythonOperator(
    #     task_id="clear_orphans_task",
    #     python_callable=actions.clear_orphans
    # )

    # update_house_task = PostgresOperator(
    #     task_id="update_house_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='dim_house.sql'
    # )

    # update_flat_params_task = PostgresOperator(
    #     task_id="update_flat_params_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='dim_flat_params.sql'
    # )

    # update_metro_task = PostgresOperator(
    #     task_id="update_metro_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='dim_metro.sql'
    # )

    get_nogeo_task = PythonOperator(
        task_id="get_nogeo_task",
        python_callable=actions.get_nogeo
    )

    load_geo = PostgresOperator(
        task_id="load_geo",
        postgres_conn_id="postgres_conn",
        sql="geo.sql",
    )

    get_geo_task = PythonOperator(
        task_id="get_geo",
        python_callable=actions.get_geo
    )

    # start_task >> [create_dim_flat_params, create_dim_house, create_dim_metro, create_dim_geo] >> create_fact_flat >> get_links_task >> \
    # extract_flats_task >> transform_flats_task >> save_db_task >> [load_metro_task, load_flat_params, load_house_task, load_fact_task] >> clear_orphans_task >> [update_house_task, update_flat_params_task, update_metro_task] >> 
    get_nogeo_task >> get_geo_task >> load_geo


flats_avito_test()