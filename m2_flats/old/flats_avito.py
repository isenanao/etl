import actions
import pendulum
from datetime import timedelta
import generate_test
import parse

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
def flats_avito_vault():

    # def holder():
    #     pass

    # holder_task = PythonOperator(
    #     task_id="holder",
    #     python_callable=holder
    # )

    # create_hubs = PostgresOperator(
    #     task_id="create_hubs",
    #     postgres_conn_id='postgres_conn',
    #     sql=actions.create_hubs
    # )

    # create_metrics_table_task = PostgresOperator(
    #     task_id="create_metrics_table_task",
    #     postgres_conn_id='postgres_conn',
    #     sql=actions.create_metrics
    # )

    # create_shap_table_task = PostgresOperator(
    #     task_id="create_shap_table_task",
    #     postgres_conn_id='postgres_conn',
    #     sql=actions.create_shap_table
    # )

    # create_satelites = PostgresOperator(
    #     task_id="create_satelites",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_satelites
    # )

    # create_links = PostgresOperator(
    #     task_id="create_links",
    #     postgres_conn_id='postgres_conn',
    #     sql= actions.create_links
    # )

    # get_links_task = PythonOperator(
    #     task_id="get_links",
    #     python_callable=parse.get_links,
    #     execution_timeout=timedelta(minutes=10)
    # )

    # extract_flats_task = PythonOperator(
    #     task_id="extract_flats_task",
    #     python_callable=parse.get_flats,
    #     execution_timeout=timedelta(minutes=15)
    # )

    # check_data_task = PythonOperator(
    #     task_id="check_data_task",
    #     python_callable=actions.check_data,
    # )

    # create_dup_sql_task = PythonOperator(
    #     task_id="create_dups_task",
    #     python_callable=actions.load_dups,
    # )

    # update_links_task = PythonOperator(
    #     task_id="update_links_task",
    #     python_callable=actions.update_links,
    # )

    # load_new_data_task = PythonOperator(
    #     task_id="load_new_data_task",
    #     python_callable=actions.load_new_data,
    # )

    # create_hub_sql = PythonOperator(
    #     task_id="create_hub_sql",
    #     python_callable=actions.load_hubs,
    # )

    # create_sat_sql = PythonOperator(
    #     task_id="create_sat_sql",
    #     python_callable=actions.load_sats,
    # )

    # create_link_sql = PythonOperator(
    #     task_id="create_link_sql",
    #     python_callable=actions.load_links,
    # )

    # load_hubs = PostgresOperator(
    #     task_id="load_hubs_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='hubs.sql'
    # )

    # load_sats = PostgresOperator(
    #     task_id="load_sats_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='sats.sql'
    # )

    # load_links = PostgresOperator(
    #     task_id="load_links_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='links.sql'
    # )

    # load_dups_task = PostgresOperator(
    #     task_id="load_dups_task",
    #     postgres_conn_id='postgres_conn',
    #     sql='dups.sql'
    # )

    # clear_data_task = PythonOperator(
    #     task_id="clear_data_task",
    #     python_callable=actions.clear_data,
    # )

    get_metro_price = PythonOperator(
        task_id = "metro_price_task",
        python_callable=actions.get_metro_price
    )

    get_shap_values_task = PythonOperator(
        task_id = "get_shap_values_task",
        python_callable=actions.get_importances
    )

    load_metrics_task = PostgresOperator(
        task_id="load_metrics_task",
        postgres_conn_id='postgres_conn',
        sql='metrics.sql'
    )

    #clear_data_task >> [create_metrics_table_task, create_shap_table_task, create_hubs, create_links, create_satelites] >> get_links_task >> extract_flats_task >> check_data_task >> create_dup_sql_task >> update_links_task >> load_dups_task  >> load_new_data_task >> [create_hub_sql, create_sat_sql, create_link_sql] >> load_hubs >> load_sats >> load_links >>
    get_metro_price >> get_shap_values_task >> load_metrics_task


flats_avito_vault()