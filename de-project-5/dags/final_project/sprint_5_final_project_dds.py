import logging
import pendulum
import requests

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from lib import ConnectionBuilder
from final_project.dm_couriers_loader import CourierLoader
from final_project.dm_deliveries_loader import DeliveriesLoaderDm
from final_project.dm_rates_loader import RateLoader


log = logging.getLogger()

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 10, 21, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'final project', 'dds'],
    is_paused_upon_creation=True
)
def sprint5_final_project_stg_to_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        c_loader = CourierLoader(dwh_pg_connect, log)
        c_loader.load_courier()

    
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        d_loader = DeliveriesLoaderDm(dwh_pg_connect, log)
        d_loader.load_delivery()


    @task(task_id="dm_rates_load")
    def load_dm_rates():
        r_loader = RateLoader(dwh_pg_connect, log)
        r_loader.load_rate()


    couriers_dim = load_dm_couriers()
    deliveries_dim = load_dm_deliveries()
    rates_dim = load_dm_rates()


    couriers_dim >> deliveries_dim >> rates_dim


dds_final_project_dag = sprint5_final_project_stg_to_dds_dag()