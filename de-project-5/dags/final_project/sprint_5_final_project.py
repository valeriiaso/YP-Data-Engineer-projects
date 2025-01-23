import logging
import pendulum
import requests

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from lib import ConnectionBuilder
from final_project.pg_saver_couriers import PgSaverC
from final_project.pg_saver_deliveries import PgSaverD
from final_project.couriers_loader import CouriersLoader
from final_project.deliveries_loader import DeliveriesLoader
from final_project.dm_couriers_loader import CourierLoader
from final_project.dm_deliveries_loader import DeliveriesLoaderDm


log = logging.getLogger()

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 10, 21, tz='UTC'),
    catchup=False,
    tags=['sprint5', 'final project', 'stg'],
    is_paused_upon_creation=True
)
def sprint5_final_project_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    nickname = "valeriya-strelnikova"
    headers = {
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": nickname,
            "X-Cohort": "19"
        }

    
    @task(task_id="stg_couriers_load")
    def load_couriers():
        pg_saver = PgSaverC()

        sort_field = '_id'
        sort_direction = 'asc'
        limit = 50
        offset = 0
        
        while True:
            url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
            get_couriers = requests.get(url, headers=headers)
            if get_couriers.status_code == 200:
                print('Response successful')
                print(offset, len(get_couriers.json()), url)
                if len(get_couriers.json()) == 0:
                    print('Reading from the API is completed')
                    break
                else:
                    data = get_couriers.json()
                    loader = CouriersLoader(dwh_pg_connect, pg_saver, log)
                    loader.run_copy(data)
                    offset += limit
            else:
                print(f"Request failed with status code {get_couriers.status_code}")
                break
        

    @task(task_id="stg_deliveries_load")
    def load_deliveries():
        pg_saver = PgSaverD()

        sort_field = 'order_ts'
        sort_direction = 'asc'
        limit = 50
        offset = 0
        restaurant_id = ''
        fr = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        while True:
            url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={restaurant_id}&from={fr}&to={to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
            get_deliveries = requests.get(url, headers=headers)
            if get_deliveries.status_code == 200:
                print('Response successful')
                if len(get_deliveries.json()) == 0:
                    print('Reading from API is completed')
                    break
                else:
                    data = get_deliveries.json()
                    loader = DeliveriesLoader(dwh_pg_connect, pg_saver, log)
                    loader.run_copy(data)
                    offset += limit
            else:
                print(f'Request failed with status code {get_deliveries.status_code}')
                break

    
    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        c_loader = CourierLoader(dwh_pg_connect, log)
        c_loader.load_courier()

    
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        d_loader = DeliveriesLoaderDm(dwh_pg_connect, log)
        d_loader.load_delivery()
        

    couriers_loader = load_couriers()
    deliveries_loader = load_deliveries()
    couriers_dim = load_dm_couriers()
    deliveries_dim = load_dm_deliveries()


    couriers_loader >> deliveries_loader >> couriers_dim >> deliveries_dim


final_project_dag = sprint5_final_project_dag()
    