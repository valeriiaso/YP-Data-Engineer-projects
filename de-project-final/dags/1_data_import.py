import logging
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

from dags.lib.loader_stg import load_currencies, load_transactions
from dags.lib.writer_stg import write_data, delete_files


pg_conn_id = 'pg_conn_id'
pg_connection = PostgresHook(pg_conn_id).get_conn()

vertica_conn_id = 'vertica_conn_id'
vertica_connection = VerticaHook(vertica_conn_id=vertica_conn_id).get_conn()

path_transactions = '/data/transactions'
path_currencies = '/data/currencies'

logger = logging.getLogger(__name__)

with DAG('load_stg_from_postgres',
         schedule_interval='@daily',
         start_date = pendulum.datetime(2022, 10, 1),
         end_date=pendulum.datetime(2022, 10, 31),
         catchup=True,
         tags=['final_project', 'stg', 'postgres', 'vertica'],
         is_paused_upon_creation=True) as dag:
    
    transactions_load = PythonOperator(task_id='load_transactions',
                                       python_callable=load_transactions,
                                       op_kwargs={'pg_conn': pg_connection,
                                                  'date': '{{ ds }}',
                                                  'path': path_transactions},
                                        dag=dag)
    
    logger.info('Transaction data has been retrieved')
    
    currencies_load = PythonOperator(task_id='load_currencies',
                                     python_callable=load_currencies,
                                     op_kwargs={'pg_conn': pg_connection,
                                                'date': '{{ ds }}',
                                                'path': path_currencies},
                                     dag=dag)
    
    logger.info('Currencies data has been retrived')
    
    transactions_insert = PythonOperator(task_id='insert_transactions',
                                         python_callable=write_data,
                                         op_kwargs={'vertica_conn': vertica_connection,
                                                    'date': '{{ ds }}',
                                                    'path': path_transactions,
                                                    'data_type': 'transactions'},
                                         dag=dag)
    
    logger.info('Transaction data has been inserted')
    
    currencies_insert = PythonOperator(task_id='insert_currencies',
                                       python_callable=write_data,
                                       op_kwargs={'vertica_conn': vertica_connection,
                                                  'date': '{{ ds }}',
                                                  'path': path_currencies,
                                                  'data_type': 'currencies'},
                                        dag=dag)
    
    logger.info('Currencies data has been inserted')

    remove_files = PythonOperator(task='delete_files',
                                  python_callable=delete_files,
                                  op_kwargs={'path_transactions': path_transactions,
                                             'path_currencies': path_currencies,
                                             'date': '{{ ds }}'},
                                    dag=dag)
    
    logger.info('Files were delted')
    
    transactions_load >> currencies_load >> transactions_insert >> currencies_insert >> remove_files