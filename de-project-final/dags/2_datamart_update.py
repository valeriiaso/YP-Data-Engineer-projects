import logging
import pendulum
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook

from lib.loader_dwh import insert_global_metrics, insert_h_currencies, insert_h_transactions, insert_l_transaction_currency, insert_s_currencies_rates, insert_s_transactions_amount, insert_s_transactions_send_info, insert_s_transactions_status


vertcia_conn_id = 'vertica_connection'
vertica_connection = VerticaHook(vertica_conn_id=vertcia_conn_id).get_conn()

logger = logging.getLogger(__name__)

with DAG('load_dwh_from_stg',
         schedule_interval='@daily',
         start_date = pendulum.datetime(2022, 10, 1),
         end_date=pendulum.datetime(2022, 11, 1),
         catchup=True,
         tags=['final_project', 'dwh', 'vertica'],
         is_paused_upon_creation=True) as dag:
    
    source = 'postgres'
    load_dt = datetime.datetime.now()

    h_transactions = PythonOperator(task_id='insert_h_transactions',
                                    python_callable=insert_h_transactions,
                                    op_kwargs={'date': '{{ ds }}',
                                               'vertica_connection': vertica_connection,
                                               'source': source,
                                               'load_dt': load_dt},
                                    dag=dag)
    
    logger.info('Data has been inserted into table h_transactions')
    
    h_currencies = PythonOperator(task_id='insert_h_currencies',
                                    python_callable=insert_h_currencies,
                                    op_kwargs={'date': '{{ ds }}',
                                               'vertica_connection': vertica_connection,
                                               'source': source,
                                               'load_dt': load_dt},
                                    dag=dag)
    
    logger.info('Data has been inserted into table h_currencies')
    
    l_transaction_currency = PythonOperator(task_id='insert_l_transaction_currency',
                                            python_callable=insert_l_transaction_currency,
                                            op_kwargs={'date': '{{ ds }}',
                                                        'vertica_connection': vertica_connection,
                                                        'source': source,
                                                        'load_dt': load_dt},
                                            dag=dag)
    
    logger.info('Data has been inserted into table l_transaction_currency')
    
    s_currencies_rates = PythonOperator(task_id='insert_s_currencies_rates',
                                        python_callable=insert_s_currencies_rates,
                                        op_kwargs={'date': '{{ ds }}',
                                                    'vertica_connection': vertica_connection,
                                                    'source': source,
                                                    'load_dt': load_dt},
                                        dag=dag)
    
    logger.info('Data has been inserted into table s_currencies_rates')
    
    s_transactions_send = PythonOperator(task_id='insert_s_transactions_send_info',
                                        python_callable=insert_s_transactions_send_info,
                                        op_kwargs={'date': '{{ ds }}',
                                                    'vertica_connection': vertica_connection,
                                                    'source': source,
                                                    'load_dt': load_dt},
                                        dag=dag)
    
    logger.info('Data has been inserted into table s_transactions_send')
    
    s_transactions_amount = PythonOperator(task_id='insert_s_transactions_amount',
                                            python_callable=insert_s_transactions_amount,
                                            op_kwargs={'date': '{{ ds }}',
                                                    'vertica_connection': vertica_connection,
                                                    'source': source,
                                                    'load_dt': load_dt},
                                            dag=dag)
    
    logger.info('Data has been inserted into table s_transactions_amount')
    
    s_transactions_status = PythonOperator(task_id='insert_s_transactions_status',
                                            python_callable=insert_s_transactions_status,
                                            op_kwargs={'date': '{{ ds }}',
                                                    'vertica_connection': vertica_connection,
                                                    'source': source,
                                                    'load_dt': load_dt},
                                            dag=dag)
    
    logger.info('Data has been inserted into table s_transactions_status')
    
    global_metrics = PythonOperator(task_id='insert_global_metrics',
                                            python_callable=insert_global_metrics,
                                            op_kwargs={'date': '{{ ds }}',
                                                    'vertica_connection': vertica_connection},
                                            dag=dag)
    
    logger.info('Data has been inserted into table global_metrics')
    
    [h_currencies, h_transactions] >> l_transaction_currency >> [s_currencies_rates, s_transactions_send, s_transactions_amount, s_transactions_status] >> global_metrics