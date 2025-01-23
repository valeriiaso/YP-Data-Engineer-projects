from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import pendulum
import vertica_python
import pandas as pd

conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023121116',       
             'password': '7Zokis3pieao3MD',
             'database': 'dwh',
             'autocommit': True
}


@dag(schedule_interval=None, start_date=pendulum.parse('2024-01-23'))
def sprint6_project_read_csv():

    @task(task_id='read_csv')
    def read_csv():

        log = pd.read_csv('/data/group_log.csv')
        group_log_df = pd.DataFrame(log)
        group_log_df['user_id'] = pd.array(group_log_df['user_id'], dtype='int64')
        group_log_df['user_id_from'] = pd.array(group_log_df['user_id_from'], dtype='int64')

        group_log_df.to_csv('/data/log_data.csv', sep=',', index=False)

    
    @task(task_id='load_csv')
    def load_csv():
        query = """copy STV2023121116__STAGING.group_log(group_id,user_id,user_id_from,event,"datetime")
                    from local '/data/log_data.csv'
                    delimiter ',';"""
        
        # conn = vertica_python.connect(**conn_info)
        # cursor = conn.cursor()
        # cursor.execute(query)
        # cursor.commit()
        
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)

    
    read_group_log = read_csv()
    copy_group_log = load_csv()

    read_group_log >> copy_group_log


upload_data_project_dag = sprint6_project_read_csv()


