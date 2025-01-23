import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import datetime
 
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'
 
default_args = {
'owner': 'valeriiast',
'start_date': datetime.datetime(2024, 1, 31),
}
 
dag_spark = DAG(
dag_id = "project_sprint_7",
default_args=default_args,
schedule_interval="@daily",
)

users_geo_data = SparkSubmitOperator(
    task_id='users_geo_data',
    dag=dag_spark,
    application='/home/valeriiast/users_geo.py',
    conn_id='yarn_spark',
    application_args = ['/user/master/data/geo/events', '/user/valeriiast/geo.csv', '/user/valeriiast/data/analytics/users_data_geo'],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

geo_count_data = SparkSubmitOperator(
    task_id='geo_count_data',
    dag=dag_spark,
    application='/home/valeriiast/geo_count_data.py',
    conn_id='yarn_spark',
    application_args = ['/user/master/data/geo/events', '/user/valeriiast/geo.csv', '/user/valeriiast/data/analytics/geo_events_count'],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

user_recommendations = SparkSubmitOperator(
    task_id='user_recommendations',
    dag=dag_spark,
    application='/home/valeriiast/users_recommendation.py',
    conn_id='yarn_spark',
    application_args = ['/user/master/data/geo/events', '/user/valeriiast/geo.csv', '/user/valeriiast/data/analytics/recommendations'],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

users_geo_data >> geo_count_data >> user_recommendations