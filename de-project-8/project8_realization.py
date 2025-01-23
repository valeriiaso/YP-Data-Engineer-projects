import psycopg2
import logging

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# создание таблицы subscribers_feedback
config = {'database': 'de',
          'user': 'jovyan',
          'password': 'jovyan',
          'host': 'localhost',
          'port': 5432}

conn = psycopg2.connect(**config)

cursor = conn.cursor()
query = """
CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
"""
cursor.execute(query)


# код основного приложения

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

kafka_options = {'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
                 'kafka.security.protocol': 'SASL_SSL',
                 'kafka.sasl.jaas.config': 
                            'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
                'kafka.sasl.mechanism': 'SCRAM-SHA-512'}

jdbc_options = {'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
                'driver': 'org.postgresql.Driver',
                'dbtable': 'subscribers_restaurants',
                'user': 'student',
                'password': 'de-student'}

postgresql_out = {'url': 'jdbc:postgresql://localhost:5432/de',
                  'driver': 'org.postgresql.Driver',
                  'dbtable': 'public.subscribers_feedback',
                  'user': 'jovyan',
                  'password': 'jovyan'}

topic_in = 'project8_valeriiast_in'
topic_out = 'project8_valeriiast_out'

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def create_spark_session(app_name) -> SparkSession:
    spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

    return spark


def read_kafka_stream(spark):
    kafka_stream_df = spark.readStream \
                    .format('kafka') \
                    .options(**kafka_options)\
                    .option("startingOffsets", "earliest") \
                    .option('subscribe', topic_in) \
                    .load()
    
    return kafka_stream_df


def get_current_timestamp_utc():
    current_time = datetime.now()
    current_timestamp_utc = int(round(current_time.timestamp()))

    return current_timestamp_utc


def transform_and_filter_stream_data(df, current_timestamp_utc, incoming_schema):
    filtered_df = df.select(from_json(col('value').cast('string'), incoming_schema)\
                                    .alias('parsed_value'))\
                            .select('parsed_value.*')\
                            .where((col('adv_campaign_datetime_start') < current_timestamp_utc) & 
                                   (col('adv_campaign_datetime_end') > current_timestamp_utc))
    
    return filtered_df


def read_subscribers_data(spark, jdbc_config):
    subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .options(**jdbc_config) \
                    .load()
    
    return subscribers_restaurant_df


def join_and_transform(kafka_df, postgresql_df):
    result_df = kafka_df.join(postgresql_df, 'restaurant_id', 'inner')\
                                    .withColumn('trigger_datetime_created', unix_timestamp(current_timestamp()))\
                                    .drop('id')
    
    return result_df


def foreach_batch_function(df, epoch_id):
   
    df_persist = df.persist()
    
    df_persist = df_persist.withColumn('feedback', lit(None).cast(StringType()))
    
    try:
        df_persist.write.format('jdbc') \
                        .mode('append')\
                        .options(**postgresql_out)\
                        .save()
    except Exception as e:
        logger.error(f"Error occured while writing to PostgreSQL: {str(e)}")
    
    df_persist = df_persist.drop('feedback')
    
    try:
        df_persist = df_persist.withColumn('value', to_json(struct(col('restaurant_id'), col('adv_campaign_id'),
                                                                col('adv_campaign_content'), col('adv_campaign_owner'),
                                                                col('adv_campaign_owner_contact'), col('adv_campaign_datetime_start'),
                                                                col('adv_campaign_datetime_end'), col('datetime_created'),
                                                                col('client_id'), col('trigger_datetime_created'))))\
                                .select('value')
        
        df_persist.write\
                .format('kafka')\
                .options(**kafka_options)\
                .option('topic', topic_out)\
                .save()
    except Exception as e:
        logger.error(f"Error occured while writing to Kafka: {str(e)}")

    df_persist.unpersist()


def save_to_postgresql_and_kafka(df):
    df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


if __name__ == "__main__":
    spark = create_spark_session("RestaurantSubscribeStreamingService_project8")

    restaurant_read_stream_df = read_kafka_stream(spark)

    current_timestamp_utc = get_current_timestamp_utc()

    incoming_message_schema = StructType([StructField('restaurant_id', StringType(), True),
                                       StructField('adv_campaign_id', StringType(), True),
                                       StructField('adv_campaign_content', StringType(), True),
                                       StructField('adv_campaign_owner', StringType(), True),
                                       StructField('adv_campaign_owner_contact', StringType(), True),
                                       StructField('adv_campaign_datetime_start', LongType(), True),
                                       StructField('adv_campaign_datetime_end', LongType(), True),
                                       StructField('datetime_created', LongType(), True)])

    filtered_data = transform_and_filter_stream_data(restaurant_read_stream_df, current_timestamp_utc, incoming_message_schema)
    
    subscribers_data = read_subscribers_data(spark, jdbc_options)
    
    result_df = join_and_transform(filtered_data, subscribers_data)

    save_to_postgresql_and_kafka(result_df)
    
    spark.stop() 

