import sys
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
from p_tools import geo_preprocess, get_city, get_local_time


def main():
    geo_events_path = sys.argv[1]
    geo_data_path = sys.argv[2]
    output_path = sys.argv[3]

    date = datetime.date.today()

    conf = SparkConf().setAppName(f"UsersRecommendation-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df_geo = sql.read.csv(geo_data_path, sep=';', inferSchema=True, header=True)

    df_geo = geo_preprocess(df_geo)

    geo_events = sql.read.parquet(geo_events_path)\
                    .withColumn('user_id', F.col('event.message_from'))\
                    .withColumnRenamed('lat', 'lat_msg')\
                    .withColumnRenamed('lon', 'lon_msg')\
                    .withColumn('event_id', F.monotonically_increasing_id())
    
    events = get_city(geo_events, df_geo)

    window = Window().partitionBy(F.col('user_id')).orderBy(F.col('event.message_ts').desc())
    last_viewed = events.where('lat_msg is not null')\
                    .withColumn('row_num', F.row_number().over(window))\
                    .filter(F.col('row_num') == 1)\
                    .selectExpr('user_id', 'lat_msg as lat', 'lon_msg as lon', 'id')
    
    events_w_local = get_local_time(events)

    events_w_city = events_w_local.selectExpr('user_id', 'local_time')

    last_viewed = last_viewed.join(events_w_city, 'user_id', 'inner')

    last_viewed_channel = events.select(F.col('event.subscription_channel').alias('channel'), 
                                    F.col('event.user').alias('user_id')).distinct()
    
    new = last_viewed_channel.join(last_viewed_channel.withColumnRenamed('user_id', 'user_id_2'), 
                                     'channel', 'inner')\
                                .filter('user_id < user_id_2')
    
    all_pair_users = new.join(last_viewed, 'user_id', 'inner')\
                    .withColumnRenamed('lat', 'lat_1')\
                    .withColumnRenamed('lon', 'lon_1')\
                    .drop('city').drop('local_time')
    
    all_pair_users = all_pair_users.join(last_viewed.drop('id'), last_viewed['user_id'] == all_pair_users['user_id_2'], 'inner')\
                        .drop(last_viewed['user_id'])\
                        .withColumnRenamed('lat', 'lat_2')\
                        .withColumnRenamed('lon', 'lon_2')
    
    pair_users_dist = all_pair_users.withColumn('distance', (2 * F.lit(6371) * F.asin(F.sqrt(
        F.pow(F.sin((F.radians(F.col("lat_1")) - F.radians(F.col("lat_2"))) / 2), 2) + 
        F.cos(F.radians(F.col("lat_1"))) * F.cos(F.radians(F.col("lat_2"))) * 
        F.pow(F.sin((F.radians(F.col("lon_1")) - F.radians(F.col("lon_2"))) / 2), 2)))).cast(FloatType()))\
        .filter(F.col('distance') <= 1)
    
    pairs_msg = events.select(F.col('event.message_from').alias('sender'), 
                         F.col('event.message_to').alias('receiver'))
    
    pairs = pairs_msg.join(pair_users_dist, [pairs_msg.sender == pair_users_dist.user_id, 
                                        pairs_msg.sender == pair_users_dist.user_id_2])\
                    .filter('user_id < user_id_2')\
                    .select('user_id', 'user_id_2', 'id', 'local_time')
    
    pair_users_dist = pair_users_dist.select('user_id', 'user_id_2', 'id', 'local_time')
    users_recommendation = pair_users_dist.withColumn('processed_dttm', F.current_timestamp())\
                                        .join(pairs, [pair_users_dist.user_id == pairs.user_id,
                                                   pair_users_dist.user_id_2 == pairs.user_id_2], 'left_anti')\
                                        .filter('user_id < user_id_2').distinct()\
                                        .selectExpr('user_id as user_left', 'user_id_2 as user_right', 
                                                    'processed_dttm', 'id as zone_id', 'local_time')
    
    users_recommendation.write.parquet(f'{output_path}/date={date}')


if __name__ == "__main__":
    main()