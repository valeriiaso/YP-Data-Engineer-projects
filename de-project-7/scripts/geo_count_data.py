import sys
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from p_tools import geo_preprocess, get_city


def main():
    geo_events_path = sys.argv[1]
    geo_data_path = sys.argv[2]
    output_path = sys.argv[3]

    date = datetime.date.today()

    conf = SparkConf().setAppName(f"GeoDataCountWeekMonth-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df_geo = sql.read.csv(geo_data_path, sep=';', inferSchema=True, header=True)

    df_geo = geo_preprocess(df_geo)

    events = sql.read.parquet(geo_events_path)\
                    .where('lat is not null')\
                    .withColumnRenamed('lat', 'lat_msg')\
                    .withColumnRenamed('lon', 'lon_msg')\
                    .withColumn('event_id', F.monotonically_increasing_id())
    
    events = get_city(events, df_geo)

    events = events.withColumn('week', F.weekofyear(F.col('date')))\
                    .withColumn('month', F.month(F.col('date')))
    
    window_week = Window().partitionBy(['week', 'city'])
    window_month = Window().partitionBy(['month', 'city'])

    events_geo_w_m = events.withColumn('week_message', 
                                    F.count(F.when(events.event_type == 'message','event_id'))\
                                        .over(window_week))\
                            .withColumn('week_reaction', 
                                    F.count(F.when(events.event_type == 'reaction','event_id'))\
                                        .over(window_week))\
                            .withColumn('week_subscription', 
                                    F.count(F.when(events.event_type == 'subscription','event_id'))\
                                        .over(window_week))\
                            .withColumn('week_user', 
                                    F.count(F.when(events.event_type == 'registration','event_id'))\
                                        .over(window_week))\
                            .withColumn('month_message', 
                                    F.count(F.when(events.event_type == 'message','event_id'))\
                                        .over(window_month))\
                            .withColumn('month_reaction', 
                                    F.count(F.when(events.event_type == 'reaction','event_id'))\
                                        .over(window_month))\
                            .withColumn('month_subscription', 
                                    F.count(F.when(events.event_type == 'subscription','event_id'))\
                                        .over(window_month))\
                            .withColumn('month_user', 
                                    F.count(F.when(events.event_type == 'registration','event_id'))\
                                        .over(window_month))\
                            .selectExpr('week', 'month', 'id as zone_id', 'week_message', 'week_reaction',
                                    'week_subscription', 'week_user', 'month_message', 'month_reaction',
                                    'month_subscription', 'month_user').distinct()
    
    events_geo_w_m.write.parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()