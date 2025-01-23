import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window


def geo_preprocess(df):
    return df.withColumn('lat', F.regexp_replace('lat', ',', '.'))\
                .withColumn('lat', F.col('lat').cast(FloatType()))\
                .withColumn('lng', F.regexp_replace('lng', ',', '.'))\
                .withColumn('lng', F.col('lng').cast(FloatType()))\
                .withColumnRenamed('lat', 'lat_city')\
                .withColumnRenamed('lng', 'lng_city')


def get_city(events, geo):
    radius = 6371
    distance = 2 * F.lit(radius) * F.asin(F.sqrt(
        F.pow(F.sin((F.radians(F.col('lat_msg')) - F.radians(F.col('lat_city'))) / 2), 2) +
        F.cos(F.radians(F.col('lat_msg'))) * F.cos(F.radians(F.col('lat_city'))) * 
        F.pow(F.sin((F.radians(F.col('lon_msg')) - F.radians(F.col('lng_city'))) / 2), 2)))
    window = Window().partitionBy('event.message_from').orderBy(F.col('distance').asc())
    events_w_city = events.crossJoin(geo).withColumn('distance', distance)\
                            .withColumn('row_num', F.row_number().over(window))\
                            .filter(F.col('row_num') == 1)\
                            .drop('row_num').persist()
    return events_w_city


def get_actual_address(events):
    window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
    event_w_actual = events.withColumn('row_num', F.row_number().over(window))\
                            .filter(F.col('row_num') == 1)\
                            .drop('row_num')
    return event_w_actual


def get_travel_info(events):
    window = Window().partitionBy('event.message_from', 'id').orderBy(F.col('date'))
    events_travel = events.withColumn('dense_rank', F.dense_rank().over(window))\
                    .withColumn('datediff', F.datediff(F.col('date').cast(DateType()), 
                                                       F.to_date(F.col("dense_rank").cast("string"), 'dd')))\
                    .selectExpr('datediff', 'user_id', 'date', 'id', 'city')\
                    .groupBy('user_id', 'datediff', 'id', 'city')\
                    .agg(F.countDistinct(F.col('date')).alias('count_days'))
    return events_travel


def get_home_address(travels):
    window = Window().partitionBy('user_id')
    user_home_city = travels.filter((F.col('count_days') >= 27))\
                        .withColumn('max_date', F.max(F.col('datediff')).over(window))\
                        .where(F.col('max_date') == F.col('datediff'))\
                        .selectExpr('user_id', 'city as home_city')
    return user_home_city


def get_travel_array(travels):
    travel_array_df = events.groupBy('user_id').agg(F.collect_list('city').alias('travel_array'))\
                .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))
    return travel_array_df


def get_local_time(events):
    local_time_events = events.withColumn('time', F.col('event.datetime').cast('Timestamp'))\
                                .withColumn('local_time', F.from_utc_timestamp(F.col('event.datetime'), 'Australia/Sydney'))\
                                .select('user_id', 'city', 'time', 'local_time')
    return local_time_events