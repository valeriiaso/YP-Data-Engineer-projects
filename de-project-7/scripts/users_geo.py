import sys
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window
from p_tools import geo_preprocess, get_city, get_actual_address, get_travel_info, get_home_address, get_travel_array, get_local_time


def main():

    geo_events_path = sys.argv[1]
    geo_data_path = sys.argv[2]
    output_path = sys.argv[3]

    date = datetime.date.today()

    conf = SparkConf().setAppName(f"UsersGeoData-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df_geo = sql.read.csv(geo_data_path, sep=';', inferSchema=True, header=True)

    geo_events = sql.read.parquet(geo_events_path)\
                    .where("event_type == 'message'")\
                    .withColumn('user_id', F.col('event.message_from'))\
                    .withColumnRenamed('lat', 'lat_msg')\
                    .withColumnRenamed('lon', 'lon_msg')\
                    .withColumn('event_id', F.monotonically_increasing_id())
    
    df_geo = geo_preprocess(df_geo)
    
    events = get_city(geo_events, df_geo)

    events_act_city = get_actual_address(events)

    act_city = events_act_city.selectExpr('user_id', 'id as city_id','city as act_city')

    travels = get_travel_info(events)

    travels_array = get_travel_array(travels)

    user_home_city = get_home_address(travels)

    events_local_time = get_local_time(events_act_city)

    df_users = events.join(act_city, 'user_id', 'left')\
                    .join(travels_array, 'user_id', 'left')\
                    .join(user_home_city, 'user_id', 'left')\
                    .join(events_local_time, 'user_id', 'left')\
                    .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
    
    df_users.write.parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()
