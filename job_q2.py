import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.window import Window

import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()

from FlightRadar24.api import FlightRadar24API

def generate_fligt(spark, api):
    flights = fr_api.get_flights()

    for flight in flights:
        try:
            details = fr_api.get_flight_details(flight.id)
            flight.set_flight_details(details)
        except:
            print('cannot extract details for this flight : ',flight)

    rdd_flights =spark.sparkContext.parallelize(flights)


    rdd_flights = rdd_flights.map(lambda x: (x.get_altitude(), x.get_ground_speed(), x.id, x.airline_name if hasattr(x, 'airline_name') else None ))

    df_flights = rdd_flights.toDF(["altitude","speed","id", 'airline'])
    df_flights = df_flights.filter(df_flights.airline != 'N/A').na.drop()
    return df_flights

def most_active_company_by_continent(spark, df_flights, api):

    schema = StructType([StructField('id', StringType(), True), StructField('continent', StringType(), True)])



    df_flight_by_continents = spark.createDataFrame([], schema)


    continents = ['asia', 'europe', 'northamerica', 'africa', 'southamerica', 'oceania']
    spark.createDataFrame([], schema)
    for continent in continents:
        zone = fr_api.get_zones()[continent]
        bounds = fr_api.get_bounds(zone)
        flight_by_continent = fr_api.get_flights(bounds = bounds)
        rdd_flight_by_continent =spark.sparkContext.parallelize(flight_by_continent)
        rdd_flight_by_continent = rdd_flight_by_continent.map(lambda x: (x.id, continent))
        df_flight_by_continent = rdd_flight_by_continent.toDF(["id", 'continent'])
        df_flight_by_continent = df_flight_by_continent.na.drop()
        df_flight_by_continents = df_flight_by_continents.union(df_flight_by_continent)

    flight_airline_contient = df_flights.join(df_flight_by_continents, on = 'id', how = 'inner')

    flight_airline_contient = flight_airline_contient.groupBy(['continent','airline']).count()


    windowSpec  = Window.partitionBy("continent")

    flight_airline_contient = flight_airline_contient.withColumn("max", F.max(F.col("count")).over(windowSpec))
    flight_airline_contient = flight_airline_contient.filter(F.col('count') == F.col('max'))



    return flight_airline_contient





 
if __name__ == "__main__":
    fr_api = FlightRadar24API()
    df_flights = generate_fligt(spark, fr_api)
    flight_airline_contient = most_active_company_by_continent(spark, df_flights, fr_api)
    flight_airline_contient.show()



    
