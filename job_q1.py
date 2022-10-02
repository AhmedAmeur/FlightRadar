import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()

from FlightRadar24.api import FlightRadar24API

def company_with_most_active_flight(spark,api):
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
    df_flights.show()

    df_flights_max = df_flights.groupBy('airline').count()


    your_max_value = df_flights_max.agg({"count": "max"}).collect()[0][0]

    df_flights_max = df_flights_max.filter(F.col('count') == your_max_value)

    return df_flights_max





 
if __name__ == "__main__":
    fr_api = FlightRadar24API()
    df_flights_max = company_with_most_active_flight(spark,fr_api)
    df_flights_max.show()
    

    
