from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType,DoubleType
import logging.config
logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('udf')
from math import radians, sin, cos, sqrt, atan2

"""
THIS IS A UDF CLASS 
THAT CONTAINS THE DEFINATION
AND REGISTRATIONS OF
UDF FUNCTIONS

"""

#FUNCTION TO CATEGORIZE MAGNITUDE LEVELS
def categorize_magnitude(magnitude):
    if magnitude > 7.0:
        return "High"
    elif magnitude > 5.0:
        return "Moderate"
    else:
        return "Low"



#FUNCTION TO FIND DISTANCE FROM (0,0)
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of the Earth in kilometers

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance





def levels(df):
    logging.info("...............INSIDE UDF FUNCTION TO CATEGRORIZE MAGBNITUDE LEVELS.....................")

    # Register the UDF with Spark
    categorize_magnitude_udf = F.udf(categorize_magnitude, StringType())

    # Applied  the UDF to create a new column 'Magnitude_Level'
    level_df = df.withColumn("Magnitude_Level", categorize_magnitude_udf("Magnitude"))

    return level_df




def distance(df):
    logging.info("...............INSIDE distance UDF FUNCTION....................")
    reference_latitude = 0.0
    reference_longitude = 0.0

    # Register the UDF with Spark
    distance_udf = F.udf(haversine_distance, DoubleType())



    #refernce location
    df_with_distance = df.withColumn("Distance_From_Reference",
                                     distance_udf("Latitude", "Longitude", lit(reference_latitude),
                                                        lit(reference_longitude)))


    return  df_with_distance

