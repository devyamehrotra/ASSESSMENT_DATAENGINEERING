import logging.config
import folium as folium
from pyspark.sql import Window
from pyspark.sql.functions import to_timestamp, concat_ws,col,avg
from pyspark.sql.types import StructType, StructField ,FloatType

from pyspark.sql.types import StringType
import udf as UDF
logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Ingest')

"""
THIS IS INGEST CLASS
HERE ALL THE TRANSFORMATIONS ARE CARRIED OUT

"""

#function to save the final output file
def output_csv(df,outputpath):
    #assessment 8 Please include the final csv in the repository.
    try:

        df.write.option("header", True).option("sep", ",").mode('overwrite').csv(outputpath)
        logger.info("DONE WRITING")

    except Exception as e:
        logger.error("exception as ", str(e))


#function to display df
def display_df(df):
    df_show = df.show()

    return df_show


#function to perform all tthe transformations
def transform(df):

    #2.Convert the Date and Time columns into a timestamp column named Timestamp.
    df_with_updated = df.withColumn("Timestamp", to_timestamp(concat_ws(" ", df["Date"], df["Time"]), "MM/dd/yyyy HH:mm:ss"))
    logging.info("assessmnet 2")
    display_df(df_with_updated)



    # 3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0.

    filter_df = df_with_updated.filter(col("Magnitude") > 5.0)
    logging.info("assessmnet 3")
    display_df(filter_df)


    #4th4.Calculate the average depth and magnitude of earthquakes for each earthquake type.

    window_spec = Window.partitionBy("Type")
    average_df = filter_df.withColumn(
        "Average_Magnitude", avg("Magnitude").over(window_spec)
    ).withColumn(
        "Average_Depth", avg("Depth").over(window_spec)
    ).distinct()

    logging.info("assessmnet 4")
    display_df(average_df)

    #5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.
    levels_df = UDF.levels(average_df)

    #displaying df of  assessment 5
    logging.info("assessmnet 5")
    display_df(levels_df)

    #6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).

    distance_df  = UDF.distance(levels_df)

    # displaying df of  assessment 6
    logging.info("assesment6 ")
    display_df(distance_df)

    # 7 .Visualize the geographical distribution of earthquakes on a world map using appropriate
    selected_columns = ["Latitude", "Longitude", "Depth", "Magnitude", "Type","Distance_From_Reference","ID"]
    df_selected = distance_df.select(*selected_columns)

    # mean coordinates to set the initial center of the map
    mean_latitude = df_selected.agg({"Latitude": "mean"}).collect()[0][0]
    mean_longitude = df_selected.agg({"Longitude": "mean"}).collect()[0][0]

    # Folium map centered
    earthquake_map = folium.Map(location=[mean_latitude, mean_longitude], zoom_start=2)

    #  markers for each earthquake to the map
    for row in df_selected.collect():
        popup_text = f"ID: {row['ID']}<br>Depth: {row['Depth']} km<br>Distance from Reference: {row['Distance_From_Reference']:.2f} km"
        folium.Marker([row["Latitude"], row["Longitude"]], popup=popup_text).add_to(earthquake_map)

    # SaveD MAP AS HTML FILE
    earthquake_map.save("output/earthquake_map.html")

    return distance_df




# FUNCTION WHERE THE FILE IS READ AND PASSED ON TO TRANSFORMATION


def load(spark ,file_dir,file_format,header,inferSchema):
    global df
    custom_schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Latitude", FloatType(), True),
        StructField("Longitude", FloatType(), True),
        StructField("Type", StringType(), True),
        StructField("Depth", FloatType(), True),
        StructField("Magnitude", FloatType(), True),
        StructField("ID", StringType(), True),
    ])

    try:
        logging.warning('load files method started ..................')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format(file_format) \
                .schema(custom_schema) \
                .load(file_dir)

    except Exception as e:
        logging.error("An error occured at load FILE =>", str(e))

    else:
        logging.warning("data frame created successfully of format {} ".format(file_format))


    transform_df = transform(df)


    return transform_df







