from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import functions as F
from pyspark.sql.functions import concat,lit,col,avg,count,when
from pyspark.sql.functions import to_timestamp
import os


from pyspark.sql import SparkSession
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/afnan/toronto-ride-share/dags/config/fetch-from-gcs.json"
spark = SparkSession.builder.appName('abc').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
bucket = "toronto-ride-share-files"
spark.conf.set('temporaryGcsBucket', bucket)



def read_parquet_files_from_gcs():
    file_location = "gs://toronto-ride-share-files/rides/*.parquet"
    file_type = "parquet"

    df = spark.read.format(file_type).load(file_location)

    return df

def read_csv_files_from_gcs():
    file_location = "gs://toronto-ride-share-files/weather/*.csv"
    file_type = "csv"

    # CSV options
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","

    # The applied options are for CSV files. For other file types, these will be ignored.
    weather_df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)


    return weather_df


def transform_weather(weather_df):
    cols = ['Station Name','Date/Time','Max Temp (°C)','Min Temp (°C)','Mean Temp (°C)','Total Precip (mm)','Snow on Grnd (cm)']
    weather_df = weather_df.select(*cols)

    weather_df = weather_df.withColumn("Max Temp (°C)", weather_df["Max Temp (°C)"].cast(FloatType()))
    weather_df = weather_df.withColumn("Min Temp (°C)", weather_df["Min Temp (°C)"].cast(FloatType()))
    weather_df = weather_df.withColumn("Total Precip (mm)", weather_df["Total Precip (mm)"].cast(FloatType()))
    weather_df = weather_df.withColumn("Snow on Grnd (cm)", weather_df["Min Temp (°C)"].cast(IntegerType()))
    weather_df = weather_df.withColumn('Date', to_date(weather_df['Date/Time'],'yyyy-MM-dd'))

    weather_df = weather_df.withColumnRenamed("Station Name", "Station_Name")\
       .withColumnRenamed("Date/Time", "Date_Time")\
        .withColumnRenamed("Max Temp (°C)","Max_Temp_Degree_Celsius")\
        .withColumnRenamed("Min Temp (°C)","Min_Temp_Degree_Celsius")\
        .withColumnRenamed("Mean Temp (°C)","Mean_Temp_Degree_Celsius")\
        .withColumnRenamed("Total Precip (mm)","Total_Precip_mm")\
        .withColumnRenamed("Snow on Grnd (cm)","Snow_on_Grnd_cm")\
        .drop("Date_Time")

    weather_df.write.format('bigquery') \
    .option('table', 'ride_share.weather') \
    .save()
    print("weather records written")
    
    return weather_df

def transform_rides_data(rides_df):
    df_with_times = rides_df.withColumn("Start Time New", concat(col("Start Time"),lit(":00") ))
    df_with_times = df_with_times.withColumn("End Time New", concat(col("End Time"),lit(":00") ))


    df_formatted_time = df_with_times.withColumn("Start Time New", to_timestamp(df_with_times['Start Time New'], 'MM/dd/yyyy HH:mm:ss'))
    df_formatted_time = df_formatted_time.withColumn("End Time New", to_timestamp(df_with_times['End Time New'], 'MM/dd/yyyy HH:mm:ss'))
    df_with_date_paritioned_column = df_formatted_time.withColumn('Year', year(to_date(df_formatted_time['Start Time New'])))
    df_with_date_paritioned_column = df_formatted_time.withColumn('Start Date', to_date(df_formatted_time['Start Time New']))

    df_with_date_paritioned_column = df_with_date_paritioned_column.withColumnRenamed("Trip Id", "Trip_Id")\
       .withColumnRenamed("Trip  Duration", "Trip_Duration")\
        .withColumnRenamed("Start Station Id","Start_Station_Id")\
        .withColumnRenamed("Start Station Name","Start_Station_Name")\
        .withColumnRenamed("Start Time New","Start_Time")\
        .withColumnRenamed("End Station Id","End_Station_Id")\
        .withColumnRenamed("End Time New","End_Time")\
        .withColumnRenamed("End Station Name","End_Station_Name")\
        .withColumnRenamed("Bike Id","Bike_Id")\
        .withColumnRenamed("User Type","User_Type")\
        .withColumnRenamed("Start Date","Start_Date")\
        .drop("Start Time")\
        .drop("End Time")

    df_with_date_paritioned_column.write.format('bigquery') \
  .option('table', 'ride_share.rides_fact') \
  .save()
    print("rides records written")

    return df_with_date_paritioned_column

    


def aggregate_data(rides_df,weather_df):
    df_agg = rides_df.groupBy("Start_Date").agg(
                                                         avg("Trip_Duration").alias("Mean_Trip_Time"),
                                                         count(when(col('User_Type') == 'Casual Member',col('Trip_Id'))).alias('Casual_Member_Trips'),
                                                         count(when(col('User_Type') == 'Annual Member',col('Trip_Id'))).alias('Annual_Member_Trips'),
                                                         count("Trip_Id") .alias("Total_Trips")
                                                         )

    df_joined = df_agg.join(weather_df,df_agg['Start_Date'] ==  weather_df['Date'],"left")
    df_joined.select('Start_Date','Mean_Trip_Time','Casual_Member_Trips','Annual_Member_Trips','Total_Trips','Max_Temp_Degree_Celsius','Min_Temp_Degree_Celsius','Mean_Temp_Degree_Celsius','Total_Precip_mm','Snow_on_Grnd_cm')
    
    df_joined.write.format('bigquery') \
  .option('table', 'ride_share.rides_agg') \
  .save()
    print("aggregated records written")

def main():
    rides_df = read_parquet_files_from_gcs()
    weather_df = read_csv_files_from_gcs()
    rides_df = transform_rides_data(rides_df)
    weather_df = transform_weather(weather_df)
    aggregate_data(rides_df,weather_df)

main()


    


