import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import row_number , lit
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t



config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS_ACCESS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = " "
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("SongTable")
    
    # extract columns to create artists table
    artists_table = df.selectExpr(artist_id, "artist_name as name" , " artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude" )

    # write artists table to parquet files
    artists_table.write.parquet("Artists_Table")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = ' '

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter((f.col('page')=='NextSong'))

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name" , " lastName as  last_name", "gender", "level" )
    
    # write users table to parquet files
    users_table.write.parquet("Users_Table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp (df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime (df.ts))

   # create weekday column from original timestamp column
    get_datetime =  udf(lambda x:  datetime.fromtimestamp(x/1000) .strftime('%A'))
    df= df.withColumn("weekday", get_weekday (df.ts))
    
    # extract columns to create time table
    time_table = df.select("timestamp",
    hour("timestamp").alias('hour'), 
    dayofmonth("timestamp").alias('day'), 
    weekofyear("timestamp").alias('weekofyear'), 
    month("timestamp").alias('month'), 
    year("timestamp").alias('year'), 
   "weekday"
)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet("time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(" ")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.alias('a').join(df.alias('b'),(col('b.song') == col('a.title')) & (col('b.artist') == col('a.artist_name') ) )\
.selectExpr("ts as start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "useragent as user_agent").distinct() 
    w = Window().orderBy(lit('A'))
    songplays_table = songplays_table.withColumn("songplay_id", row_number().over(w))
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet("songplays_table")


def main():
    spark = create_spark_session()
    input_data = " "
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
