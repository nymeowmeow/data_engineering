import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, StringType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    print ('loading song data')
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    print ('creating songs table')
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs_table"), mode="overwrite", partitionBy=["year", "artist_id"])

    print ('creating artists table')
    # extract columns to create artists table
    artists_table = df.select("artist_id", \
                              col("artist_name").alias("name"), \
                              col("artist_location").alias('location'),  \
                              col("artist_latitude").alias("latitude"), \
                              col("artist_longitude").alias("longitude")) \
                      .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists_table"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print ("loading log data json")
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    print ('creating users table')
    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), \
                            col("firstName").alias("first_name"), \
                            col("lastName").alias('last_name'), \
                            'gender', \
                            'level') \
                     .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users_table"), mode="overwrite")

    print ('creating time table')
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: datetime.fromtimestamp(t/1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts")) 
        
    # extract columns to create time table
    time_table = df.select(col("timestamp").alias("start_time"), \
                   hour(col("timestamp")).alias("hour"), \
                   dayofmonth(col("timestamp")).alias("day"), \
                   weekofyear(col("timestamp")).alias("week"), \
                   month(col("timestamp")).alias("month"), \
                   year(col("timestamp")).alias("year"), \
                   dayofweek(col("timestamp")).alias("weekday") \
                  ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table"), mode="overwrite", partitionBy=["year", "month"])

    print ('creating songplays table')
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs_table"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title) \
                        .withColumn("songplay_id", monotonically_increasing_id()) \
                        .select("songplay_id", \
                                col("timestamp").alias("start_time"), \
                                col("userId").alias("user_id"), \
                                "level",
                                "song_id", \
                                "artist_id", \
                                col("sessionId").alias("session_id"), \
                                "location", \
                                col("UserAgent").alias("user_agent"), \
                                year("timestamp").alias("year"), \
                                month("timestamp").alias("month") \
                        ).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays_table"), mode="overwrite", partitionBy=["year", "month"] )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-project-west/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
