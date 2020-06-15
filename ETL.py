import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def read_song_data(spark, input_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json' #'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    return df
    

def process_song_data(spark, song_df, output_data):

    # extract columns to create songs table
    songs_table = song_df.select(["song_id","title","duration","year","artist_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data+'songs.parquet', partitionBy=["year","artist_id"])
    
    print("(1/2): songs.parquet completed")

    # extract columns to create artists table
    artists_table = song_df.select(["artist_id","artist_location","artist_name"])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artist.parquet')
     
    print("(2/2): artist.parquet completed")

    
    
def process_log_data(spark, song_df, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-12-events.json'#"log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select(["userId","firstName","gender","lastName","level","location"])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users.parquet')
    
    print("(1/3): users.parquet completed")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x)/1000),TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp("ts"))
    
    
    # extract columns to create time table
    log_df = log_df.withColumn("hour",hour("start_time"))\
                .withColumn("month",dayofmonth("start_time"))\
                .withColumn("week", weekofyear("start_time"))\
                .withColumn("month", month("start_time"))\
                .withColumn("year",year("start_time"))\
                .withColumn("weekday",dayofweek("start_time"))
                
    time_table = log_df.select("ts","start_time","hour", "month", "week", "year","weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'time.parquet')
    
    print("(2/3): time.parquet completed")

    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table = log_df.join(song_df, log_df.song == song_df.title, how='inner').select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId"),col("level"),col("song_id"),col("artist_id"),col("sessionId"),col("location"),col("userAgent"),log_df.year,col("month"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'songplays.parquet')
    
    print("(3/3): songplays.parquet completed")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://karen-vargas-bucket/data_lake/"
    
    song_df = read_song_data(spark, input_data)
    process_song_data(spark, song_df, output_data)    
    process_log_data(spark, song_df, input_data, output_data)


if __name__ == "__main__":
    main()

