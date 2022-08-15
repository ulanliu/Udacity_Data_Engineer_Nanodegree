import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, TimestampType

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


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/A/A/*/*.json')
    
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', FloatType()),
        StructField('artist_longitude', FloatType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', FloatType()),
        StructField('year', IntegerType())
    ])
    # read song data file
    df = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(os.path.join(output_data + 'songs'))

    # extract columns to create artists table
    artist_fields = ["artist_id", "artist_name AS name", "artist_location AS location", "artist_latitude AS latitude",
                      "artist_longitude AS longitude"]
    artists_table = df.selectExpr(artist_fields).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data + 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/*/*.json'

    log_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('first_name', StringType()),
        StructField('gender', StringType()),
        StructField('item_in_session', IntegerType()),
        StructField('last_name', StringType()),
        StructField('length', FloatType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', IntegerType()),
        StructField('session_id', IntegerType()),
        StructField('song', StringType()),
        StructField('status', StringType()),
        StructField('ts', IntegerType()),
        StructField('user_agent', StringType()),
        StructField('user_id', IntegerType()),
    ])
    # read log data file
    df = spark.read.json(log_data, schema= log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data + 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('start_time', get_datetime(df.timestamp))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('year', year(df.start_time))
    df = df.withColumn('weekday', dayofweek(df.start_time))
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data + 'time'), mode='overwrite', partitionBy =['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data + 'song_data/A/A/*/*.json'))
    songplays_df = song_df.join(df, (song_df.title == df.song) & (song_df.artist_name == df.artist))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_df.select('start_time', 'user_id', 'level', 'song_id', \
                                            'artist_id', 'session_id','location','user_agent', \
                                            df.year.alias('year'), 'month') \
                                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data + 'songplays'), mode='overwrite', partitionBy =['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
