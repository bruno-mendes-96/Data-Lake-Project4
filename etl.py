import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, from_unixtime, dayofweek

def create_spark_session():
    """
    Create SparkSession

    Returns:
        spark: SparkSession
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Load song_data from a S3, transform the data and create dim_artists
    and dim_song parquet tables.

    Args:
        spark (SparkSession): SparkSession with follow config:
            spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0;
        input_data (str): String with the path of data directory;
        output_data (str): String with the path to output data directory;
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')\
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write\
        .partitionBy('year', 'artist_id')\
        .parquet(output_data + 'dim_songs/', 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
        .dropDuplicates()\
        .withColumnRenamed('artist_name', 'name')\
        .withColumnRenamed('artist_location', 'location')\
        .withColumnRenamed('artist_latitude', 'latitude')\
        .withColumnRenamed('artist_longitude', 'longitude')

    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + 'dim_artists/', 'overwrite')


def process_log_data(spark, input_data, output_data):
    """  
    Load log_data and song_data from a S3, transform the data 
    and create dim_users, dim_time and fact_songplays parquet tables.

    Args:
        spark (SparkSession): SparkSession with follow config:
            spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0;
        input_data (str): String with the path of data directory;
        output_data (str): String with the path to output data directory;
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    max_ts_user_df = df.groupBy('userId')\
        .max('ts')\
        .withColumnRenamed('max(ts)', 'max_ts')\
        .withColumnRenamed('userId', 'user_id')

    mask = (df.userId == max_ts_user_df.user_id) & (df.ts == max_ts_user_df.max_ts)

    users_table = df.join(max_ts_user_df, mask, how='inner')\
        .select('userId', 'firstName', 'lastName', 'gender', 'level')\
        .withColumnRenamed('userId', 'user_id')\
        .withColumnRenamed('firstName', 'first_name')\
        .withColumnRenamed('lastName', 'last_name')

    # write users table to parquet files
    users_table.write.parquet(output_data + 'dim_users/', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x/1000)))
    df = df.withColumn('ts_ms', get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.withColumn('datetime', from_unixtime(df.ts_ms))\
        .withColumn('hour', hour(col('datetime')))\
        .withColumn('day', dayofmonth(col('datetime')))\
        .withColumn('week', weekofyear(col('datetime')))\
        .withColumn('month', month(col('datetime')))\
        .withColumn('year', year(col('datetime')))\
        .withColumn('weekday', dayofweek(col('datetime')))\
        .withColumnRenamed('datetime', 'start_time')\
        .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'dim_time/', 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'

    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name), how='left')\
        .withColumn('datetime', from_unixtime(df.ts_ms))\
        .withColumn("songplay_id", monotonically_increasing_id())\
        .select('songplay_id', 'datetime', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')\
        .withColumnRenamed('datetime', 'start_time')\
        .withColumnRenamed('userId', 'user_id')\
        .withColumnRenamed('sessionId', 'session_id')\
        .withColumnRenamed('userAgent', 'user_agent')\
        .withColumn('year', year(col('start_time')))\
        .withColumn('month', month(col('start_time')))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'fact_songplays/', 'overwrite')
        

def main():
    """
    Execute ETL:
        Load the json files from S3 bucket, transform the data and load
    this data into S3 following the described data model.
    """

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
