import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Instantiate a SparkSession object using Hadoop-AWS package
    Returns a SparkSession object
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load the song data stored in the input_data parameter into a Spark dataframe
    Create songs and artists tables based on the song_data dataframe by writing them to parquet files
    '''

    # get filepath to song data file
    # 3 subdirectories with hashed file names - need 4 wildcards for comprehensive search
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    # define temp view to query
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('song_data')

    # extract columns to create songs table - distinct rows with group by
    songs_table = spark.sql('''
    SELECT song_id,
    title,
    artist_id,
    year,
    duration
    FROM song_data
    GROUP BY 1,2,3,4,5
    ORDER BY 1
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .mode('overwrite') \
                     .parquet(os.path.join(output_data, 'songs/songs.parquet'))

    # extract columns to create artists table - distinct rows with group by
    artists_table = spark.sql('''
    SELECT artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    FROM song_data
    GROUP BY 1,2,3,4,5
    ORDER BY 1
    ''')

    
    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
                       .parquet(os.path.join(output_data, 'artists/artists.parquet'))


def process_log_data(spark, input_data, output_data):
    '''
    Load the log data stored in the input_data parameter into a Spark dataframe
    Create users, time, and songplays tables based on the log_data dataframe by writing them to parquet files
    '''
    
    # get filepath to log data file
    # 2 subdirectory with hashed file names - need 2 wildcards for comprehensive search
    log_data = os.path.join(input_data, 'log_data/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    df.createOrReplaceTempView('log_data')

    # extract columns for users table - distinct rows with group by
    users_table = spark.sql('''
    SELECT userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
    FROM log_data
    GROUP BY 1,2,3,4,5
    ORDER BY 1
    ''')
    
    # write users table to parquet files
    users_table.write.mode('overwrite') \
                     .parquet(os.path.join(output_data, 'users/users.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x)/1000)
    log_df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    # use datetime.fromtimestamp to get local time instead of utc
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))

    # recreate temp view with clean timestamp columns
    log_df.createOrReplaceTempView('log_data_ts')
    
    # extract columns to create time table - distinct rows with group by
    time_table = spark.sql('''
    SELECT datetime as start_time,
    hour(datetime) as hour,
    day(datetime) as day,
    weekofyear(datetime) as week,
    month(datetime) as month,
    year(datetime) as year,
    dayofweek(datetime) as weekday
    FROM log_data_ts
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1
    ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .mode('overwrite') \
                    .parquet(os.path.join(output_data, 'time/time.parquet'))


    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data) 

    # extract columns from joined song and log datasets to create songplays table
    # add surrogate key for songplay_id in the log_df prior to join
    # recreate temp view with new surrogate key
    log_df = log_df.withColumn('songplay_id', monotonically_increasing_id())
    log_df.createOrReplaceTempView('logs')
    song_df.createOrReplaceTempView('songs')

    # add year and month for partitioning
    songplays_table = spark.sql('''
    SELECT l.songplay_id,
    l.datetime as start_time,
    l.userId as user_id,
    l.level,
    s.song_id,
    s.artist_id,
    l.sessionId as session_id,
    l.location,
    l.userAgent as user_agent,
    year(l.datetime) as year,
    month(l.datetime) as month
    FROM logs l
    LEFT JOIN songs s on l.song = s.title and l.artist = s.artist_name
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                         .mode('overwrite') \
                         .parquet(os.path.join(output_data, 'songplays/songplays.parquet'))

def main():
 
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-joh/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()

if __name__ == "__main__":
    main()
