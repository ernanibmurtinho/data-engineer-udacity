import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file

    song_data = "{input_data}song-data/*/*/*/*.json"
    # songs_out = "{out_data}songs".format(out_data=output_data)
    songs_out = "{out_data}songs".format(out_data="s3n://awsds/")
    # artists_out = "{out_data}artists".format(out_data=output_data)
    artists_out = "{out_data}artists".format(out_data="s3n://awsds/")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('append').parquet(songs_out)

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")

    # ['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year']

    # write artists table to parquet files
    artists_table.write.mode('append').parquet(artists_out)


def process_log_data(spark, input_data, output_data="s3n://awsds/"):
    # get filepath to log data file
    log_data = "{input_data}log_data/*/*/"
    song_data = "{input_data}song_data/*/*/"
    users_out = "{output_data}artists"
    timetable_out = "{output_data}time"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("user_id", "first_name", "last_name", "gender", "level")

    # write users table to parquet files
    users_table.write.mode('append').parquet(users_out)

    # create timestamp column from original timestamp column
    get_timestamp = udf(ts, TimestampType())
    df = df.selectExpr("*, get_timestamp(start_time) as start_time")

    # create datetime column from original timestamp column
    get_datetime = udf(dt, DateType())
    df = df.selectExpr("*, get_datetime(dt) as start_date")

    # extract columns to create time table
    time_table = df.selectExpr(
        "start_time, hour(start_date) as hour, day(start_date) as day, "
        "weekofyear(start_date) as week, month(start_date) as month, "
        "year(start_date) as year, weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "artist_id").mode('append').parquet(timetable_out)

    # read in song data to use for songplays table
    artists_out = "{out_data}artists".format(out_data="s3n://awsds/")

    artists_df = spark.read.json(artists_out)
    song_df = spark.read.json(song_data)

    songs_join = df.join(song_df, df.song == song_df.title)
    songplays = artists_df.join(songs_join, artists_df.name == song_df.artist, how='inner')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplays.selectExpr(
        "songplay_id, ts as start_time, userId AS user_id, level,"
        "song_id, artist_id, sessionId AS session_id, location, userAgent AS user_agent"
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-out/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
