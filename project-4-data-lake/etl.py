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

access_id = os.environ['AWS_ACCESS_KEY_ID']
access_key = os.environ['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file

    song_data = f"{input_data}song-data/A/A/A/*.json"
    # songs_out = "{out_data}songs".format(out_data=output_data)
    songs_out = f"{output_data}songs"
    # artists_out = "{out_data}artists".format(out_data=output_data)
    artists_out = f"{output_data}artists"

    # read song data file
    print(f"loading the df {song_data}")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    print("writing songs table")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(songs_out)

    # extract columns to create artists table
    artists_table = df.selectExpr("NVL(artist_id, 0) as artist_id", "NVL(artist_name, '') AS artist_name",
                                  "NVL(artist_location, '') AS artist_location", "NVL(artist_latitude, 0) AS artist_latitude",
                                  "NVL(artist_longitude, 0) AS artist_longitude")

    print("printing - artists schema")
    artists_table.printSchema()

    # ['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year']

    print("writing artists table")
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(artists_out)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/"
    #song_data = f"{input_data}song_data/A/A/A/"
    song_data = f"{output_data}songs"
    users_out = f"{output_data}artists"
    timetable_out = f"{output_data}time"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    #users_table.write.mode('append').parquet(users_out)

    # create timestamp column from original timestamp column
    #get_timestamp = udf(ts, TimestampType())
    df = df.selectExpr("*", "ts as start_time")

    # create datetime column from original timestamp column
    #get_datetime = udf(dt, DateType())
    df = df.selectExpr("*", "FROM_UNIXTIME(ts/1000) as start_date")

    # extract columns to create time table
    time_table = df.selectExpr(
        "start_time", "hour(start_date) as hour", "day(start_date) as day",
        "weekofyear(start_date) as week", "month(start_date) as month",
        "year(start_date) as year", "dayofweek(start_date) as weekday", "artist as artist_id")

    print("count time table: ")
    time_table.count()
    # write time table to parquet files partitioned by year and month
    #time_table.write.partitionBy("year", "artist_id").mode('append').parquet(timetable_out)

    # read in song data to use for songplays table
    artists_out = "{out_data}artists".format(out_data="s3n://awsds/")

    artists_df = spark.read.parquet(artists_out)
    song_df = spark.read.parquet(song_data)

    songs_join = df.join(song_df, df.song == song_df.title)
    songs_join.createOrReplaceTempView("songs_join")
    artists_df.createOrReplaceTempView("artists_df")
    songplays_table = spark.sql("""
                            SELECT 
                                ts as start_time, userId AS user_id, level,
                                song_id, artists.artist_id, sessionId AS session_id, 
                                location, userAgent AS user_agent 
                            FROM songs_join songs
                            Inner Join artists_df artists
                            ON artists.artist_id = songs.artist_id
                    """)
    #songplays = artists_df.join(songs_join, artists_df.artist_id == song_df.artist_id, how='inner')

    #songplays.printSchema()

    # extract columns from joined song and log datasets to create songplays table
    # songplays_table = songplays.selectExpr(
    #     "ts as start_time", "userId AS user_id", "level",
    #     "song_id", "artist_id.artist_id", "sessionId AS session_id", "location", "userAgent AS user_agent"
    # )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.printSchema()


def main():
    spark = create_spark_session()
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

    input_data = "s3n://udacity-dend/"
    #output_data = "s3n://udacity-dend-out/"
    output_data = "s3n://awsds-out/"

    print("starting process data")
    process_song_data(spark, input_data, output_data)
    print("starting log_data")
    process_log_data(spark, input_data, output_data)
    print("ending log_data")

if __name__ == "__main__":
    main()
