import configparser
import os
from pyspark.sql import SparkSession


class JobDataLakeIngestion:
    """
    Class responsible to make the inheritance
    of the process methods you need to run
    """

    def __init__(self, pex_file=None):
        self.submit_args = "--conf spark.submit.deployMode=cluster --conf spark.driver.maxResultSize=0 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.driver.memory=8G --conf spark.io.compression.snappy.blockSize=65536 --conf spark.sql.catalogImplementation=hive --conf spark.yarn.dist.files=datalakeingestion.pex --conf spark.executorEnv.PEX_ROOT=./.pex --conf spark.app.name='Datalake ingestion - process' --conf spark.submit.deployMode=client --conf spark.sql.shuffle.partitions=8 --conf spark.default.parallelism=8 --conf spark.driver.memoryOverhead=2g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' --conf spark.executor.memoryOverhead=2g --conf spark.memory.fraction=0.8 --conf spark.dynamicAllocation.enabled=false --driver-cores 2 --executor-cores 2 --executor-memory 8G pyspark-shell"
        self.pex_file = pex_file

    def process_log_data(self, spark, input_data, output_data):
        """
        Method that process the songs files and write the following tables:
        1) Users
        2) Time
        3) Songplays
        """
        # defining filepaths to the log_data, song_data and users_data and the file for output

        # get filepath to log data file
        log_data = f"{input_data}log_data/*/*/"
        song_data = f"{output_data}songs"
        users_out = f"{output_data}users"
        timetable_out = f"{output_data}time"
        songplaystable_out = f"{output_data}songplays"

        # read log data file
        df = spark.read.json(log_data)

        # filter by actions for song plays
        df = df.filter(df.page == "NextSong")

        # extract columns for users table
        users_table = df.select("userId", "firstName", "lastName", "gender", "level")

        # write users table to parquet files
        users_table.write.mode('overwrite').parquet(users_out)

        # create timestamp column from original timestamp column
        df = df.selectExpr("*", "ts as start_time")

        # create datetime column from original timestamp column
        df = df.selectExpr("*", "FROM_UNIXTIME(ts/1000) as start_date")

        # extract columns to create time table
        time_table = df.selectExpr(
            "start_time", "hour(start_date) as hour", "day(start_date) as day",
            "weekofyear(start_date) as week", "month(start_date) as month",
            "year(start_date) as year", "dayofweek(start_date) as weekday", "artist as artist_id")

        # write time table to parquet files partitioned by year and month
        time_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(timetable_out)

        # read in song data to use for songplays table
        artists_out = f"{output_data}artists"

        artists_df = spark.read.parquet(artists_out)
        song_df = spark.read.parquet(song_data)

        songs_join = df.join(song_df, df.song == song_df.title)
        songs_join.createOrReplaceTempView("songs")
        artists_df.createOrReplaceTempView("artists")

        # uncoment to see the df struct
        #songs_join.printSchema()
        #artists_df.printSchema()

        songplays_table = spark.sql("""
                                SELECT 
                                    ts as start_time, userId AS user_id, level,
                                    song_id, artists.artist_id, sessionId AS session_id, 
                                    location, userAgent AS user_agent,
                                    year(start_date) as year,
                                    month(start_date) as month
                                FROM songs songs
                                Inner Join artists
                                ON artists.artist_id = songs.artist_id
                        """)

        # write songplays table to parquet files partitioned by year and month
        songplays_out = songplays_table.select("start_time", "user_id", "level", "song_id", "artist_id", "session_id",
                                               "location", "user_agent", "year", "month")
        #uncoment to see the df struct
        #songplays_out.printSchema()
        songplays_out.write.partitionBy("year", "month").mode('overwrite').parquet(songplaystable_out)


    def process_song_data(self, spark, input_data, output_data):
        """
        Method that process the songs files and write the following tables:
        1) Songs
        2) Artists
        """
        # defining filepaths to the song_data and output files

        song_data = f"{input_data}song-data/*/*/*/*.json"
        songs_out = f"{output_data}songs"
        artists_out = f"{output_data}artists"

        # read song data file
        df = spark.read.json(song_data)

        songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

        # writing songs table to parquet, partitioned by year and artist_id
        songs_table.write.partitionBy("year", "artist_id").mode('append').parquet(songs_out)

        # getting the columns for artists table from song_data
        artists_table = df.selectExpr("NVL(artist_id, 0) as artist_id", "NVL(artist_name, '') AS artist_name",
                                      "NVL(artist_location, '') AS artist_location",
                                      "NVL(artist_latitude, 0) AS artist_latitude",
                                      "NVL(artist_longitude, 0) AS artist_longitude")

        # writing the artists table
        artists_table.write.mode('overwrite').parquet(artists_out)

    def init_config_aws(self):
        """
        This method reads the config file, defined below
        you need to put the file on the root of this project
        the file example is on the readme file
        """
        config = configparser.ConfigParser()
        config.read('dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']

        access_id = os.environ['AWS_ACCESS_KEY_ID']
        access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        return access_id, access_key

    def create_spark_session(self):
        """
        This method creates the sparkSession with the pre defined
        parameters especified below
        """
        os.environ["PYSPARK_SUBMIT_ARGS"] = self.submit_args
        spark = SparkSession \
            .builder \
            .appName('Datalake ingestion - process') \
            .config("spark.submit.deployMode", "client") \
            .config("spark.executorEnv.PEX_ROOT", "./.pex") \
            .config("spark.yarn.dist.files", self.pex_file) \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()

        return spark

    def run(self):
        access_id, access_key = self.init_config_aws()
        spark = self.create_spark_session()

        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

        input_data = "s3n://udacity-dend/"
        output_data = "s3n://udacity-dend-out/"

        print("starting process data")
        self.process_song_data(spark, input_data, output_data)
        print("starting log_data")
        print("ending log_data")
