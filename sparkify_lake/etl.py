import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import from_unixtime


def create_spark_session():
    """Creates a spark session and returns a spark object"""

    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Process song files
    - Transform data to create songs and artists tables
    - Write to parquet files
    """

    # get filepath to song data file
    song_data = "{0}song_data/*/*/*/*.json".format(input_data)

    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    query = """select song_id, artist_id, year, duration
    from song_data_table
    """

    songs_table = spark.sql(query)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        output_data + "songs/songs_table.parquet"
    )

    # extract columns to create artists table
    query = """select artist_id, artist_name, artist_latitude, artist_longitude
    from song_data_table
    """

    artists_table = spark.sql(query)

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(
        output_data + "artists/artists_table.parquet"
    )


def process_log_data(spark, input_data, output_data):
    """
    - Process log files
    - Transform data to create songplays, users and time tables
    - Write to parquet files
    """

    # get filepath to log data file
    log_data = "{0}log_data/".format(input_data)

    # uncomment line below if reading from udacity S3 data; comment out the line above
    # log_data = '{0}log-data/*/*/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)

    get_timestamp = udf(lambda x: int(x / 1000), IntegerType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    df = df.withColumn("datetime", from_unixtime("start_time"))
    df.createOrReplaceTempView("log_data_table")

    # extract columns from joined song and log datasets to create songplays table
    query = """with songplay_data as
    (
        select e.userId as user_id, 
            s.song_id as song_id, 
            s.artist_id as artist_id, 
            e.sessionid as session_id, 
            e.start_time as start_time, 
            e.level as level, 
            e.location as location, 
            e.userAgent as user_agent,
            month(e.datetime) as month,
            year(e.datetime) as year
 
        from log_data_table e
        join song_data_table s
        on s.title = e.song 
            and s.artist_name = e.artist
                and round(s.duration) = round(e.length)
        where page = 'NextSong'
    ) 
    select row_number() over (order by "monotonically_increasing_id") as songplay_id, * 
    
    from songplay_data
    """

    songplays_table = spark.sql(query)
    songplays_table.createOrReplaceTempView("songplays_data_table")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        output_data + "songplays/songplays_table.parquet"
    )

    # extract columns for users table

    query = """with user_data (
        select userId, firstName, lastName, gender, level, 
            row_number() over (partition by userId order by ts desc) as obs_num
        from log_data_table
        where page = 'NextSong'
    )
    select userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level,
        obs_num
    from user_data
    where obs_num = 1
    """
    users_table = spark.sql(query)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(
        output_data + "users/users_table.parquet"
    )

    # extract columns to create time table
    query = """
    with time_data as
    (
        select distinct start_time, from_unixtime(start_time) as datetime
        from songplays_data_table
    )
    select start_time,
        hour(datetime) as hour,
        dayofyear(datetime) as day,
        weekofyear(datetime) as week,
        month(datetime) as month,
        year(datetime) as year,
        dayofweek(datetime) as weekday
    from time_data
    """

    time_table = spark.sql(query)
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        output_data + "time/time_table.parquet"
    )


def main():
    """Call functions to process song and events data"""

    spark = create_spark_session()

    # Uncomment to use udacity S3 data
    # input_data = "s3a://udacity-dend/"

    # Comment out line below if using udacity S3 data as input
    input_data = "s3n://sparkify-music-lake/"
    output_data = "s3n://sparkify-music-lake/"

    # Uncomment lines below to process data locally
    # input_data = "../data/"
    # output_data = "../data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
