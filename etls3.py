import pyspark.sql.functions as F

import configparser
from helper import *
import boto3


config = configparser.ConfigParser()
config.read('dl-s3.cfg')

aws_key             = config.get("AWS","key")
aws_secret          = config.get("AWS","secret")
input_bucket_name   = config.get("S3","input_bucket") 
songs_path          = config.get("S3","songs_prefix") 
logs_path           = config.get("S3","logs_prefix")

os.environ['AWS_ACCESS_KEY_ID']        = aws_key
os.environ['AWS_SECRET_ACCESS_KEY']    = aws_secret

print(input_bucket_name)
print(songs_path)
print(logs_path)

def process_song_data(spark, songs_df, output_location):
  
    # extract columns to create songs table
    songs_table = songs_df.select([
        songs_df.song_id,
        songs_df.title,
        songs_df.artist_id,
        songs_df.year,
        songs_df.duration
    ])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.distinct()
    songs_table.show(2)
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_location+'/songs.parquet')
    

    # extract columns to create artists table
    artists_table = songs_df.select([
        songs_df.artist_id,
        songs_df.artist_name,
        songs_df.artist_location.alias("location"),
        songs_df.artist_longitude.alias("longitude"),
        songs_df.artist_latitude.alias("latitude")
    ])
    
    artists_table = artists_table.distinct()
    artists_table.show(2)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_location+'/artists.parquet')


def process_log_data(spark, logs_df, songs_df, output_location):  
    # filter by actions for song plays
    logs_df = logs_df.filter("page = 'NextSong'")

    # extract user data 
    users_table = logs_df.select([
        logs_df.userId.alias("user_id"),
        logs_df.firstName.alias("first_name"),
        logs_df.lastName.alias("last_name"),
        logs_df.gender,
        logs_df.level
    ])
    
    users_table = users_table.distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_location+'/users.parquet')
    users_table.show(2)

    # build time fact table
    time_df = logs_df.select(getDateTime(logs_df.ts).alias("timestamp"))
    time_table = time_df.select([
        time_df.timestamp.alias("start_time"),
        F.hour(time_df.timestamp).alias("hour"),
        F.year(time_df.timestamp).alias("year"),
        F.dayofmonth(time_df.timestamp).alias("day"),
        F.month(time_df.timestamp).alias("month"),
        F.dayofweek(time_df.timestamp).alias("weekday")
    ])
    
    time_table = time_table.distinct()
    time_table.show(2)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_location+'/time.parquet')

    # extract columns from joined song and log datasets to create songplays table
    songs_df.createOrReplaceTempView("songs")
    logs_df.createOrReplaceTempView("logs")
    spark.udf.register("getDateTime", getDateTime)
    
    songplays_table = spark.sql("""select
        getDateTime(l.ts) as start_time, l.userId as user_id,l.level,
        s.song_id, s.artist_id, l.location, l.userAgent as user_agent
        from logs as l  left outer join songs as s
        on s.artist_name = l.artist
        and s.title = l.song
        and s.duration = l.length
    """)
    
    songplays_table = songplays_table.withColumn("year",
                            F.year(songplays_table.start_time))
    
    songplays_table = songplays_table.withColumn("month",
                            F.year(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_location+'/song_plays.parquet')


def main():
    spark = create_spark_session()
    
    # output data
    output_path = config.get("S3","output")

    print("reading data ... \n")
    
    songs_sch = get_songs_staging_schema()
    songs_file_list = get_s3_files(aws_key, aws_secret, input_bucket_name, songs_path,".json")
    print("songs file count {}".format(len(songs_file_list)))
    songs_df = spark.read.schema(songs_sch).json(songs_file_list)
    
    logs_sch = get_logs_staging_schema()
    logs_file_list = get_s3_files(aws_key, aws_secret, input_bucket_name, logs_path,".json")
    print("logs file count {}".format(len(logs_file_list)))
    logs_df = spark.read.schema(logs_sch).json(logs_file_list)
    

    print("processing songs ... \n")
    process_song_data(spark,songs_df,output_path)
    
    print("processing logs ... \n")
    process_log_data(spark, logs_df,songs_df,output_path)
    
    print("etl complete")

if __name__ == "__main__":
    main()
