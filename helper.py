import findspark

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, TimestampType
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.sql.types import IntegerType, FloatType

import os
import glob
from datetime import datetime
import boto3

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def get_s3_files(aws_key, aws_secret, bucket_name, prefix,file_ext):
    print(bucket_name,prefix,file_ext)
    s3 = boto3.resource('s3', aws_access_key_id = aws_key, 
                        aws_secret_access_key = aws_secret)
    fileList = []
    
    bucket = s3.Bucket(bucket_name)
    for element in bucket.objects.filter(Prefix = prefix):
        if file_ext in element.key:
            s3file = "/".join(["s3a:/",element.bucket_name,element.key])
            fileList.append(s3file)
    return fileList


def get_songs_staging_schema():
    schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs",LongType()),      
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", LongType())]
    )
    return schema

def get_logs_staging_schema():
    schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration",DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", LongType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())]
    )
    
    return schema


@F.udf(TimestampType())
def getDateTime(x):
    if x is None:
        return None
    return datetime.fromtimestamp(float(x/1000.)).replace(microsecond=0)


def create_spark_session():
    findspark.init('/home/kbaafi/spark')
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark








