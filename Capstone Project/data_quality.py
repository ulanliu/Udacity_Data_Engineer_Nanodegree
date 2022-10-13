import os
import configparser
from pathlib import Path
from pyspark.sql import SparkSession
import boto3

config = configparser.ConfigParser()
config.read('capstone.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession.builder\
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                    .enableHiveSupport().getOrCreate()

dir_list = ['immigration', 'person', 'country_code', 'demographics', 'state_code', 'temperature', 'transportation_code', 'visa_code']

for my_dir in dir_list:
    df = spark.read.parquet(config['S3']['DEST_S3_BUCKET'] + my_dir + '/*')
    record_num = df.count()
    if record_num <= 0:
        raise ValueError("This table is empty!")
    else:
        print("Table: " + my_dir + f" is not empty: total {record_num} records.")
