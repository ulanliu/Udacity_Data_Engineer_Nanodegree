from tkinter import ALL
import pandas as pd
from pyspark.rdd import Partitioner
from pyspark.sql.functions import udf, to_date
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
import configparser
from datetime import datetime
import logging

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"


# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('capstone.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def convert_sas_to_date(sas_date):
    """
    Convert SAS date format to calendar format.
    
    Parameters
    ----------
    sas_date : 
        SAS date
    """
    if sas_date is None:
        dt = None
    else:
        dt = datetime.fromordinal(datetime(1960, 1, 1).toordinal() + int(float(sas_date)))
    return dt

def code_mapper(idx):
    """
    Read the file "I94_SAS_Labels_Descriptions.SAS" and extract the content after idx and convert them into dictionary format.
    
    Parameters
    ----------
    idx : 
        The index.
    """
    with open('I94_SAS_Labels_Descriptions.SAS', 'r') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def cbp_mapping(cbp_code):
    """
    Map the CBP code with city of CBP table
    
    Parameters
    ----------
    cbp_code : 
        CBP code
    """
    cbp_code_table = code_mapper('i94prtl')
    value = cbp_code_table.get(cbp_code)
    if ',' in value:
        return value.split(',')[0].strip()
    else:
        return None
    
def cbp_mapping_2(cbp_code):
    """
    Map the CBP code with state of CBP table
    
    Parameters
    ----------
    cbp_code : 
        CBP code
    """
    cbp_code_table = code_mapper('i94prtl')
    value = cbp_code_table.get(cbp_code)
    if ',' in value:
        return value.split(',')[1].strip()
    else:
        return None

def create_spark_session():
    """
    Creates a new spark session or use the existing one.
    """
    spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()

    return spark

def rename_columns(table, new_columns):
    """
    Rename the columns of table.
    
    Parameters
    ----------
    table : 
        The table want to be modified.
    new_columns :
        The list of new column name.
    """
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table

def process_immigration_data(spark, output_path):
    """
    Processes the song data from input_data path with spark and loads them back into output_data path.
    
    Parameters
    ----------
    spark : 
        The spark session
    output_data :
        The filepath of output data
    """
    
    convert_SAS_date = udf(lambda x: convert_sas_to_date(x), DateType())
    cbp_code_mapping = udf(lambda x: cbp_mapping(x))
    cbp_code_mapping_2 = udf(lambda x: cbp_mapping_2(x))
    
    df_immi_temp = spark.read.format('csv').option("header", True).load('immigration_data_sample.csv')
    df_immi_temp = df_immi_temp.withColumn("arrival_date", convert_SAS_date("arrdate"))
    df_immi_temp = df_immi_temp.withColumn("departure_date", convert_SAS_date("depdate"))
    
    df_immi = df_immi_temp.select(['cicid', 'i94yr', 'i94mon', 'i94addr', 'i94port', 'arrival_date', \
             'i94mode', 'departure_date', 'i94visa', 'entdepa', 'dtaddto', 'visatype'])
    new_columns = ['cicid', 'year', 'month', 'arrival_state', 'cbp_code', \
                   'arrival_date', 'transportation_mode', 'departure_date', 'visa', \
                  'arrival_flag', 'expiry_date', 'visa_type']
    df_immi = rename_columns(df_immi, new_columns)

    df_immi = df_immi \
        .withColumn("port_city", cbp_code_mapping("cbp_code")) \
        .withColumn("port_state", cbp_code_mapping_2("cbp_code"))
    
    df_immi.write.parquet(os.path.join(output_path + 'immigration'), mode='overwrite', partitionBy='port_state')
    
    df_person = df_immi_temp.select(['cicid', 'i94cit', 'i94res', 'biryear', 'gender'])
    
    df_person.write.parquet(os.path.join(output_path + 'person'), mode='overwrite')
    
def process_SAS_Label_data(spark, output_path):
    """
    Process the label description data with spark and loads them back into output_data path.
    
    Parameters
    ----------
    spark : 
        The spark session
    output_data :
        The filepath of output data
    """
    country_code_table = code_mapper('i94cntyl')
    transportation_mode_table = code_mapper('i94model')
    state_code_table = code_mapper('i94addrl')
    visa_table = code_mapper('visa_code')
    
    df_country = spark.createDataFrame(country_code_table.items(), ['country_code', 'country'])
    df_country.write.parquet(os.path.join(output_path + 'country_code'), mode='overwrite')
    
    df_trans = spark.createDataFrame(transportation_mode_table.items(), ['transportation_code', 'transportation_type'])
    df_trans.write.parquet(os.path.join(output_path + 'transportation_code'), mode='overwrite')
    
    df_state = spark.createDataFrame(state_code_table.items(), ['state_abbr', 'state'])
    df_state.write.parquet(os.path.join(output_path + 'state_code'), mode='overwrite')
    
    df_visa = spark.createDataFrame(visa_table.items(), ['visa_code', 'category'])
    df_visa.write.parquet(os.path.join(output_path + 'visa_code'), mode='overwrite')

def process_demographics_data(spark, output_path):
    """
    Process the demographics data with spark and loads them back into output_data path.
    
    Parameters
    ----------
    spark : 
        The spark session
    output_data :
        The filepath of output data
    """
    df_demo = spark.read.format('csv').options(delimiter=';').option("header", True).load('us-cities-demographics.csv')
    
    new_columns = ['city', 'state', 'median_age', 'male_pop', 'female_pop', 'total_pop', 'veterans_num'\
                   , 'foreign-born', 'average_household_size', 'state_code', 'race', 'count']
    df_demo = rename_columns(df_demo, new_columns)
    df_demo.write.parquet(os.path.join(output_path + 'demographics'), mode='overwrite')
    
def process_temperature_data(spark, output_path):
    """
    Process the temperature data with spark and loads them back into output_data path.
    
    Parameters
    ----------
    spark : 
        The spark session
    output_data :
        The filepath of output data
    """
    df_temp = spark.read.format('csv').option("header", True).load('GlobalLandTemperaturesByUS_State.csv')
    df_temp = df_temp.select(to_date(df_temp.dt).alias('date'), 'AverageTemperature', 'AverageTemperatureUncertainty', 'State', 'Country')
    
    new_columns = ['date', 'avg_temp', 'avg_temp_uncertainty', 'state', 'country']
    df_temp = rename_columns(df_temp, new_columns)
    df_temp.write.parquet(os.path.join(output_path + 'temperature'), mode='overwrite')
    
def main():
    spark = create_spark_session()
    output_path = config['S3']['DEST_S3_BUCKET']
    
    process_immigration_data(spark, output_path)
    logging.info("Immigration data processing completed")
    
    process_SAS_Label_data(spark, output_path)
    logging.info("SAS label data processing completed")

    process_demographics_data(spark, output_path)
    logging.info("Demographics data processing completed")
    
    process_temperature_data(spark, output_path)
    logging.info("Temperature data processing completed")
    
if __name__ == "__main__":
    main()