import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka")\
        .option("subscribe", "nd.project.crimestats")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 100) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()


    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # example call_date_time string: '2018-12-26T13:32:00.000'
    with_timestamp = service_table.withColumn('datetime', psf.to_timestamp(service_table.call_date_time, "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    
    


    # TODO select original_crime_type_name and disposition
    distinct_table = with_timestamp\
        .select("original_crime_type_name","disposition", "datetime") \
        .withWatermark("datetime", "60 minutes")


    '''
    # count the number of original crime type
    agg_df = distinct_table.agg(psf.approx_count_distinct("original_crime_type_name"))


    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream \
        .format("console") \
        .queryName("myAwesomeQuery") \
        .outputMode('Complete') \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/chkpnt") \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()
    '''

    
    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)


    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    #print(radio_code_df.show())
    
    # TODO join on disposition column
    join_query = distinct_table.join(radio_code_df,"disposition")
  
    
    count_query = join_query \
        .groupBy("original_crime_type_name", psf.window("datetime", "60 minutes")) \
        .count() \
        .sort("original_crime_type_name", "window")
    

    
    query = count_query.writeStream \
        .format("console") \
        .queryName("pdb") \
        .outputMode('Complete') \
        .trigger(processingTime="10 seconds") \
        .option("truncate", "false") \
        .start()
        
        #.option("checkpointLocation", "/tmp/chkpnt") \

    # TODO attach a ProgressReporter
    query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")
    
    spark.sparkContext.setLogLevel("WARN")

    run_spark_job(spark)

    spark.stop()
