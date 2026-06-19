from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import datetime
import os

spark=(
    SparkSession.builder.appName("ipl_cdc")\
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("spark://spark-master:7077") \
    .config("spark.streaming.StopGracefullyOnShutdown", "true") \
    ##aws
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID","")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY","")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate() 
)

df_match_stream=spark.readStream.format("kafka")\
                   .option("kafka.bootstrap.servers","kafka:9092")\
                   .option("subscribe","ipl_cdc.public.matches")\
                   .option("startingOffsets","latest")\
                   .load()
                   
df_deliveries_stream=spark.readStream.format("kafka")\
                        .option("kafka.bootstrap.servers","kafka:9092")\
                        .option("subscribe","ipl_cdc.public.deliveries")\
                        .option("startingOffsets","latest")\
                        .load()
                        
deliveries_schema=StructType([
    StructField("op", StringType(), True),
    StructField("source", StructType([
        StructField("table", StringType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("delivery_id", IntegerType(), True), 
        StructField("match_id", IntegerType(), True),
        StructField("inning", IntegerType(), True),
        StructField("batting_team", StringType(), True),
        StructField("bowling_team", StringType(), True),
        StructField("over", IntegerType(), True),
        StructField("ball", IntegerType(), True),
        StructField("batter", StringType(), True),
        StructField("bowler", StringType(), True),
        StructField("non_striker", StringType(), True),
        StructField("batsman_runs", IntegerType(), True),
        StructField("extra_runs", IntegerType(), True),
        StructField("total_runs", IntegerType(), True),
        StructField("extras_type", StringType(), True),
        StructField("is_wicket", IntegerType(), True),
        StructField("player_dismissed", StringType(), True),
        StructField("dismissal_kind", StringType(), True),
        StructField("fielder", StringType(), True)
    ]), True)
])

match_schema=StructType([
    StructField("op",StringType(),True),
    StructField("source",StructType([
        StructField("table",StringType(),True)
    ]),True),
    StructField("after",StructType([
        StructField("id",IntegerType(),True),
        StructField("season",StringType(),True),
        StructField("city",StringType(),True),
        StructField("match_date", IntegerType(), True),  
        StructField("match_type", StringType(), True),
        StructField("player_of_match", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("team1", StringType(), True),
        StructField("team2", StringType(), True),
        StructField("toss_winner", StringType(), True),
        StructField("toss_decision", StringType(), True),
        StructField("winner", StringType(), True),
        StructField("result", StringType(), True),
        StructField("result_margin", IntegerType(), True),
        StructField("target_runs", IntegerType(), True),
        StructField("target_overs", StringType(), True), 
        StructField("super_over", StringType(), True),
        StructField("method", StringType(), True),
        StructField("umpire1", StringType(), True),
        StructField("umpire2", StringType(), True)
    ]),True)
])

df_deliveries=df_deliveries_stream.select(col("value").cast("string"))
df_match=df_match_stream.select(col("value").cast("string"))

df_deliveries=df_deliveries.select(from_json(col("value"),deliveries_schema).alias("data"))\
    .select(col("data.op"),col("data.source.table"),col("data.after.*"))

df_match=df_match.select(from_json(col("value"),match_schema).alias("data"))\
            .select(col("data.op"),col("data.source.table"),col("data.after.*"))
            
df_match=df_match.withColumn("match_date",expr("date_add('1970-01-01',match_date)"))\
            .withColumn("target_overs", col("target_overs").cast("float")) \
            .withColumn("method", when(col("method") == "NA", None).otherwise(col("method"))) \
            .withColumn("super_over", when(col("super_over") == "Y", True).otherwise(False))
            
df_match.writeStream.format("delta") \
        .option("path","s3a://ipl-cdc/streaming/matches") \
        .option("checkpointLocation","s3a://ipl-cdc/streaming/checkpoint/matches") \
        .trigger(processingTime="5 seconds") \
        .start()
        
df_deliveries.writeStream.format("delta") \
        .option("path","s3a://ipl-cdc/streaming/deliveries/") \
        .option("checkpointLocation","s3a://ipl-cdc/streaming/checkpoint/deliveries") \
        .trigger(processingTime="5 seconds") \
        .start()
        
spark.streams.awaitAnyTermination()

