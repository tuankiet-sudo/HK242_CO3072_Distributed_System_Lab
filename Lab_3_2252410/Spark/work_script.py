from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType

spark_entity = SparkSession.builder.appName("Spark").getOrCreate()

hdfs_output_path = "hdfs://node1:9000/water"
checkpoint_location = "hdfs://node1:9000/checkpoint"

bootstrap_servers = "node2:9092,node3:9092"

data_schema = StructType(
    [
        StructField("time", StringType(), True),
        StructField("station", StringType(), True),
	StructField("ph", DoubleType(), True),
	StructField("do", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
	StructField("salinity", DoubleType(), True),
    ]
)

df = (
    spark_entity.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", "water")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

csv_query = (
    df.selectExpr("CAST(value AS STRING) as value")
    .select(from_json(col("value"), data_schema).alias("data"))
    .select("data.*")
    .writeStream.format("csv")
    .option("path", hdfs_output_path)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="20 seconds")
    .outputMode("append")
    .start()
)

csv_query.awaitTermination()
