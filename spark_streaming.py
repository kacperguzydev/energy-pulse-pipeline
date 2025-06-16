import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as sum_, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.26.4-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energy_stream")

single_csv_path = "output/energy_usage.csv"

def write_to_csv(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.warning(f"No data in batch {batch_id}, skipping.")
        return

    logger.info(f"Writing batch {batch_id} to {single_csv_path}")
    (
        batch_df.coalesce(1)
        .write
        .mode("append")
        .option("header", True)
        .csv("output/temp_energy_batch")
    )

    latest_temp = [f for f in os.listdir("output/temp_energy_batch") if f.endswith(".csv")]
    if latest_temp:
        temp_file_path = os.path.join("output/temp_energy_batch", latest_temp[0])
        with open(single_csv_path, "a", encoding="utf-8") as main_csv, open(temp_file_path, "r", encoding="utf-8") as temp_csv:
            lines = temp_csv.readlines()
            if os.path.exists(single_csv_path) and os.path.getsize(single_csv_path) > 0:
                lines = lines[1:]
            main_csv.writelines(lines)
        os.remove(temp_file_path)

def main():
    logger.info("Starting Spark Structured Streaming job...")

    schema = StructType() \
        .add("household_id", StringType()) \
        .add("region", StringType()) \
        .add("kw", DoubleType()) \
        .add("timestamp", StringType())

    spark = (
        SparkSession.builder
        .appName("EnergyPulseKafkaToCSV")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "energy_meters")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), schema).alias("data"))
        .select(
            col("data.household_id"),
            col("data.region"),
            col("data.kw"),
            col("data.timestamp").cast("timestamp").alias("timestamp")
        )
    )

    agg = (
        df.withWatermark("timestamp", "30 seconds")
        .groupBy(window("timestamp", "30 seconds"), "region")
        .agg(
            sum_("kw").alias("total_kw"),
            avg("kw").alias("avg_kw"),
            count("*").alias("count")
        )
        .selectExpr("window.start as hour", "region", "total_kw", "avg_kw", "count")
    )

    (
        agg.writeStream
        .foreachBatch(write_to_csv)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    main()
