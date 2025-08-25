from pyspark.sql import SparkSession
import os

def run_spark_job():
    spark = SparkSession.builder\
        .appName("AirflowSparkJob")\
        .getOrCreate()

    data = [("Python", "3.11"), ("Spark", "3.5"), ("Java", "17")]
    columns = ["Tech", "Version"]

    df = spark.createDataFrame(data, columns)
    df.show()

    spark.stop()