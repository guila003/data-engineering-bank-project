from pySpark.sql import SparkSession
import logging
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bank Transactions ETL") \
    .getOrCreate()

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)
#Load data
df = spark.read.csv("../data/bank-full.csv",header=True, inferSchema=True)

df.show(5)