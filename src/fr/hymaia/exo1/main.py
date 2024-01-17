import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import os


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
    path = os.getcwd()
    df = spark.read.option("header", "true").csv(f"{path}/src/resources/exo1/data.csv")
    df_out = wordcount(df, "text")
    df_out.write.mode("overwrite").partitionBy("count").parquet(f"{path}/src/fr/hymaia/exo1/output")
    df_out.show()

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()