import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import os


def main():
    # On lance une session spark en local avec autant de coeurs que possible pour bien tester le parallelisme
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
    path = os.getcwd()
    # On lit le fichier csv
    df = spark.read.option("header", "true").csv(f"{path}/src/resources/exo1/data.csv")
    # On compte le nombre de mots
    df_out = wordcount(df, "text")
    # On écrit le résultat dans un fichier parquet
    df_out.write.mode("overwrite").partitionBy("count").parquet(f"{path}/data/exo1/output")


def wordcount(df, col_name):
    # On split la colonne text par espace et on compte le nombre de mots
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
