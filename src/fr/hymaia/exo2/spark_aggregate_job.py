import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


def main():
    spark = SparkSession.builder \
        .appName("aggregate") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_clean = f"{path}/src/fr/hymaia/exo2/clean"

    df_zip_code = (spark.read.parquet(f"{path_clean}"))

    df_agg = agg_departement(df_zip_code)
    df_agg.coalesce(1).write.mode("overwrite").csv(f"{path}/src/fr/hymaia/exo2/agg.csv", header=True)


def agg_departement(df):
    # Nous souhaiterions savoir combien de nos clients vivent par département et trier le résultat du département
    # le plus peuplé au moins peuplé. En cas d'égalité, c'est l'ordre alphabétique qui nous intéressera.
    return df.groupBy(col('departement')).count().sort(col('count').desc(), col('departement'))
