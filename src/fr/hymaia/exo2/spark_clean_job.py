import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, substring
import os


def main():
    spark = SparkSession.builder \
        .appName("exo2") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_resources = f"{path}/src/resources/exo2"
    path_output = f"{path}/src/fr/hymaia/exo2/clean"

    df_zip_code = (spark.read.option("header", "true").option("delimiter", ",")
                   .csv(f"{path_resources}/city_zipcode.csv"))

    df_clients = (spark.read.option("header", "true").option("delimiter", ",")
                  .csv(f"{path_resources}/clients_bdd.csv"))

    output = join_zip(df_clients, df_zip_code)
    output.show()
    output.write.mode("overwrite").parquet(f"{path_output}")
    output_dep = ajout_departement(output)
    output_dep.write.mode("overwrite").parquet(f"{path_output}")
    output_dep.show()


def adult_clients(df):
    return df.filter(col('age') >= 18)


def join_zip(df_clients, df_zip_code):
    return adult_clients(df_clients).join(df_zip_code, 'zip')


def ajout_departement(df):
    df_output = (df.withColumn('departement',
                               when(substring(col('zip'), 0, 2) != '20', substring(col('zip'), 0, 2))
                               .otherwise(when(col('zip') <= 20190, '2A').otherwise('2B'))))
    return df_output
