from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql import SparkSession
import os


spark = (SparkSession.builder
         .config('spark.jars', 'src/resources/exo4/udf.jar')
         .appName("exo4")
         .master("local[*]")
         .getOrCreate())


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    path = os.getcwd()
    path_sell = f"{path}/src/resources/exo4/sell.csv"

    # On lit le fichier parquet
    df_sell = (spark.read.csv(f"{path_sell}", header=True))

    df_sell.withColumn('category_name', addCategoryName(df_sell.category)).show(4)
