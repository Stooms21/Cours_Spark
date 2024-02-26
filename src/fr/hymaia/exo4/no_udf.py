from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import os


def main():
    # On lance une session spark en local avec autant de coeurs que possible pour bien tester le parallelisme
    spark = SparkSession.builder \
        .appName("no_udf") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_sell = f"{path}/src/resources/exo4/sell.csv"

    df_sell = (spark.read.csv(f"{path_sell}", header=True))

    df_sell = df_sell.withColumn('category_name', f.when(f.col('category') < 6, 'food').otherwise('furniture'))

    windowSpec = Window.partitionBy("category", "date")
    df_sell = df_sell.withColumn('total_price_per_category_per_day', f.sum('price').over(windowSpec))

    df_sell.show(10)
