from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import os
import time


def main():
    # read csv file
    spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")

    df_cat = add_category_name_no_udf(df)
    distinct_count = df_cat.groupby("category_name").count().collect()


def add_category_name_no_udf(df):
    return df.withColumn('category_name',f.when(df.category < 6, 'food').otherwise('furniture'))

def add_total_price_per_category_per_day(df, category_name, date, price):
    window = Window.partitionBy(date, category_name)
    sum_price = f.sum(price).over(window)
    return df.withColumn('total_price_per_category_per_day', sum_price)

def add_total_price_per_category_per_day_last_30_days(df, category_name, date, price):
    window = Window.partitionBy(category_name).orderBy(date).rowsBetween(-30, 0)
    sum_price = f.sum(price).over(window)
    # Ajout de la colonne 'total_price_per_category_per_day_last_30_days'
    return df.withColumn('total_price_per_category_per_day_last_30_days', sum_price)