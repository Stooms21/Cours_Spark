from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
import os


# On crée une udf pour ajouter la colonne category_name
@f.udf('string')
def category_name(category):
    if int(category) < 6:
        return "food"
    else:
        return "furniture"
def add_category_name(df):
    return df.withColumn('category_name', category_name(df.category))


def main():
    # On lance une session spark en local avec autant de coeurs que possible pour bien tester le parallelisme
    spark = SparkSession.builder \
        .appName("python_udf") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_sell = f"{path}/src/resources/exo4/sell.csv"

    df_sell = (spark.read.csv(f"{path_sell}", header=True))
    df_result = add_category_name(df_sell)

    #category_name_udf = f.udf(category_name, StringType())

    #df_sell.withColumn('category_name', category_name_udf(df_sell.category)).show(4)
