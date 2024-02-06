import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, substring
import os

from pyspark.sql.types import StringType


# Fonction qui filtre les clients majeurs
def adult_clients(df):
    return df.filter(col('age') >= 18)


# Fonction qui joint les deux dataframes : celle des clients et celle des codes postaux
def join_zip(df_clients, df_zip_code):
    return adult_clients(df_clients).join(df_zip_code, 'zip')


# Fonction qui ajoute une colonne département à partir du code postal des clients et prend en compte le cas de la Corse
def ajout_departement(df):
    df_output = (df.withColumn('departement',
                               when(substring(col('zip').cast(StringType()), 0, 2) != '20',
                                    substring(col('zip').cast(StringType()), 0, 2))
                               .otherwise(when(col('zip').cast(StringType()) <= '20190', '2A').otherwise('2B'))))
    return df_output


def main():
    # Initialisation de la session spark
    spark = SparkSession.builder \
        .appName("exo2") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_resources = f"{path}/src/resources/exo2"
    path_output = f"{path}/src/fr/hymaia/exo2/clean"

    # Chargement des données à partir des fichiers csv dans des dataframes spark
    df_zip_code = (spark.read.option("header", "true").option("delimiter", ",")
                   .csv(f"{path_resources}/city_zipcode.csv"))

    df_clients = (spark.read.option("header", "true").option("delimiter", ",")
                  .csv(f"{path_resources}/clients_bdd.csv"))

    # On applique la jointure ainsi que le filtrage des clients majeurs
    output = join_zip(df_clients, df_zip_code)
    # On ajoute une colonne département à partir du code postal
    output_dep = ajout_departement(output)
    # On sauvegarde le résultat dans un fichier parquet
    output_dep.write.mode("overwrite").parquet(f"{path_output}")
