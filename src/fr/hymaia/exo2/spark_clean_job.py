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
    not_corse = substring(col('zip').cast(StringType()), 0, 2) != '20'
    write_not_corse = substring(col('zip').cast(StringType()), 0, 2)
    is_corse_2A = col('zip').cast(StringType()) <= '20190'

    df_output = (df.withColumn('departement',
                               when(not_corse, write_not_corse)
                               .otherwise(when(is_corse_2A, '2A').otherwise('2B'))))
    return df_output


def clean(df_clients, df_zip_code):
    # On applique la jointure ainsi que le filtrage des clients majeurs
    output = join_zip(df_clients, df_zip_code)

    # On ajoute une colonne département à partir du code postal
    output_dep = ajout_departement(output)

    return output_dep


def main():
    # Initialisation de la session spark
    spark = SparkSession.builder \
        .appName("exo2") \
        .master("local[*]") \
        .getOrCreate()

    path = os.getcwd()
    path_resources = f"{path}/src/resources/exo2"
    path_output = f"{path}/data/exo2/clean"

    # Chargement des données à partir des fichiers csv dans des dataframes spark
    df_zip_code = (spark.read.option("header", "true")
                   .option("delimiter", ",")
                   .csv(f"{path_resources}/city_zipcode.csv"))

    df_clients = (spark.read.option("header", "true")
                  .option("delimiter", ",")
                  .csv(f"{path_resources}/clients_bdd.csv"))

    # On applique la jointure ainsi que le filtrage des clients majeurs
    output_dep = clean(df_clients, df_zip_code)

    # On sauvegarde le résultat dans un fichier parquet
    output_dep.write.mode("overwrite").parquet(f"{path_output}")
