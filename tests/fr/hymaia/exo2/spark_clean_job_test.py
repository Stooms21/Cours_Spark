import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import adult_clients, join_zip, ajout_departement, clean
from tests.fr.hymaia.spark_test_case import spark


class SparkCleanJobTest(unittest.TestCase):

    spark = SparkSession.builder.appName('unittest').getOrCreate()

    def test_adult_clients(self):
        # Given
        data = [("Laurent", 17, "92130"), ("Fiodor", 18, "25540"), ("Daniel", 19, "17300")]
        df = spark.createDataFrame(data, ["name", "age", "zip"])

        # When
        result = adult_clients(df)

        expected_data = [("Fiodor", 18, "25540"), ("Daniel", 19, "17300")]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "age", 'zip'])

        # Then
        self.assertEqual(result.columns, expected_df.columns)
        self.assertEqual(result.collect(), expected_df.collect())

    def test_join_zip(self):
        # GIven
        data_clients = [("Laurent", 17, "92130"), ("Fiodor", 18, "25540")]
        df_clients = spark.createDataFrame(data_clients, ["name", "age", "zip"])

        data_zip_code = [("92130", "Issy-les-Moulineaux"), ("25540", "Saint-Petersbourg")]
        df_zip_code = spark.createDataFrame(data_zip_code, ["zip", "city"])

        # When
        result = (join_zip(df_clients, df_zip_code)
                  .select("name", "age", "zip", "city")
                  .orderBy("name", "age", "zip", "city"))

        expected_data = [("Fiodor", 18, "25540", "Saint-Petersbourg")]
        expected_df = ((spark.createDataFrame(expected_data, ["name", "age", "zip", "city"])
                       .select("name", "age", "zip", "city"))
                       .orderBy("name", "age", "zip", "city"))

        # Then
        self.assertEqual(result.columns, expected_df.columns)
        self.assertEqual(result.collect(), expected_df.collect())

    def test_ajout_departement(self):
        # Given
        data = [("Machin", "20000"), ("Bidule", "20200"), ("Chouette", "75000")]
        df = spark.createDataFrame(data, ["name", "zip"])

        # When
        result = ajout_departement(df)

        expected_data = [("Machin", "20000", "2A"), ("Bidule", "20200", "2B"), ("Chouette", "75000", "75")]
        expected_df = spark.createDataFrame(expected_data, ["name", "zip", "departement"])

        # Then
        self.assertEqual(result.columns, expected_df.columns)
        self.assertEqual(result.collect(), expected_df.collect())

    def test_integration(self):
        # Given
        data_clients = [("Laurent", 17, "92130"), ("Fiodor", 18, "25540"), ("Daniel", 19, "17300")]
        df_clients = spark.createDataFrame(data_clients, ["name", "age", "zip"])

        data_zip = [("92130", "Issy-les-Moulineaux"), ("25540", "Saint-Petersbourg"), ("17300", "Rennes")]
        df_zip = spark.createDataFrame(data_zip, ["zip", "city"])

        # When
        df_add_departement = (clean(df_clients, df_zip)
                              .select("name", "age", "zip", "city", "departement")
                              .orderBy("name", "age", "zip", "city", "departement"))

        # Then
        expected_data = [("Fiodor", 18, "25540", "Saint-Petersbourg", "25"), ("Daniel", 19, "17300", "Rennes", "17")]
        expected_df = (spark.createDataFrame(expected_data, ["name", "age", "zip", "city", "departement"])
                       .orderBy("name", "age", "zip", "city", "departement"))

        self.assertEqual(df_add_departement.columns, expected_df.columns)
        self.assertEqual(df_add_departement.collect(), expected_df.collect())



