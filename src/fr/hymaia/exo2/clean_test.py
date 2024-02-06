import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import adult_clients, join_zip, ajout_departement


class SparkCleanJobTest(unittest.TestCase):

    spark = SparkSession.builder.appName('unittest').getOrCreate()

    def test_adult_clients(self):
        # Given
        data = [("Laurent", 17, "92130"), ("Fiodor", 18, "25540"), ("Daniel", 19, "17300")]
        df = self.spark.createDataFrame(data, ["name", "age", "zip"])

        # When
        result = adult_clients(df)

        expected_data = [("Fiodor", 18, "25540"), ("Daniel", 19, "17300")]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "age", 'zip'])

        # Then
        self.assertEqual(result.collect(), expected_df.collect())

    def test_join_zip(self):
        # GIven
        data_clients = [("Laurent", 17, "92130"), ("Fiodor", 18, "25540")]
        df_clients = self.spark.createDataFrame(data_clients, ["name", "age", "zip"])

        data_zip_code = [("92130", "Issy-les-Moulineaux"), ("25540", "Saint-Petersbourg")]
        df_zip_code = self.spark.createDataFrame(data_zip_code, ["zip", "city"])

        # When
        result = (join_zip(df_clients, df_zip_code).select("name", "age", "zip", "city")
                  .orderBy("name", "age", "zip", "city"))

        expected_data = [("Fiodor", 18, "25540", "Saint-Petersbourg")]
        expected_df = (self.spark.createDataFrame(expected_data, ["name", "age", "zip", "city"])
                       .select("name", "age", "zip", "city")).orderBy("name", "age", "zip", "city")

        # Then
        self.assertEqual(result.collect(), expected_df.collect())

    def test_ajout_departement(self):
        # Given
        data = [("Machin", "20000"), ("Bidule", "20200"), ("Chouette", "75000")]
        df = self.spark.createDataFrame(data, ["name", "zip"])

        # When
        result = ajout_departement(df)

        expected_data = [("Machin", "20000", "2A"), ("Bidule", "20200", "2B"), ("Chouette", "75000", "75")]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "zip", "departement"])

        # Then
        self.assertEqual(result.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()