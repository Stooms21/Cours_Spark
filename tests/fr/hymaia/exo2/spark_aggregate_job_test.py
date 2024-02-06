import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import agg_departement


class SparkAggregateJobTest(unittest.TestCase):

    spark = SparkSession.builder.appName('unittest').getOrCreate()

    def test_agg_departement(self):
        # Given
        data = [("Lars", "2A"), ("Ulrich", "2A"),
                ("Bruce", "75"), ("Dickinson", "13"),
                ("Laurent", "13"), ("Garnier", "13")]
        df = self.spark.createDataFrame(data, ["name", "departement"])

        # When
        result = agg_departement(df)

        expected_data = [("13", 3), ("2A", 2), ("75", 1)]
        expected_df = self.spark.createDataFrame(expected_data, ["departement", "count"])

        # Then
        self.assertEqual(result.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
