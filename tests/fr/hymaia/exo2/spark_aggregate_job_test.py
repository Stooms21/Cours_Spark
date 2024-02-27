import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import agg_departement
from pyspark.sql.types import StructType, StructField, StringType
from tests.fr.hymaia.spark_test_case import spark
import pyspark

class SparkAggregateJobTest(unittest.TestCase):
    def test_agg_departement(self):
        # Given
        data = [("Lars", "2A"), ("Ulrich", "2A"),
                ("Bruce", "75"), ("Dickinson", "13"),
                ("Laurent", "13"), ("Garnier", "13")]
        df = spark.createDataFrame(data, ["name", "departement"])

        # When
        result = agg_departement(df)

        expected_data = [("13", 3), ("2A", 2), ("75", 1)]
        expected_df = spark.createDataFrame(expected_data, ["departement", "count"])

        # Then
        self.assertEqual(result.columns, expected_df.columns)
        self.assertEqual(result.collect(), expected_df.collect())


    def test_agg_departement_dataframe_vide(self):
        # Given
        data = []
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("departement", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)

        # When
        with self.assertRaises(Exception) as context:
            agg_departement(df)

        #
        self.assertEqual(type(context.exception), ValueError)
