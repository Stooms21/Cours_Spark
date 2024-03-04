import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import agg_departement
from pyspark.sql.types import StructType, StructField, StringType, LongType
from tests.fr.hymaia.spark_test_case import spark
import pyspark

class SparkAggregateJobTest(unittest.TestCase):
    def test_agg_departement(self):
        # Given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),
            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        # When
        result = agg_departement(df)

        expected_df = spark.createDataFrame(
            [
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            ['departement', 'count']
        )

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
    def test_agg_with_different_schema(self):
        #given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),
            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        expected_result_schema = StructType([
            StructField('departement', StringType(), True),
            StructField('count', LongType(), False)
        ])
        expected_result = spark.createDataFrame(
            [
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            ['departement', 'count']
        )

        #when
        df_result = agg_departement(df)

        #then

        with self.assertRaises(AssertionError) as context:
            # Appel de la fonction ou du code susceptible de lever une AssertionError
            self.assertEqual(expected_result.schema, df_result.schema)

        # Vérifiez que l'erreur levée est bien une AssertionError
        self.assertEqual(type(context.exception), AssertionError)

    def test_agg_with_same_schema(self):
        #given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),
            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        expected_result_schema = StructType([
            StructField('departement', StringType(), True),
            StructField('count', LongType(), False)
        ])
        expected_result = spark.createDataFrame(
            [
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            expected_result_schema
        )

        #when
        df_result = agg_departement(df)

        #then


        self.assertEqual(expected_result.schema, df_result.schema)
        self.assertEqual(expected_result.collect(), df_result.collect())
    def test_agg_with_invalid_data(self):
        #given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),

            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        with self.assertRaises(Exception) as context:
            # when
            agg_result = agg_departement(df.select("name", "age"))

        self.assertEqual(type(context.exception), pyspark.sql.utils.AnalysisException)
