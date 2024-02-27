from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo4.no_udf import add_category_name_no_udf, add_total_price_per_category_per_day, add_total_price_per_category_per_day_last_30_days
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark

class PythonNoUdfTest(unittest.TestCase):
    def test_add_category_name_no_udf(self):
        #given
        df = spark.createDataFrame(
            [
                (0, '2019-02-17', 6, 49.00),
                (1, '2015-10-01', 4, 69.00)

            ],
            ['id', 'date', 'category', 'price']
        )

        expected_result = spark.createDataFrame(
            [
                (0, '2019-02-17', 6, 49.00, 'furniture'),
                (1, '2015-10-01', 4, 69.00, 'food')

            ],
            ['id', 'date', 'category', 'price', 'category_name']
        )

        #when
        df_result = add_category_name_no_udf(df)


        #then
        self.assertEqual(df_result.columns, expected_result.columns)
        self.assertEqual(df_result.collect(), expected_result.collect())

    def test_add_total_price_per_category_per_day(self):
        # given
        df = spark.createDataFrame(
            [
                (0, '2019-02-17', 6, 40.0, 'furniture'),
                (0, '2019-02-17', 6, 33.0, 'furniture'),
                (0, '2019-02-17', 4, 70.0, 'food'),
                (0, '2019-02-17', 4, 12.0, 'food'),
                (0, '2019-02-18', 6, 20.0, 'furniture'),
                (0, '2019-02-18', 6, 25.0, 'furniture')
            ],
            ['id', 'date', 'category', 'price', 'subcategory']
        )

        expected_result = spark.createDataFrame(
            [
                (0, '2019-02-17', 4, 70.0, 'food', 82.0),
                (0, '2019-02-17', 4, 12.0, 'food', 82.0),
                (0, '2019-02-17', 6, 40.0, 'furniture', 73.0),
                (0, '2019-02-17', 6, 33.0, 'furniture', 73.0),
                (0, '2019-02-18', 6, 20.0, 'furniture', 45.0),
                (0, '2019-02-18', 6, 25.0, 'furniture', 45.0)
            ],
            ['id', 'date', 'category', 'price', 'subcategory', 'total_price_per_category_per_day']
        )

        # when
        df_result = add_total_price_per_category_per_day(df, 'subcategory', 'date', 'price')

        # then
        self.assertEqual(df_result.columns, expected_result.columns)
        self.assertEqual(df_result.collect(), expected_result.collect())

    def test_add_total_price_per_category_per_day_last_30_days(self):
        # given
        df = spark.createDataFrame(
            [
                (0, '2019-02-16', 6, 40.0, 'furniture'),
                (0, '2019-02-17', 6, 33.0, 'furniture'),
                (0, '2019-02-18', 6, 70.0, 'furniture'),
                (0, '2019-02-16', 4, 12.0, 'food'),
                (0, '2019-02-17', 4, 20.0, 'food'),
                (0, '2019-02-18', 4, 25.0, 'food')
            ],
            ['id', 'date', 'category', 'price', 'category_name']
        )

        expected_result = spark.createDataFrame(
            [
                (0, '2019-02-16', 4, 12.0, 'food', 12.0),
                (0, '2019-02-17', 4, 20.0, 'food', 32.0),
                (0, '2019-02-18', 4, 25.0, 'food', 57.0),
                (0, '2019-02-16', 6, 40.0, 'furniture', 40.0),
                (0, '2019-02-17', 6, 33.0, 'furniture', 73.0),
                (0, '2019-02-18', 6, 70.0, 'furniture', 143.0),
            ],
            ['id', 'date', 'category', 'price', 'category_name', 'total_price_per_category_per_day_last_30_days']
        )

        # when
        df_result = add_total_price_per_category_per_day_last_30_days(df, 'category_name', 'date', 'price')
        # then
        self.assertEqual(df_result.columns, expected_result.columns)
        self.assertEqual(df_result.collect(), expected_result.collect())
