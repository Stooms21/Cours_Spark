from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo4.python_udf import add_category_name
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark

class PythonUdfTest(unittest.TestCase):
    def test_python_udf(self):
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
        df_result = add_category_name(df)

        #then
        self.assertEqual(df_result.columns, expected_result.columns)
        self.assertEqual(df_result.collect(), expected_result.collect())