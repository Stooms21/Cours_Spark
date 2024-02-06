import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo1.main import wordcount


class WordCountTest(unittest.TestCase):
    spark = SparkSession.builder.appName('wordcount_unittest').getOrCreate()

    def test_wordcount(self):
        # Given
        data = [("hello world",), ("test test test",)]
        df = self.spark.createDataFrame(data, ["text"])

        # When
        actual = wordcount(df, "text").orderBy("word")

        # Expected
        expected_data = [("hello", 1), ("world", 1), ("test", 3)]
        expected_df = self.spark.createDataFrame(expected_data, ["word", "count"]).orderBy("word")

        # On convertit les dataframes en listes pour pouvoir les comparer
        result_data = [(row['word'], row['count']) for row in actual.collect()]
        expected_data = [(row['word'], row['count']) for row in expected_df.collect()]

        # On compare le résultat entre le résultat attendu et le résultat obtenu
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
