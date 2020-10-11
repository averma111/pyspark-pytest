from datetime import date
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StringType, StructField, Row


def to_Date_Df(df, fmt, field):
    return df.withColumn(field, to_date(col(field), fmt))


class RowTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("aap-pyspark-pytest") \
            .getOrCreate()

        my_schema = StructType([
            StructField("Id", StringType()),
            StructField("EventDate", StringType())])

        my_rows = [Row("101", "4/5/2020"), Row("102", "8/9/2020"), Row("103", "3/5/2020"), Row("104", "9/10/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_date_type(self):
        rows = to_Date_Df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = to_Date_Df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))
