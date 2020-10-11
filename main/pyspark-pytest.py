import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession

from lib.logger import Log4j
from lib.utils import get_spark_app_config


def to_Date_Df(df, fmt, field):
    return df.withColumn(field, to_date(col(field), fmt))


if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)
    logger.info("Starting pyspark application")
    my_schema = StructType([
        StructField("Id", StringType()),
        StructField("EventDate", StringType())
    ])

    my_rows = [Row("101", "4/5/2020"), Row("102", "8/9/2020"), Row("103", "3/5/2020"), Row("104", "9/10/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)

    logger.info("Creating the dataframe from Row data")
    my_df = spark.createDataFrame(my_rdd, my_schema)

    logger.info("Dataframe with old date format")
    my_df.printSchema()
    my_df.show()

    my_new_df = to_Date_Df(my_df, "M/d/y", "EventDate")
    logger.info("Dataframe with new date format")
    my_new_df.printSchema()
    my_new_df.show()
    logger.info("Stopping pyspark application")
