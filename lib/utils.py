from pyspark import SparkConf
import configparser


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_movie_df(spark, datafile):
    return spark.read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv(datafile)
