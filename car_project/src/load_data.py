from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName("Car Data Analysis") \
        .getOrCreate()

def load_car_data(path):
    spark = get_spark()
    df = spark.read.option("header", True).csv(path, inferSchema=True)
    return spark, df

def load_rating_data(path):
    spark = get_spark()
    df = spark.read.option("header", True).csv(path, inferSchema=True)
    return spark, df