from pyspark.sql import SparkSession
from src.transform import clean_car_data

def test_clean_car_data():
    spark = SparkSession.builder.appName("Test").getOrCreate()
    test_data = [("22–30", "Toyota", "Corolla"), ("19–25", "Honda", "Civic")]
    df = spark.createDataFrame(test_data, ["MPG", "brand", "Model"])
    cleaned = clean_car_data(spark, df)
    assert "MPG_avg" in cleaned.columns