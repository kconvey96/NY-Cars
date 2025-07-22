from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, avg, when
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from utils import clean_model, clean_ny_cars, clean_car_rates, join_data

# Creation of the Spark session
def get_spark_session(app_name="NY Car Analysis"):
    return SparkSession.builder.appName(app_name).getOrCreate()

# Load CSVs from correct folder path
def load_data(spark, ny_path, rates_path):
    ny_cars = spark.read.csv(ny_path, header=True, inferSchema=True)
    car_rates = spark.read.csv(rates_path, header=True, inferSchema=True)
    return ny_cars, car_rates

spark = get_spark_session()
ny_cars, car_rates = load_data(spark, "car_project/data/New_York_cars.csv", "car_project/data/Car_Rates.csv")

clean_udf = udf(clean_model, StringType())

ny_cars = clean_ny_cars(ny_cars, clean_udf)
car_rates = clean_car_rates(car_rates, clean_udf)

print("NY Cars Columns:", ny_cars.columns)
print("Car Ratings Columns:", car_rates.columns)

ny_cars.printSchema()
ny_cars.show(10)

merged = join_data(ny_cars, car_rates)

# Question 1 – Most common cars and their scores
print("\n--- Q1: Most Common Car Models and Their Ratings ---")
top_models = merged.groupBy("ny.Model").count().orderBy("count", ascending=False)
top_models.show(10)

top_model_scores = merged.groupBy("ny.Model").agg(
    avg("rt.Reliability").alias("avg_reliability"),
    avg("rt.General_rate").alias("avg_overall_rating")
).orderBy("avg_overall_rating", ascending=False)
top_model_scores.show(10)


# Question 2 – Best value models (rating vs price or ratings divided by price)
print("\n--- Q2: Value for Money (Rating / Price) ---")
value_df = merged.withColumn("value_score", col("rt.General_rate") / col("ny.money"))
value_df.select("ny.brand", "ny.Model", "ny.money", "rt.General_rate", "value_score") \
    .orderBy("value_score", ascending=False).show(10)

# Question 3 – Fuel price vs car cost (Fuel divided by car price)
print("\n--- Q3: Avg Car Price by Fuel Type ---")
merged.groupBy("ny.Fuel type").agg(
    avg("ny.money").alias("avg_price"),
    avg("rt.General_rate").alias("avg_rating")
).orderBy("avg_price", ascending=False).show()

# Question 4 – One owner vs reliability 
print("\n--- Q4: Reliability by One Owner ---")
merged = merged.withColumn("is_one_owner", (col("1-owner vehicle") == "True").cast("int"))
merged.groupBy("is_one_owner").agg(avg("rt.Reliability").alias("avg_reliability")).show()


# Question 5 – Fuel type and user satisfaction
print("\n--- Q5: Satisfaction by Fuel Type ---")
merged.groupBy("ny.Fuel type").agg(
    avg("ny.money").alias("avg_price"),
    avg("rt.General_rate").alias("avg_rating")
).orderBy("avg_rating", ascending=False).show()


# Stop the session
spark.stop()