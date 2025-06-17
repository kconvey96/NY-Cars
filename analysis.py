from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, avg, when
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Step 1: Creation of the Spark session
spark = SparkSession.builder.appName("NY Car Analysis").getOrCreate()

# Step 2: Load CSVs from correct folder path
ny_cars = spark.read.csv("car_project/data/New_York_cars.csv", header=True, inferSchema=True)
car_rates = spark.read.csv("car_project/data/Car_Rates.csv", header=True, inferSchema=True)

print("NY Cars Columns:", ny_cars.columns)
print("Car Ratings Columns:", car_rates.columns)

ny_cars.printSchema()
ny_cars.show(10)

# Step 3: Cleaning car listings data
ny_cars = ny_cars.dropna(subset=["money", "Mileage", "Model", "brand"])
ny_cars = ny_cars.dropDuplicates()

# Clean model string
def clean_model(model):
    if model:
        return model.lower().strip().replace("-", "").replace(".", "").replace(",", "")
    return None

clean_udf = udf(clean_model, StringType())
ny_cars = ny_cars.withColumn("model_clean_ny", clean_udf(col("Model")))


# Step 4: Clean ratings data
car_rates = car_rates.dropna(subset=["Model", "Brand", "Reliability", "General_rate"])
car_rates = car_rates.withColumn("model_clean_rate", clean_udf(col("Model")))

# Step 5: Joining of the datasets
ny = ny_cars.alias("ny")
ratings = car_rates.alias("rt")


merged = ny.join(
    ratings,
    (col("ny.brand") == col("rt.Brand")) &
    (col("ny.model_clean_ny") == col("rt.model_clean_rate")),
    how="inner"
)

# Step 6: Question 1 – Most common cars and their scores
print("\n--- Q1: Most Common Car Models and Their Ratings ---")
top_models = merged.groupBy("ny.Model").count().orderBy("count", ascending=False)
top_models.show(10)

top_model_scores = merged.groupBy("ny.Model").agg(
    avg("rt.Reliability").alias("avg_reliability"),
    avg("rt.General_rate").alias("avg_overall_rating")
).orderBy("avg_overall_rating", ascending=False)
top_model_scores.show(10)


# Step 7: Question 2 – Best value models (rating vs price)
print("\n--- Q2: Value for Money (Rating / Price) ---")
value_df = merged.withColumn("value_score", col("rt.General_rate") / col("ny.money"))
value_df.select("ny.brand", "ny.Model", "ny.money", "rt.General_rate", "value_score") \
    .orderBy("value_score", ascending=False).show(10)

# Step 8: Question 3 – Fuel price vs car cost
print("\n--- Q3: Avg Car Price by Fuel Type ---")
merged.groupBy("ny.Fuel type").agg(
    avg("ny.money").alias("avg_price"),
    avg("rt.General_rate").alias("avg_rating")
).orderBy("avg_price", ascending=False).show()

# Step 9: Question 4 – One owner vs reliability
print("\n--- Q4: Reliability by One Owner ---")
merged = merged.withColumn("is_one_owner", (col("1-owner vehicle") == "True").cast("int"))
merged.groupBy("is_one_owner").agg(avg("rt.Reliability").alias("avg_reliability")).show()


# Step 10: Question 5 – Fuel type and user satisfaction
print("\n--- Q5: Satisfaction by Fuel Type ---")
merged.groupBy("ny.Fuel type").agg(
    avg("ny.money").alias("avg_price"),
    avg("rt.General_rate").alias("avg_rating")
).orderBy("avg_rating", ascending=False).show()


# Stop the session
spark.stop()