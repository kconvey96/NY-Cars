from pyspark.sql.functions import col

# Clean model string
def clean_model(model):
    if model:
        return model.lower().strip().replace("-", "").replace(".", "").replace(",", "")
    return None

# Cleaning Car Listings

def clean_ny_cars(ny_cars, clean_udf):
    ny_cars = ny_cars.dropna(subset=["money", "Mileage", "Model", "brand"])
    ny_cars = ny_cars.dropDuplicates()
    ny_cars = ny_cars.withColumn("model_clean_ny", clean_udf(col("Model")))
    return ny_cars


# Cleaning Reviews data
def clean_car_rates(car_rates, clean_udf):
    car_rates = car_rates.dropna(subset=["Model", "Brand", "Reliability", "General_rate"])
    car_rates = car_rates.withColumn("model_clean_rate", clean_udf(col("Model")))
    return car_rates


# Joining of the datasets
def join_data(ny_cars, car_rates):
    ny = ny_cars.alias("ny")
    ratings = car_rates.alias("rt")
    return ny.join(
        ratings,
        (col("ny.brand") == col("rt.Brand")) &
        (col("ny.model_clean_ny") == col("rt.model_clean_rate")),
        how="inner"
    )