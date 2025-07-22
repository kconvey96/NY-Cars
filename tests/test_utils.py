# Importing functions to test

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import clean_model, clean_ny_cars, join_data

# Spark Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Mini Tests").getOrCreate()

# Setup between utils file
@pytest.fixture
def clean_udf():
    return udf(clean_model, StringType())

@pytest.fixture
def ny_sample_data(spark):
    data = [
        ("BMW", "3 Series", 15000, 30000, "3series"),
        ("Ford", "Focus", 9000, 45000, "focus")
    ]
    columns = ["brand", "Model", "money", "Mileage", "model_clean_ny"]
    return spark.createDataFrame(data, columns)

@pytest.fixture
def rates_sample_data(spark):
    data = [
        ("3 Series", "BMW", 4.7, 4.5, "3series"),
        ("Focus", "Ford", 4.0, 3.8, "focus")
    ]
    columns = ["Model", "Brand", "Reliability", "General_rate", "model_clean_rate"]
    return spark.createDataFrame(data, columns)

# Clean_model logic
def test_clean_model_basic():
    assert clean_model(" 3-Series. ") == "3series"
    assert clean_model("Focus-SE") == "focusse"
    assert clean_model(None) is None

# Cleans car listings drops incomplete rows
def test_clean_ny_cars_row_cleaning(spark, clean_udf):
    data = [
        ("Ford", "Focus", 9000, 45000),     # valid
        (None, "3 Series", None, 30000)     # invalid
    ]
    columns = ["brand", "Model", "money", "Mileage"]
    df = spark.createDataFrame(data, columns)

    cleaned = clean_ny_cars(df, clean_udf)

    assert cleaned.count() == 1
    assert "model_clean_ny" in cleaned.columns
    assert cleaned.select("model_clean_ny").first()[0] == "focus"

# joins data matches cleaned models and brands
def test_join_data_matches_correctly(ny_sample_data, rates_sample_data):
    joined = join_data(ny_sample_data, rates_sample_data)
    assert joined.count() == 2

    # Check brands are joined correctly
    brands = [row["brand"] for row in joined.collect()]
    assert "BMW" in brands
    assert "Ford" in brands