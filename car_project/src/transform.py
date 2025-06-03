from pyspark.sql.functions import trim, col

def clean_car_data(spark, df):
    for c in ["brand", "Model"]:
        df = df.withColumn(c, trim(col(c)))

    def parse_mpg(mpg):
        if mpg and "–" in mpg:
            try:
                low, high = map(float, mpg.split("–"))
                return round((low + high) / 2, 2)
            except:
                return None
        return None

    parse_mpg_udf = spark.udf.register("parse_mpg", parse_mpg)
    df = df.withColumn("MPG_avg", parse_mpg_udf(col("MPG")))
    return df

def clean_rating_data(spark, df):
    for c in ["Brand", "Model"]:
        df = df.withColumn(c, trim(col(c)))
    return df