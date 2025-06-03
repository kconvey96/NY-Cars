def join_datasets(cars_df, ratings_df):
    return cars_df.join(ratings_df, on=["Year", "Brand", "Model"], how="left")

def run_analysis(df):
    df.groupBy("Brand").count().orderBy("count", ascending=False).show(10)
    df.select("money", "MPG_avg", "General_rate").describe().show()