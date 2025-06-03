from src.load_data import load_car_data, load_rating_data
from src.transform import clean_car_data, clean_rating_data
from src.join_and_analysis import join_datasets, run_analysis

if __name__ == "__main__":
    spark, cars_df = load_car_data("data/New_York_cars.csv")
    _, ratings_df = load_rating_data("data/Car_Rates.csv")

    cleaned_cars_df = clean_car_data(spark, cars_df)
    cleaned_ratings_df = clean_rating_data(spark, ratings_df)

    joined_df = join_datasets(cleaned_cars_df, cleaned_ratings_df)
    run_analysis(joined_df)