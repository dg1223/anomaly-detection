from pyspark.sql.session import SparkSession
from pytest import raises

import src.caaswx.spark.features as ft
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()

# Get data
ufg_df = load_test_data(
    "data", "parquet_data", "user_feature_generator_tests", "data.parquet",
)

def test_UniqueUserOU():
    result = ft.UniqueUserOU().get_transformer(["CN"]).transform(ufg_df)
    result.show()

