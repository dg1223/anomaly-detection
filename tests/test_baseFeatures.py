from src.caaswx.spark.base import GroupbyTransformer

from src.caaswx.spark.utils import (
    HasTypedInputCol,
    HasTypedInputCols,
    HasTypedOutputCol, load_test_data,
)

from src.caaswx.spark.base import (
    GroupbyFeature,
    CounterFeature,
    DistinctCounterFeature,
    ArrayDistinctFeature,
    ArrayRemoveFeature,
    SizeArrayRemoveFeature,
)

from pyspark.sql.functions import (
    col,
    when,
    lag,
    isnull,
    regexp_extract,
    countDistinct,
    array_distinct,
    sort_array,
    collect_set,
    collect_list,
    mean as sparkmean,
    stddev as sparkstddev,
    min as sparkmin,
    max as sparkmax,
    round as sparkround,
    sum as sparksum,
    slice as sparkslice,
)
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ArrayType,
    TimestampType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


class TestCounterFeature(CounterFeature, HasTypedInputCol):
    """
    Feature to test basic CounterFeature functionality.
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT"):
        super(TestCounterFeature, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        return when(col(self.getOrDefault("inputCol")) == 1, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class TestingFeatureGenerator(GroupbyTransformer):
    """
    Base Implementation of the TestingFeatureGenerator.

    To add a feature implement the feature as subclass of GroupbyFeature and
    include feature in features variable in the constructor and in super
    constructor.
    """

    def __init__(self):
        group_keys = ["CN"]
        features = [
            TestCounterFeature(),
        ]
        super(TestingFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )


class TestingDistinctCounterFeature(DistinctCounterFeature, HasTypedInputCol):


# get data and run transformer
df = load_test_data(
    "data", "parquet_data", "user_feature_generator_tests", "data.parquet"
)

ans_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "ans.parquet",
)
fg = TestingFeatureGenerator()
result_df = fg.transform(df)

print(result_df)


# check cols for accuracy (asserts)
def test_counter_feature():
    print("tmp")
        # assert new col = ans' COUNT_AUTH_ACCEPT
