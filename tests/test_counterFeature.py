from src.caaswx.spark.utils import (
    HasTypedInputCol,
    HasTypedInputCols,
    HasTypedOutputCol,
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


class Test_CounterFeature(CounterFeature, HasTypedInputCol):
    """
    Feature.
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT"):
        super(Test_CounterFeature, self).__init__(outputCol)
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


def test_counter:
    fg = Test_CounterFeature()

    # gonna put some basic counter tests...?