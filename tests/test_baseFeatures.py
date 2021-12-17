from src.caaswx.spark.utils import (
    HasTypedInputCol,
    HasTypedInputCols,
    HasTypedOutputCol, load_test_data,
)
import src.caaswx.spark.features as ft
from src.caaswx.spark.base import (
    GroupbyFeature,
    GroupbyTransformer,
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
    (Original: CountAuthAccept)
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


class TestArrayDistinctFeature(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature to test basic ArrayDistinctFeature functionality.
    (Original: UniqueSMActions)
    """

    def __init__(
        self, inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS"
    ):
        super(TestArrayDistinctFeature, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS"
        )
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_ACTION
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class TestArrayRemoveFeature(ArrayRemoveFeature, HasTypedInputCol):
    """
    Feature to test basic ArrayRemove functionality.
    (Original: UniqueUserOU)
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="UNIQUE_USER_OU"):
        super(TestArrayRemoveFeature, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="UNIQUE_USER_OU")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_USERNAME
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"ou=(,*?),", 0)
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class TestSizeArrayRemoveFeature(SizeArrayRemoveFeature, HasTypedInputCol):
    """
    Feature to test basic ArrayRemove functionality.
    (Original: CountUniqueOU)
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_UNIQUE_OU"):
        super(TestSizeArrayRemoveFeature, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_UNIQUE_OU")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_USERNAME
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"ou=(,*?),", 0)
        )

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
            # Testing Base Features
            TestCounterFeature(),
            TestArrayDistinctFeature(),
            TestArrayRemoveFeature(),
            TestSizeArrayRemoveFeature(),

            # Testing Individual Features that inherit from GroupbyFeature
            ft.MinUserTimestamp(),
            ft.MinTimeBtRecords(),
            ft.MaxUserTimestamp(),
            ft.MaxTimeBtRecords(),
            ft.AvgTimeBtRecords(),
            ft.UserNumOfAccountsLoginWithSameIPs(),
            ft.StdBtRecords(),
            # ft.UserIsUsingUnusualBrowser(),
            TestingDistinctCounterFeature(),
        ]
        super(TestingFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )


class TestingDistinctCounterFeature(DistinctCounterFeature, HasTypedInputCol):
    """
        Feature to test basic DistinctCounterFeature functionality.
        """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT"):
        super(TestingDistinctCounterFeature, self).__init__(outputCol)
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

# get data and run transformer
ufg_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "data.parquet",
)

ans_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "ans.parquet",
)
fg = TestingFeatureGenerator()

result_df = fg.transform(ufg_df)


# check cols for accuracy (asserts)
def test_counter_feature():
    rTest = result_df.select("COUNT_AUTH_ACCEPT")
    aTest = ans_df.select("COUNT_AUTH_ACCEPT")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0


def test_array_distinct_feature():
    rTest = result_df.select("UNIQUE_SM_ACTIONS")
    aTest = ans_df.select("UNIQUE_SM_ACTIONS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0


def test_array_remove_feature():
    rTest = result_df.select("UNIQUE_USER_OU")
    aTest = ans_df.select("UNIQUE_USER_OU")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0


def test_size_array_remove_feature():
    rTest = result_df.select("COUNT_UNIQUE_OU")
    aTest = ans_df.select("COUNT_UNIQUE_OU")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0


def test_min_user_timestamp():
    rTest = result_df.select("MIN_USER_TIMESTAMP")
    aTest = ans_df.select("MIN_USER_TIMESTAMP")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_min_time_bt_records():
    rTest = result_df.select("MIN_TIME_BT_RECORDS")
    aTest = ans_df.select("MIN_TIME_BT_RECORDS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0


def test_max_user_timestamp():
    rTest = result_df.select("MAX_USER_TIMESTAMP")
    aTest = ans_df.select("MAX_USER_TIMESTAMP")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_max_time_bt_records():
    rTest = result_df.select("MAX_TIME_BT_RECORDS")
    aTest = ans_df.select("MAX_TIME_BT_RECORDS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_min_user_timestamp():
    rTest = result_df.select("MIN_USER_TIMESTAMP")
    aTest = ans_df.select("MIN_USER_TIMESTAMP")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_avg_time_bt_records():
    rTest = result_df.select("AVG_TIME_BT_RECORDS")
    aTest = ans_df.select("AVG_TIME_BT_RECORDS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_std_bt_records():
    rTest = result_df.select("SDV_BT_RECORDS")
    aTest = ans_df.select("SDV_BT_RECORDS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0

def test_user_num_acc_same_ips():
    rTest = result_df.select("USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS")
    aTest = ans_df.select("USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0
def test_distinct_counter_feature():
    rTest = result_df.select("COUNT_AUTH_ACCEPT")
    aTest = ans_df.select("COUNT_AUTH_ACCEPT")

    # Size test
    assert rTest.count() == aTest.count()

    # content test
    assert rTest.subtract(aTest).count() == 0
