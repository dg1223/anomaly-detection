from pyspark.sql.functions import (
    col,
    when,
    regexp_extract,
    collect_list,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)
import src.caaswx.spark.features as ft
from src.caaswx.spark.base import (
    GroupbyTransformer,
    CounterFeature,
    DistinctCounterFeature,
    ArrayDistinctFeature,
    ArrayRemoveFeature,
    SizeArrayRemoveFeature,
)
from src.caaswx.spark.utils import (
    HasTypedInputCol,
    load_test_data,
)

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


class TestDistinctCounterFeature(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature to test basic DistinctCounterFeature functionality.
    (Original: CountUniqueEvents)
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_UNIQUE_EVENTS"):
        super(TestDistinctCounterFeature, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_UNIQUE_EVENTS"
        )
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
        """
        :return: Returns the column SM_EVENTID
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class TestArrayDistinctFeature(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature to test basic ArrayDistinctFeature functionality.
    (Original: UniqueSMActions)
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS"):
        super(TestArrayDistinctFeature, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS")
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
            TestDistinctCounterFeature(),
            # Testing Individual Features that inherit from GroupbyFeature
            ft.MinUserTimestamp(),
            ft.MinTimeBtRecords(),
            ft.MaxUserTimestamp(),
            ft.MaxTimeBtRecords(),
            ft.AvgTimeBtRecords(),
            ft.UserNumOfAccountsLoginWithSameIPs(),
            ft.StdBtRecords(),
            ft.FlattenerFeature("SM_AGENTNAME", "Flattened_SM_AGENTNAME"),
            # ft.UserIsUsingUnusualBrowser(),
        ]
        super(TestingFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )


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
    r_test = result_df.select("COUNT_AUTH_ACCEPT")
    a_test = ans_df.select("COUNT_AUTH_ACCEPT")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_distinct_counter_feature():
    r_test = result_df.select("COUNT_UNIQUE_EVENTS")
    a_test = ans_df.select("COUNT_UNIQUE_EVENTS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_array_distinct_feature():
    r_test = result_df.select("UNIQUE_SM_ACTIONS")
    a_test = ans_df.select("UNIQUE_SM_ACTIONS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_array_remove_feature():
    r_test = result_df.select("UNIQUE_USER_OU")
    a_test = ans_df.select("UNIQUE_USER_OU")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_size_array_remove_feature():
    r_test = result_df.select("COUNT_UNIQUE_OU")
    a_test = ans_df.select("COUNT_UNIQUE_OU")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_min_user_timestamp():
    r_test = result_df.select("MIN_USER_TIMESTAMP")
    a_test = ans_df.select("MIN_USER_TIMESTAMP")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_min_time_bt_records():
    r_test = result_df.select("MIN_TIME_BT_RECORDS")
    a_test = ans_df.select("MIN_TIME_BT_RECORDS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_max_user_timestamp():
    r_test = result_df.select("MAX_USER_TIMESTAMP")
    a_test = ans_df.select("MAX_USER_TIMESTAMP")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_max_time_bt_records():
    r_test = result_df.select("MAX_TIME_BT_RECORDS")
    a_test = ans_df.select("MAX_TIME_BT_RECORDS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_avg_time_bt_records():
    r_test = result_df.select("AVG_TIME_BT_RECORDS")
    a_test = ans_df.select("AVG_TIME_BT_RECORDS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_std_bt_records():
    r_test = result_df.select("SDV_BT_RECORDS")
    a_test = ans_df.select("SDV_BT_RECORDS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_user_num_acc_same_ips():
    r_test = result_df.select("USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS")
    a_test = ans_df.select("USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS")

    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0


def test_flattener_feature():
    ans_flat_df = load_test_data(
        "data",
        "parquet_data",
        "flattener_tests",
        "ans_df.parquet",
    )

    r_test = result_df.select("Flattened_SM_AGENTNAME")
    a_test = ans_flat_df.select("Flattened_SM_AGENTNAME")

    print(r_test.schema)
    print(a_test.schema)
    # Size test
    assert r_test.count() == a_test.count()

    # content test
    assert r_test.subtract(a_test).count() == 0
