# Module containing the utility files and mixins

from pyspark.ml.param.shared import (
    HasInputCol,
    HasInputCols,
    HasOutputCol,
    HasOutputCols,
)
from pyspark.ml.param import Param, Params

import os
from pyspark import keyword_only
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.functions import col


class HasTypedInputCol(HasInputCol):
    """
    A mixin for entities that maintain the input column type.
    """

    inputColType = Param(Params._dummy(), "inputColType", "input column type")

    def __init__(self):
        super(HasTypedInputCol, self).__init__()

    def getInputColType(self):
        """
        Gets type of input column.

        :return: Param inputColType
        :rtype: StringType
        """
        return self.getOrDefault("inputColType")


class HasTypedInputCols(HasInputCols):
    """
    A mixin for entities that maintain multiple columns and their types
    """

    inputColsType = Param(
        Params._dummy(), "inputColsType", "input column type"
    )

    def __init__(self):
        super(HasTypedInputCols, self).__init__()

    def getInputColType(self):
        """
        Gets multiple input column types

        :return: Param inputColsType
        :rtype: ArrayType
        """
        return self.getOrDefault("inputColsType")


class HasTypedOutputCol(HasOutputCol):
    """
    A mixin for entities that maintain an output column type
    """

    outputColType = Param(
        Params._dummy(), "outputColType", "output column type"
    )  # <- this is a string

    def __init__(self):
        super(HasTypedOutputCol, self).__init__()

    def getOutputColType(self):
        """
        Gets the output column type

        :return: Param outputColType
        :rtype: StringType
        """
        return self.getOrDefault("outputColType")


class HasTypedOutputCols(HasOutputCols):
    """
    A mixin for entities that maintain multiple output columns and their types
    """

    outputColsType = Param(
        Params._dummy(), "outputColsType", "output column type"
    )

    def __init__(self):
        super(HasTypedOutputCols, self).__init__()

    def getOutputColType(self):
        """
        Gets multiple output column types

        :return: Param outputColsType
        :rtype: ArrayType
        """
        return self.getOrDefault("outputColsType")


# From Utils Folder
sc = SparkContext("local")
spark = SparkSession(sc)

PATH_DATA = "mnt/repo-related/caa-streamworx/caaswx/spark/parquet_data/"
PATH_FLATTENER = (
    "mnt/repo-related/caa-streamworx/caaswx/spark/parquet_data" "/flattener/ "
)


spark = SparkSession.builder.getOrCreate()


def load_test_data(*args):
    """load test parquet_data from parquet_data by passing file name"""
    cwd = os.getcwd()
    path = os.path.join(cwd, *args)
    df = spark.read.parquet(path)
    return df


def load_path(*args):
    """load the path of any file for testing"""
    cwd = os.getcwd()
    path = os.path.join(cwd, *args)
    return path


def write_parquet(file_path: str, schema, data):
    data_frame = spark.createDataFrame(data, schema=schema)
    data_frame.write.parquet(file_path)
    return data_frame


def load_parquet(*argv: str):
    cwd = os.getcwd()
    path = os.path.join(cwd, *argv)
    return spark.read.parquet(path)


def null_swap(st1, st2):
    """
    Function to swap datatype null parameter within a nested dataframe
    schema.

    :param st1: The StructType that will have its nullability changed.
    :type st1: StructType

    :param st2: The StructType that will be used as the standard for changing
    nullability.
    :type st2: StructType
    """
    for sf in st1:
        sf.nullable = st2[sf.name].nullable
        if isinstance(sf.dataType, StructType):
            null_swap(sf.dataType, st2[sf.name].dataType)
        if isinstance(sf.dataType, ArrayType):
            sf.dataType.containsNull = st2[sf.name].dataType.containsNull


def struct_field_compare(sf1, sf2):
    """
    Compares two StructField against one another.

    :param sf1: The first StructField to be tested for comparison.
    :type sf1: StructField

    :param sf2:  The second StructField to be tested for comparison.
    :type sf2: StructField

    :return: Returns True if two structfields are the same, else False.
    :rtype: Boolean
    """
    if isinstance(sf1.dataType, StructType):
        return struct_compare(sf1.dataType, sf2.dataType)
    else:
        return sf1.dataType == sf2.dataType


def struct_compare(st1, st2):
    """
    Compares two StructType against one another.

    :param st1: The StructType that will be tested for being a subset.
    :type st1: StructType

    :param st2: The StructType that will be tested against(potential superset).
    :type st2: StructType

    :return: Returns True if one schema is a subset of another, else False.
    :rtype: Boolean
    """
    if not {sf.name for sf in st1}.issubset({sf.name for sf in st2}):
        return False
    for sf in st1:
        if not struct_field_compare(sf, st2[sf.name]):
            return False
    return True


def schema_is_subset(schema1, schema2, compare_nulls=False):
    """
    Tests whether each StructField of schema2 is contained in schema1.
    Comparison ignores nullable field if compare_nulls is False.

    :param schema1: The StructType that is the incoming schema(schema to be
    checked).
    :type schema1: StructType

    :param schema2: The StructType that will be the required schema(schema to
    be checked against).
    :type schema2: StructType

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean

    :return: Returns true if each StructField of schema2 is contained in
    schema1.
    :rtype: Boolean
    """

    if compare_nulls:
        return all([x in schema2 for x in schema1])
    else:
        return struct_compare(schema1, schema2)


def schema_is_equal(schema1, schema2, compare_nulls=False):
    """
    Test Whether each StructField of schema2 is contained in
    schema1, and vice versa. Comparison ignores nullable field if compare_nulls
    is False.

    :param schema1: The first StructType to be tested for equality.
    :type schema1: StructType

    :param schema2: The second StructType to be tested for equality.
    :type schema2: StructType

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean

    :return: Returns true if Returns true if each StructField of schema2 is
    contained in schema1, and vice versa.
    :rtype: Boolean
    """
    if compare_nulls:
        return schema1 == schema2
    else:
        return struct_compare(schema1, schema2) and struct_compare(
            schema2, schema1
        )


def schema_contains(schema, structfield, compare_nulls=False):
    """
    Returns true if structfield is a field in schema, ignoring nullability if
    compare_nulls is false.

    :param schema: The StructType that the StructField will be tested against.
    :type schema: StructType

    :param structfield: The StructField that will be tested for existence in
    the schema.
    :type structfield: StructField

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean
    """
    if compare_nulls:
        return structfield in schema
    return struct_compare(StructType([structfield]), schema)


def schema_concat(schema_list):
    """
    Returns a Schema, without any duplicates. If their is a duplicate name
    amongst the structfields with differing datatype an Exception will be
    thrown.
    :param schema_list: The StructType that will be processed.
    :type schema_list: StructType
    ...
    :raises Exception: Duplicate Name, Type Mismatch ERROR.
    ...
    :return: A Schema with no Duplicates.
    :rtype: StructType
    """
    duplicate_rem = set()
    for st in schema_list:
        for sf in st:
            duplicate_rem.add(sf)

    schema_name_list = [sf.name for sf in duplicate_rem]
    if len(schema_name_list) != len(set(schema_name_list)):
        raise Exception("Duplicate Name, Type Mismatch ERROR.")

    return StructType(list(duplicate_rem))


def schema_test(base_schema, test_schema):
    """
    This function tests whether test_schema is a subset of base_schema,
    and makes the nullability values of both schemas the same.

    """

    def nullTest(st1, st2):
        """
        Function to swap datatype null parameter within a nested
        dataframe schema
        """
        if not {sf.name for sf in st1}.issubset({sf.name for sf in st2}):
            raise ValueError(
                "Keys for first schema aren't a subset of " "the second."
            )
        for sf in st1:
            sf.nullable = st2[sf.name].nullable
            if isinstance(sf.dataType, StructType):
                if not {sf.name for sf in st1}.issubset(
                    {sf.name for sf in st2}
                ):
                    raise ValueError(
                        "Keys for first schema aren't a subset of the "
                        "second. "
                    )
                nullTest(sf.dataType, st2[sf.name].dataType)
            if isinstance(sf.dataType, ArrayType):
                sf.dataType.containsNull = st2[sf.name].dataType.containsNull

    nullTest(test_schema, base_schema)
    if any([x not in base_schema for x in test_schema]):
        raise ValueError(
            "Keys for first schema aren't a subset of the " "second."
        )


class WriteDataToParquet:
    """class to implement various kinds of methods to write parquets"""

    test_dataset = 0
    expected_dataset = 0
    td_file_name = 0
    expected_df_file_name = 0
    td_schema = 0
    expected_data_schema = 0

    def __init__(self, td, ed, td_fn, ed_fn):
        self.test_dataset = td
        self.expected_dataset = ed
        self.td_file_name = td_fn
        self.expected_df_file_name = ed_fn

    # function to write test datasets to parquet files based on a specific
    # schema

    def write_parquet_flattener_user(self):
        """Write datasets into parquet files for testing resources flattener"""
        td_file_path = PATH_FLATTENER + self.td_file_name
        expected_df_file_path = PATH_FLATTENER + self.expected_df_file_name

        # schema for creating a simple test dataset
        test_user_schema = StructType(
            [
                StructField("SM_USERNAME", StringType()),
                StructField("SM_RESOURCE", StringType()),
                StructField("SM_TIMESTAMP_TEMP", StringType()),
            ]
        )

        # schema for expected flattener result based on SM_USERNAME
        expected_result_user_schema = StructType(
            [
                StructField("SM_USERNAME", StringType()),
                StructField(
                    "window_temp",
                    StructType(
                        [
                            StructField("start", StringType()),
                            StructField("end", StringType()),
                        ]
                    ),
                ),
                StructField("SM_RESOURCE", ArrayType(StringType())),
            ]
        )

        test_df = spark.createDataFrame(
            self.test_dataset, schema=test_user_schema
        )
        test_df = test_df.withColumn(
            "SM_TIMESTAMP", col("SM_TIMESTAMP_TEMP").cast("timestamp")
        )
        test_df = test_df.drop("SM_TIMESTAMP_TEMP")
        test_df.write.parquet(td_file_path)

        expected_result_df = spark.createDataFrame(
            self.expected_dataset, schema=expected_result_user_schema
        )
        expected_result_df = expected_result_df.withColumn(
            "window",
            col("window_temp").cast(
                StructType(
                    [
                        StructField("start", TimestampType()),
                        StructField("end", TimestampType()),
                    ]
                )
            ),
        )
        expected_result_df = expected_result_df.drop("window_temp")
        expected_result_df = expected_result_df.select(
            "SM_USERNAME", "window", "SM_RESOURCE"
        )
        expected_result_df.write.parquet(expected_df_file_path)
        return test_df, expected_result_df

    def write_parquet_general(self, test_schema, expected_result_schema):
        """Write datasets into parquet files by specifying schema"""
        td_file_path = PATH_DATA + self.td_file_name
        expected_df_file_path = PATH_DATA + self.expected_df_file_name
        test_df = spark.createDataFrame(self.test_dataset, schema=test_schema)
        test_df.write.parquet(td_file_path)
        expected_result_df = spark.createDataFrame(
            self.expected_dataset, schema=expected_result_schema
        )
        expected_result_df.write.parquet(expected_df_file_path)
        return test_df, expected_result_df


class HasInputSchema:
    """
    A mixin for entities which maintain an input schema.
    """

    input_schema = Param(
        Params._dummy(),
        "input_schema",
        "Param specifying the required schema of the input dataframe.",
    )

    @keyword_only
    def __init__(self):
        super(HasInputSchema, self).__init__()

    def schema_is_admissable(self, schema: StructType, compare_nulls=False):
        """
        Returns ``True`` if each :class:`StructField` of ``schema``
        is contained in this entity's schema, modulo nullability if
        ``compare_nulls`` is ``False``.
        :param schema: The input schema to be checked.
        :param compare_nulls: If this flag is ``False``, comparison
        of :class:`pyspark.sql.Types.StructField`'s is done
        ignoring nullability.
        :type schema: :class:`pyspark.sql.Types.StructType`
        :type compare_nulls: ``boolean``
        """
        return schema_is_subset(
            self.input_schema, schema, compare_nulls=compare_nulls
        )

    def set_input_schema(self, schema: StructType):
        """
        Sets this entity's input schema.
        :param schema: The input schema to be set.
        :type schema: :class:`pyspark.sql.types.StructType`
        """
        self.set(self.input_schema, schema)

    def get_input_schema(self):
        """
        Gets this entity's input schema.
        """
        return self.getOrDefault("input_schema")
