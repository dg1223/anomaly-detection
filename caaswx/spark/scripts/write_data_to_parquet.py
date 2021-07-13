from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.functions import col

sc = SparkContext("local")
spark = SparkSession(sc)

path_flattener = "caa-streamworx/caaswx/spark/data/flattener/"

# function to write test datasets to parquet files based on a specific schema


def write_parquet_flattener_user(
    testDataset, expectedDataset, td_file_name, expected_df_file_name
):
    td_file_path = path_flattener + td_file_name
    expected_df_file_path = path_flattener + expected_df_file_name

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

    test_df = spark.createDataFrame(testDataset, schema=test_user_schema)
    test_df = test_df.withColumn(
        "SM_TIMESTAMP", col("SM_TIMESTAMP_TEMP").cast("timestamp")
    )
    test_df = test_df.drop("SM_TIMESTAMP_TEMP")
    test_df.write.parquet(td_file_path)

    expected_result_df = spark.createDataFrame(
        expectedDataset, schema=expected_result_user_schema
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


# test data and expected result data go here
user_one_window_case = [
    ("User_A", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_C", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:04:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:08:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:02:00.000+0000"),
    ("User_C", "Resource_C", "2018-02-01T00:05:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:01.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:05:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:08:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:08:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T00:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:08.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:09.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:40.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:50.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_E", "Resource_C", "2018-02-01T00:09:00.000+0000"),
]
expected_user_one_window_10 = [
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        [
            "Resource_A",
            "Resource_B",
            "Resource_C",
            "Resource_D",
            "Resource_E",
            "Resource_F",
            "Resource_G",
            "Resource_H",
            "Resource_I",
            "Resource_J",
        ],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_C", "Resource_B", "Resource_A", "Resource_D"],
    ),
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        [
            "Resource_A",
            "Resource_B",
            "Resource_C",
            "Resource_D",
            "Resource_E",
            "Resource_B",
            "Resource_D",
            "Resource_A",
            "Resource_E",
        ],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_C"],
    ),
]

user_multiple_window_case = [
    ("User_A", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_C", "2018-02-01T02:00:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:04:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:20:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:20:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:20:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:02:00.000+0000"),
    ("User_C", "Resource_C", "2018-02-01T00:05:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:20:01.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:20:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:20:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:08:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:08:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T01:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T01:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T01:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T01:14:08.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T01:14:09.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T01:14:40.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T01:14:50.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_E", "Resource_C", "2018-02-01T01:09:00.000+0000"),
]
expected_user_multiple_window_case = [
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B"],
    ),
    (
        "User_A",
        {
            "start": "2018-02-01T02:00:00.000+0000",
            "end": "2018-02-01T02:15:00.000+0000",
        },
        ["Resource_C"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:15:00.000+0000",
            "end": "2018-02-01T00:30:00.000+0000",
        },
        ["Resource_D", "Resource_A", "Resource_E"],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_C"],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:15:00.000+0000",
            "end": "2018-02-01T00:30:00.000+0000",
        },
        ["Resource_D", "Resource_B", "Resource_A"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T01:00:00.000+0000",
            "end": "2018-02-01T01:15:00.000+0000",
        },
        ["Resource_J", "Resource_B", "Resource_C", "Resource_A", "Resource_B"],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A"],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T01:00:00.000+0000",
            "end": "2018-02-01T01:15:00.000+0000",
        },
        ["Resource_C"],
    ),
]

user_duplicate_resources = [
    ("User_A", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:04:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:08:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:02:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:01.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:05:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:08:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:08:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T00:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:08.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:09.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:40.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:50.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:09:00.000+0000"),
]
expected_user_duplicate_resources = [
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_B"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_C", "Resource_C"],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_B", "Resource_A", "Resource_D"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_D"],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_A"],
    ),
]

user_duplicate_rows = [
    ("User_A", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:08:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:02:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:05:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:08:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:08:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T00:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:06:00.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:01:00.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:01:00.000+0000"),
]
expected_user_duplicate_rows = [
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_B"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        [
            "Resource_A",
            "Resource_B",
            "Resource_C",
            "Resource_C",
            "Resource_C",
            "Resource_B",
            "Resource_D",
        ],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_B", "Resource_A", "Resource_D"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        [
            "Resource_A",
            "Resource_A",
            "Resource_A",
            "Resource_A",
            "Resource_A",
            "Resource_B",
            "Resource_C",
            "Resource_D",
            "Resource_D",
        ],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_A"],
    ),
]

user_based_grouping = [
    ("User_A", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_C", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:02:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:04:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:08:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_C", "Resource_C", "2018-02-01T00:02:01.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:05:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:02:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T00:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:08.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:09.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:40.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:50.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:00:00.000+0000"),
    ("User_E", "Resource_B", "2018-02-01T00:01:00.000+0000"),
]
expected_user_based_grouping = [
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C"],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_A", "Resource_D"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B", "Resource_C", "Resource_D", "Resource_E"],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_A", "Resource_B"],
    ),
]

user_shuffled_data = [
    ("User_A", "Resource_A", "2018-02-01T00:10:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:10:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:02:00.000+0000"),
    ("User_A", "Resource_B", "2018-02-01T00:01:00.000+0000"),
    ("User_A", "Resource_C", "2018-02-01T00:02:00.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:10:00.000+0000"),
    ("User_C", "Resource_C", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_C", "2018-02-01T00:03:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:04:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:05:00.000+0000"),
    ("User_B", "Resource_B", "2018-02-01T00:06:00.000+0000"),
    ("User_B", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_B", "Resource_A", "2018-02-01T00:08:00.000+0000"),
    ("User_B", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_C", "Resource_B", "2018-02-01T00:05:01.000+0000"),
    ("User_C", "Resource_A", "2018-02-01T00:05:02.000+0000"),
    ("User_C", "Resource_D", "2018-02-01T00:07:00.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:10:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:08:00.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:08:01.000+0000"),
    ("User_D", "Resource_D", "2018-02-01T00:08:04.000+0000"),
    ("User_D", "Resource_E", "2018-02-01T00:09:00.000+0000"),
    ("User_D", "Resource_F", "2018-02-01T00:09:03.000+0000"),
    ("User_D", "Resource_G", "2018-02-01T00:11:00.000+0000"),
    ("User_D", "Resource_H", "2018-02-01T00:12:00.000+0000"),
    ("User_D", "Resource_I", "2018-02-01T00:13:00.000+0000"),
    ("User_D", "Resource_J", "2018-02-01T00:14:00.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:01.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:03.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:08.000+0000"),
    ("User_D", "Resource_B", "2018-02-01T00:14:09.000+0000"),
    ("User_D", "Resource_C", "2018-02-01T00:14:40.000+0000"),
    ("User_D", "Resource_A", "2018-02-01T00:14:50.000+0000"),
    ("User_E", "Resource_A", "2018-02-01T00:10:00.000+0000"),
    ("User_E", "Resource_C", "2018-02-01T00:09:00.000+0000"),
]
expected_user_shuffled_data = [
    (
        "User_A",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_B", "Resource_C", "Resource_A",],
    ),
    (
        "User_B",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_B"],
    ),
    (
        "User_C",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_C", "Resource_B", "Resource_A", "Resource_D", "Resource_A"],
    ),
    (
        "User_D",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_B", "Resource_C", "Resource_D", "Resource_E", "Resource_F"],
    ),
    (
        "User_E",
        {
            "start": "2018-02-01T00:00:00.000+0000",
            "end": "2018-02-01T00:15:00.000+0000",
        },
        ["Resource_C", "Resource_A"],
    ),
]

# creating tests data files along with the corresponding expected outcomes
write_parquet_flattener_user(
    testDataset=user_one_window_case,
    expectedDataset=expected_user_one_window_10,
    td_file_name="user_one_window_10.parquet",
    expected_df_file_name="expected_user_one_window_10.parquet",
)

write_parquet_flattener_user(
    testDataset=user_multiple_window_case,
    expectedDataset=expected_user_multiple_window_case,
    td_file_name="user_multiple_window_5.parquet",
    expected_df_file_name="expected_user_multiple_window_5.parquet",
)

write_parquet_flattener_user(
    testDataset=user_duplicate_resources,
    expectedDataset=expected_user_duplicate_resources,
    td_file_name="user_duplicate_resources.parquet",
    expected_df_file_name="expected_user_duplicate_resources.parquet",
)

write_parquet_flattener_user(
    testDataset=user_duplicate_rows,
    expectedDataset=expected_user_duplicate_rows,
    td_file_name="user_duplicate_rows.parquet",
    expected_df_file_name="expected_user_duplicate_rows.parquet",
)

write_parquet_flattener_user(
    testDataset=user_based_grouping,
    expectedDataset=expected_user_based_grouping,
    td_file_name="user_based_grouping.parquet",
    expected_df_file_name="expected_user_based_grouping.parquet",
)

write_parquet_flattener_user(
    testDataset=user_shuffled_data,
    expectedDataset=expected_user_shuffled_data,
    td_file_name="user_shuffled_data.parquet",
    expected_df_file_name="expected_user_shuffled_data.parquet",
)
