# from src.caaswx.spark.scripts.loadWriteParquet import writeParquet
import json

import pyspark.sql.functions as f
import pyspark.sql.types
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.session import SparkSession
import os
spark = SparkSession.builder.getOrCreate()

test_schema = StructType(
    [
        StructField("SM_CLIENTIP", StringType()),
        StructField("SM_AGENTNAME", StringType()),
        StructField("SM_TIMESTAMP", StringType()),
    ]
)

ans_schema = StructType(
    [
        StructField("SM_CLIENTIP", StringType()),
        StructField(
            "windowtmp",
            StructType(
                [
                    StructField("start", StringType(),False),
                    StructField("end", StringType()),
                ]
            ),
            False,
        ),
        StructField("Parsed_Agent_String", StringType()),
    ]
)

test_1_data = [
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84 Safari/537.36",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84 Safari/537.36",
        "2018-01-01T18:32:26.000+0000",
    ),
]

ans_1_data = [
    (
        "User_A",
        {
            "start": "2018-01-01T18:30:00.000+0000",
            "end": "2018-01-01T18:45:00.000+0000",
        },
        [
            {
                "platform": {"name": "Mac OS", "version": "X 10.12.6"},
                "os": {"name": "Macintosh"},
                "bot": False,
                "flavor": {"name": "MacOS", "version": "X 10.12.6"},
                "browser": {"name": "Chrome", "version": "63.0.3239.84"},
            }
        ],
    )
]

# df1 = spark.createDataFrame(test_1_data, schema=test_schema)
# df1_filePath = "/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/test_data_1_agentflattener.parquet"
# # df1.write.json(df1_filePath)
# df1.write.parquet(df1_filePath)

df2 = spark.createDataFrame(ans_1_data, schema=ans_schema)
# df2 = df2.withColumn(
#             "window",
#             f.col("windowtmp").cast(
#                 StructType(
#                     [
#                         StructField("start", TimestampType()),
#                         StructField("end", TimestampType()),
#                     ]
#                 )
#             ),
#         ).drop("windowtmp")
df2 = df2.select(["SM_CLIENTIP", "windowtmp", "Parsed_Agent_String"])
df2_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_1_agentflattener.json"
df2_schema_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_1_schema_agentflattener.json"

schema = df2.schema.json()
with open(df2_schema_filePath, 'w') as outfile:
    json.dump(schema, outfile)

# print("WHAT IT SHOULD BE:")
# print(pyspark.sql.types.StructType.fromJson(json.loads(schema)))

with open(df2_schema_filePath) as json_file:
    ans_1_data_schema = json.load(json_file)
# reader = pyspark.sql.DataFrameReader()
# df = reader
ans_1_data_schema = pyspark.sql.types.StructType.fromJson(json.loads(ans_1_data_schema))
print("WHAT WE IMPOSE:")
print(ans_1_data_schema)


# df = spark.read.format("json").load(path=df2_filePath, schema=ans_1_data_schema)
# df = spark.read.json(path=df2_filePath, schema=ans_1_data_schema)
df = spark.read.format("parquet").load(path="/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/ans_data_1_agentflattener.parquet", schema=ans_1_data_schema)
# df.show()

def nullSwap(st1, st2):
    for sf in st1:
        sf.nullable = st2[sf.name].nullable
        if isinstance(sf.dataType, StructType):
            nullSwap(sf.dataType, st2[sf.name].dataType)

nullSwap(df.schema,ans_1_data_schema)
# for struct_field in df.schema:
#     if struct_field.name == "window":
#         if(type(struct_field.dataType) == StructType):
#             for struct_prime in struct_field.dataType:
#                 if struct_prime.name == "start":
#                     struct_field.struct_prime.nullable = False

print(df.schema)

# df2.write.json(df2_filePath)
# df2.write.parquet(("/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/ans_data_1_agentflattener.parquet"))






"""
#DF = spark.read.json(df2_filePath)
#print(DF.schema)
#
#
# df1 = writeParquet(
#         "/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
#         "/test_data_1_agentflattener.parquet",
#         test_schema,
#         test_1_data,
#     )
#
# df2 = spark.createDataFrame(ans_1_data, schema=ans_schema)
# df2 = df2.withColumn(
#             "window",
#             f.col("windowtmp").cast(
#                 StructType(
#                     [
#                         StructField("start", TimestampType()),
#                         StructField("end", TimestampType()),
#                     ]
#                 )
#             ),
#         ).drop("windowtmp")
# # for struct_field in df2.schema:
# #     if struct_field.name in ["window"]:
# #         struct_field.nullable = False
# # df2 = spark.createDataFrame(df2.rdd, df2.schema)
# df2 = df2.select(["SM_CLIENTIP", "window", "Parsed_Agent_String"])
# filePath = "/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/ans_data_1_agentflattener.parquet"
# df2.write.parquet(filePath)
# # df2 = writeParquet(
# #         "/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests"
# #         "/ans_data_1_agentflattener.parquet",
# #         ans_schema,
# #         ans_1_data,
# #     )
#
#
#
#
#
#
ans_schema = StructType(
    [
        StructField("SM_CLIENTIP", StringType()),
        StructField(
            "windowtmp",
            StructType(
                [
                    StructField("start", StringType()),
                    StructField("end", StringType()),
                ]
            ),
            False,
        ),
        StructField("SM_AGENTNAME", ArrayType(StringType(), False), False),
    ]
)

test_2_data = [
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84 Safari/537.36",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84",
        "2018-01-01T18:32:26.000+0000",
    ),
    (
        "User_A",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.84 Safari/537.36",
        "2018-01-01T18:32:26.000+0000",
    ),
]

ans_2_data = [
    (
        "User_A",
        {
            "start": "2018-01-01T18:30:00.000+0000",
            "end": "2018-01-01T18:45:00.000+0000",
        },
        [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/63.0.3239.84",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/63.0.3239.84 Safari/537.36",
        ],
    )
]

df3 = spark.createDataFrame(test_2_data, schema=test_schema)
df3_filePath = "/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/test_data_2_agentflattener.parquet"
# df3.write.json(df3_filePath)
df3.write.parquet(df3_filePath)

df4 = spark.createDataFrame(ans_2_data, schema=ans_schema)
df4 = df4.withColumn(
            "window",
            f.col("windowtmp").cast(
                StructType(
                    [
                        StructField("start", TimestampType()),
                        StructField("end", TimestampType()),
                    ]
                )
            ),
        ).drop("windowtmp")
df4 = df4.select(["SM_CLIENTIP", "window", "SM_AGENTNAME"])
df4_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_2_agentflattener.json"
# df4.write.json(df4_filePath)
df4.write.parquet("/home/nds838/Documents/caa-streamworx/data/parquet_data/agent_flattener_tests/ans_data_2_agentflattener.parquet")

df4_schema_filePath = "/home/nds838/Documents/caa-streamworx/data/JSON/agent_flattener_tests/ans_data_2_schema_agentflattener.json"
schema = df4.schema.json()
print(pyspark.sql.types.StructType.fromJson(schema))

# DATAFRAME READER WITH JSON AND IMPOSE SCHEMA


# DATAFRAME READER WITH PARQUET AND IMPOSE SCHEMA 


# with open(df4_schema_filePath, 'w') as outfile:
#     json.dump(schema, outfile)

# df4.write.parquet(filePath)

# df1.show()
# df2.show()
# print(df2.schema)
# df3.show()
# df4.show()
# print(df4.schema)"""