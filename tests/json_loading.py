import json
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()


def load_json_data_and_schema(json_file_path: str):
    json_file_obj = open(json_file_path)
    json_data = json.load(json_file_obj)

    data = json_data["data"]
    for row in data:
        row["SM_TIMESTAMP"] = datetime.strptime(
            row["SM_TIMESTAMP"], "%Y-%m-%d %H:%M:%S.%f"
        )

    json_schema_field_details = json_data["schema"][0]["fields"]
    datatype_map = {
        "long": LongType(),
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "bool": BooleanType(),
    }
    schema_body = []
    for field_detail in json_schema_field_details:
        field_name = field_detail["name"]
        nullability = field_detail["nullable"]
        field_datatype = datatype_map[field_detail["type"]]
        schema_body.append(
            StructField(name=field_name, dataType=field_datatype, nullable=nullability)
        )

    spark_data = spark.createDataFrame(data, schema=StructType(schema_body))
    return spark_data, spark_data.schema
