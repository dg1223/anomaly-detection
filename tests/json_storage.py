import json
import pyspark


def store_json(df: pyspark.sql.DataFrame, storage_path):
    data_map = map(lambda row: row.asDict(), df.collect())
    data = [x for x in data_map]

    schema = [json.loads(df.schema.json())]

    json_dict = {"data": data, "schema": schema}
    with open(storage_path, "w") as outfile:
        json.dump(json_dict, outfile, default=str, indent=4)

    return True
