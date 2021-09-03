from pyspark.ml import Transformer
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark import keyword_only


class CnExtractor(Transformer):
    """
    Creates a CN column using the existing SM_USERNAME column
    by:
    - Removing the "cn=" at the beginning
    - Removing the characters after the first comma (including the comma)

    Input: Dataframe containing an SM_USERNAME that has a CN
    Output: Same dataframe with a CN column appended

    Note: Assumes "cn=" will always be at the beginning of the SM_USERNAME
    """

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            "CN", regexp_replace(dataset["SM_USERNAME"], r"^(cn=)", "")
        )
        dataset = dataset.withColumn("CN", regexp_replace(dataset["CN"], r"(,.*)$", ""))

        return dataset
