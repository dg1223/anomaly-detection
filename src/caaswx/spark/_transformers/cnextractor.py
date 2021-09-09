from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.ml.param.shared import (
    TypeConverters,
    Param,
    Params,
    HasInputCol,
    HasOutputCol,
)
from pyspark import keyword_only


class CnExtractor(SparkNativeTransformer, HasInputCol, HasOutputCol):
    """
    Creates a CN column using the existing SM_USERNAME column
    by:
    - Removing the "cn=" all chars before it
    - Removing the characters after the first comma (including the comma)
    Input: Dataframe containing an SM_USERNAME that has a CN
    Output: Same dataframe with a CN column appended
    Notes:
    - Assumes the "cn=" and its contents are not at the end of the SM_USERNAME
    - Reminder that dict must change if SM_USERNAME is no longer used
    """

    @keyword_only
    def __init__(self):
        """
        init (by default)
        inputCol: SM_USERNAME
        outputCol: CN
        """
        super(CnExtractor, self).__init__()
        self._setDefault(inputCol="SM_USERNAME", outputCol="CN")
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        Sets params for the CN extractor
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    sch_dict = {
        "SM_USERNAME": ["SM_USERNAME", StringType()],
    }

    def _transform(self, dataset):
        """
        Transform the new CN column
        Params:
        - dataset: dataframe containing SM_USERNAME, to have CN extracted
        Returns:
        - dataset with CN appended
        """
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(dataset[self.getOrDefault("inputCol")], r".*(cn=)", ""),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(dataset[self.getOrDefault("outputCol")], r"(,.*)$", ""),
        )

        return dataset
