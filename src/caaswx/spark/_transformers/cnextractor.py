from pyspark import keyword_only
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
)
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType

from src.caaswx.spark._transformers.sparknativetransformer import \
    SparkNativeTransformer


class CnExtractor(SparkNativeTransformer, HasInputCol, HasOutputCol):
    """
    Creates a CN column using the existing SM_USERNAME column
    by:
    - Removing the "cn=" all chars before it
    - Removing the characters after the first comma (including the comma)
    Input: Dataframe containing an SM_USERNAME that has a CN

    Notes:
    - Assumes the "cn=" and its contents are not at the end of the SM_USERNAME
    - Reminder that dict must change if SM_USERNAME is no longer used
    Input: A Spark dataframe
    Columns from raw_logs: SM_RESOURCE, SM_TIMESTAMP
    Please refer to README.md for description.

    Output: Same dataframe with a CN column appended
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Column containing the CommonNames|
    | Default("   |          | for each user. It is an alpha-   |
    | OutputCol") |          | numeric string and it may contain|
    |             |          | NULL values.                     |
    +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(self):
        """
        :param setInputCol: Input column to be processed within the transformer which must contain "CN" strings like "cn=<AN_ALPHANUMERIC_STRING>"
        :param OutputCol: Name of the output column to be set after extracting the CN from the SM_USERNAME column's comma separated strings
        :type setInputCol: string
        :type OutputCol: string

        :Example:
        >>> from cnextractor import CnExtractor
        >>> cne = CnExtractor(setInputCol="SM_USERNAME", OutputCol="CN")
        >>> datafame_with_CN = cne.transform(input_dataset)
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
            regexp_replace(
                dataset[self.getOrDefault("inputCol")], r".*(cn=)", ""),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"(,.*)$", ""),
        )

        return dataset
