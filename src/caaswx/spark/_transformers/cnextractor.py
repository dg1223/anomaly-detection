from pyspark import keyword_only
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
)
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType

from .sparknativetransformer import SparkNativeTransformer


class CnExtractor(SparkNativeTransformer, HasInputCol, HasOutputCol):
    """
    Creates an Output Column (Default="CN") using the Input Column
    (Default="SM_USERNAME) by:
    - Removing all characters before "cn="
    - Removing the characters after the first comma (including the comma)

    Notes:
    - Assumes the "cn=" and its contents are not at the end of the SM_USERNAME
    - Reminder that dict must change if SM_USERNAME is no longer used

    Input: A Spark dataframe the following column:

    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | The username for the user        |
    | Default("   |          | currently logged in with this    |
    | inputCol")  |          | session. SM_USERNAME in          |
    |             |          | raw_logs for reference.          |
    +-------------+----------+----------------------------------+

    Please refer to README.md for further description of raw_logs.

    Output: A Spark Dataframe with the following features calculated:

    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Column containing the CommonNames|
    | Default("   |          | for each user. It is an alpha-   |
    | outputCol") |          | numeric string and it may contain|
    |             |          | NULL values.                     |
    +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(self):
        """
        :param setInputCol: Input column to be processed within the
        transformer which must contain "CN" strings like
        "cn=<AN_ALPHANUMERIC_STRING>"
        :param OutputCol: Name of the output
        column to be set after extracting the CN from the SM_USERNAME
        column's comma separated strings
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
                dataset[self.getOrDefault("inputCol")], r".*(cn=)", ""
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"(,.*)$", ""
            ),
        )

        return dataset
