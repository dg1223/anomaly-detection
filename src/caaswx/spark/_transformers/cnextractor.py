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
    Input: A Spark dataframe
    Expected columns in the input dataframe

    Column Name                 Data type                                                          Description
    SM_USERNAME                  string                  The username for the user currently logged in with this session. Usernames encompass CNs along with abstract information about CMS and AMS requests.
                                                         It may contain SAML requests, NULLs and blank values.
    Output: Input dataframe with an additional column containing the extracted CommonNames from the SM_USERNAME's values

    Additional_Column_Name                                           Description                                                                                   Datatype
    this.getOrDefault("OutputCol")      Column containing the CommonNames for each user. It is an alpha-numeric string and it may contain NULL values.             string
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
            regexp_replace(dataset[self.getOrDefault("inputCol")], r".*(cn=)", ""),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(dataset[self.getOrDefault("outputCol")], r"(,.*)$", ""),
        )

        return dataset
