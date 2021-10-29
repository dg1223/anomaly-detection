from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
)
from .sparknativetransformer import SparkNativeTransformer
from pyspark import keyword_only


class SMResourceCleaner(SparkNativeTransformer, HasInputCol, HasOutputCol):
    """
    Consolidates SM_RESOURCE elements to simplify redundant data, based
    off of the following criteria:

    1) SAML Requests
      Suggested Categorization: Strings containing the prefix '/cmsws' and
      substrings 'redirect' and 'SAML'. The URLs starting with '/SAMLRequest'.
      Action: Replace with the string '<SAML request>'
    2) Query strings
      Suggested Categorization: Strings containing the character '?' after the
      last occurrence of '/'.
      Action: Replace everything after the relevant '?' by '*'.
    3) URLs ending with '%'
      Strip off the trailing '%'
    4) URLs which start with 'SMASSERTIONREF' are quite long and contain the
    substring '/cmsws/public/saml2sso'.
      To cleanup these long URLs, replace the entire string with
      '/cmsws/public/saml2sso'.
    5) Other strings
      Suggested Categorization: Take whatever's left over from the previous
      two categories that isn't null.
      Action: Do nothing.

    Input: A Spark dataframe containing the following column:

    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | This is the resource, for example|
    | Default("   |          | a web page, that the user is     |
    | inputCol")  |          | requesting. SM_RESOURCE in       |
    |             |          | raw_logs for reference.          |
    +-------------+----------+----------------------------------+

    Output: A Spark Dataframe with the following features calculated:

    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Column containing the cleaned    |
    | Default("   |          | forms of different URLs with     |
    | outputCol") |          | respect to the aforementioned    |
    |             |          | cleaning strategies.             |
    +-------------+----------+----------------------------------+

    :param inputCol: Input column to be processed within the transformer
    :param outputCol: Name of the output column
    :type inputCol: string
    :type outputCol: string
    """

    @keyword_only
    def __init__(self):
        """
        :param inputCol: Input column to be processed within the transformer
        :param outputCol: Name of the output column
        :type inputCol: string
        :type outputCol: string
        """
        super(SMResourceCleaner, self).__init__()
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="Cleaned_SM_RESOURCE"
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        Sets params for the SM_RESOURCE Cleaner
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    sch_dict = {"SM_RESOURCE": ["SM_RESOURCE", StringType()]}

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("inputCol")],
                r"((\/cmsws).*((redirect).*(SAML)|(SAML).*(redirect))).*|\/("
                r"SAMLRequest).*",
                "<SAML Request>",
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"\?.*$", "?*"
            ),
        )

        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"\%$", ""
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")],
                r".*\%.*(\/cmsws\/public\/saml2sso).*",
                "/cmsws/public/saml2sso",
            ),
        )

        return dataset
