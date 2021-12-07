from ._transformers.agentstringflattener import (  # noqa: F401
    AgentStringFlattener,
)
from ._transformers.cnextractor import CnExtractor  # noqa: F401
from ._transformers.ipfeaturegenerator import IPFeatureGenerator  # noqa: F401
from ._transformers.resourcesflattener import ResourcesFlattener  # noqa: F401
from ._transformers.sessionfeaturegenerator import (  # noqa: F401,E501
    SessionFeatureGenerator,
)
from ._transformers.serverfeaturegenerator import (  # noqa: F401,E501
    ServerFeatureGenerator,
)
from ._transformers.smresourcecleaner import SMResourceCleaner  # noqa: F401
from ._transformers.userfeaturegenerator import (  # noqa: F401
    UserFeatureGenerator,
)

import httpagentparser
import pyspark.sql.functions as f
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params, HasOutputCol
from pyspark.sql.functions import (
    window,
    col,
    udf,
)
from pyspark.sql.types import (
    StringType,
    TimestampType,
)

from .sparknativetransformer import SparkNativeTransformer


class AgentStringFlattener(SparkNativeTransformer, HasOutputCol):
    """
     A transformer that parses a target Flanttened_SM_AGENTNAME column of a
     spark dataframe.
    Input: A Spark dataframe containing Flanttened_SM_AGENTNAMESM_AGENTNAME,
    Output: A Spark Dataframe with the following feature appeneded to it.
     +-------------+----------+----------------------------------+
     | Column_Name | Datatype | Description                      |
     +=============+==========+==================================+
     | self.getOrD |  array   | Contains a list of parsed        |
     | efault("out | <string> | agentnames                       |
     | putCol")    |          |                                  |
     +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(
        self,
    ):
        """
        :param outputCol: Name of parsed agent string column
        :Example:
        >>> from agentstringflattener import AgentStringFlattener
        >>> flattener = AgentStringFlattener()
        >>> features = flattener.transform(input_dataset)
        """
        super(AgentStringFlattener, self).__init__()
        self._setDefault(
            outputCol="Parsed_Agent_String",
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
    ):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None,
        outputCol=None,
        thresholds=None, inputCols=None, outputCols=None)
        Sets params for this AgentStringFlattener.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def http_parser(self, value):

        base = []
        for string in value:
            if len(string.split(" ")) == 1:
                if None not in base:
                    base.append(None)
            else:
                parsed_string = httpagentparser.detect(string)
                if parsed_string not in base:
                    base.append(parsed_string)

        return base

    sch_dict = {}

    def _transform(self, dataset):
        """
        Overridden function which flattens the input dataset w.r.t URLs
        Input : Siteminder dataframe with a column with Flattened
        URLs merged into lists
        Output : Pasrsed URLs merged into lists
        """
        http_parser_udf = udf(self.http_parser, StringType())
        df = dataset.withColumn(
            self.getOrDefault("outputCol"),
            http_parser_udf(col("Flattened_SM_AGENTNAME")),
        ).drop("Flattened_SM_AGENTNAME")
        return df


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
