import re
from pyspark.ml import Transformer
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import StringType
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer


class SMResourceCleaner(SparkNativeTransformer):
    """
    Consolidates SM_RESOURCE elements to simplify redundant data, based off of the following criteria:
    1) SAML Requests
      Suggested Categorization: Strings containing the prefix '/cmsws' and substrings 'redirect' and 'SAML'. The URLs starting with '/SAMLRequest'.
      Action: Replace with the string '<SAML request>'
    2) Query strings
      Suggested Categorization: Strings containing the character '?' after the last occurrence of '/'.
      Action: Replace everything after the relevant '?' by '*'.
    3) URLs ending with '%'
      Strip off the trailing '%'
    4) URLs which start with 'SMASSERTIONREF' are quite long and contain the substring '/cmsws/public/saml2sso'.
      To cleanup these long URLs, replace the entire string with '/cmsws/public/saml2sso'.
    5) Other strings
      Suggested Categorization: Take whatever's left over from the previous two categories that isn't null.
      Action: Do nothing.
    Input: The dataframe containing SM_RESOURCE that needs needs to be cleaned.
    Output: Dataframe appended with cleaned SM_RESOURCE.
    Notes: In some entries there may exist some long
    """

    sch_dict = {"SM_RESOURCE": ["SM_RESOURCE", StringType()]}

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(
                dataset["SM_RESOURCE"],
                r"((\/cmsws).*((redirect).*(SAML)|(SAML).*(redirect))).*|\/(SAMLRequest).*",
                "<SAML Request>",
            ),
        )
        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(dataset["Cleaned_SM_RESOURCE"], r"\?.*$", "?*"),
        )

        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(dataset["Cleaned_SM_RESOURCE"], r"\%$", ""),
        )
        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(
                dataset["Cleaned_SM_RESOURCE"],
                r".*\%.*(\/cmsws\/public\/saml2sso).*",
                "/cmsws/public/saml2sso",
            ),
        )

        return dataset
