import re
from pyspark.ml import Transformer
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


class SMResourceCleaner(Transformer):
    """
    Consolidates SM_RESOURCE elements to simplify redundant data, based off of the following criteria:
    1) SAML Requests
      Suggested Categorization: Strings containing the prefix '/cmsws' and substrings 'redirect' and 'SAML'.
      Action: Replace with the string '<SAML request>'
    2) Query strings
      Suggested Categorization: Strings containing the character '?' after the last occurence of '/'.
      Action: Replace everything after the relevant '?' by '*'.
    3) Other strings
      Suggested Categorization: Take whatever's left over from the previous two categories that isn't null.
      Action: Do nothing.

    Input: The dataframe containing SM_RESOUCE that needs needs to be cleaned.
    Output: Dataframe appended with cleaned SM_RESOURCE.

    Notes: In some entries there may exist some long
    """

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(
                dataset["SM_RESOURCE"],
                r"((\/cmsws).*((redirect).*(SAML)|(SAML).*(redirect))).*|.*(SAMLRequest).*",
                "<SAML Request>",
            ),
        )
        dataset = dataset.withColumn(
            "Cleaned_SM_RESOURCE",
            regexp_replace(dataset["Cleaned_SM_RESOURCE"], r"\?.*$", "?*"),
        )

        return dataset
