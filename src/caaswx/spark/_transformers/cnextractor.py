from pyspark.ml import Transformer
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.ml.param.shared import TypeConverters, Param, Params, HasInputCol, HasOutputCol
from pyspark import keyword_only


class CnExtractor(Transformer, HasInputCol, HasOutputCol):
    """
    Creates a CN column using the existing SM_USERNAME column
    by:
    - Removing the "cn=" all chars before it
    - Removing the characters after the first comma (including the comma)

    Input: Dataframe containing an SM_USERNAME that has a CN
    Output: Same dataframe with a CN column appended

    Notes:
    - Assumes the "cn=" and its contents are not at the end of the SM_USERNAME
    """

    @keyword_only
    def __init__(self):
        super(CnExtractor, self).__init__()

        self._setDefault(
            inputCol="SM_USERNAME",
            outputCol="CN"
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        Sets params for the CN extractor
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_input_col(self, value):
        self._set(inputCol=value)

    def set_output_col(self, value):
        self._set(outputCol=value)

    def _transform(self, dataset):
        #     print(type(self.inputCol))
        #     input = ""
        inputc = self.inputCol
        #     input = "SM_USERNAME"
        outputc = "CN2"
        print(inputc)
        #     print(getInputCol())

        dataset = dataset.withColumn(
            outputc,
            regexp_replace(
                dataset[inputc],
                r".*(cn=)",
                ""
            )
        )
        dataset = dataset.withColumn(
            outputc,
            regexp_replace(
                dataset[outputc],
                r"(,.*)$",
                ""
            )
        )

        return dataset
