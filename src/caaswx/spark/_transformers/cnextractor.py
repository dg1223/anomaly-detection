from pyspark.ml import Transformer
from pyspark import keyword_only
import pyspark.sql.functions as F
from pyspark.ml.param.shared import TypeConverters, Param, Params


class CnExtractor(Transformer):
    windowLength = Param(
        Params._dummy(),
        "windowLength",
        "Length of the sliding window used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    windowStep = Param(
        Params._dummy(),
        "windowStep",
        "Length of the sliding window step-size used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    inputCol = Param(
        Params._dummy(),
        "inputCol",
        "Name of col that we are working with",
        typeConverter=TypeConverters.toString,
    )

    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "Name of col that we are outputting",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self, windowLength=900, windowStep=900, inputCol="SM_USERNAME", outputCol="CN"
    ):
        super(CnExtractor, self).__init__()

        self._setDefault(
            windowLength=900, windowStep=900, inputCol="SM_USERNAME", outputCol="CN"
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
        """
        Sets the input column
        """
        self._set(inputCol=value)

    def set_output_col(self, value):
        """
        Sets the output column
        """
        self._set(outputCol=value)

    def set_window_length(self, value):
        """
        Sets this ResourcesFlattener's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this ResourcesFlattener's window step size.
        """
        self._set(window_step=value)

    def _transform(self, dataset):
        def cleanUsername(row: str) -> str:
            row = row.split(",", 2)[0]
            if "cn=" in row:
                return row.split("=")[1]
            else:
                return row

        extract_udf = F.udf(lambda row: cleanUsername(row))
        dataset = dataset.withColumn("CN", extract_udf(dataset.SM_USERNAME))
        return dataset
