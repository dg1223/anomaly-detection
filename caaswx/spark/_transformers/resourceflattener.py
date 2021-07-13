# Importing various datatypes supported by Spark for specifying the schemas of result dataframes
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
    MapType,
)

# Importing OOP decorators
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params

# Importing window module for performing time slicing while grouping data
from pyspark.sql.functions import window
from pyspark.sql.window import Window

# Importing the Transformer class to be extended by Flattener classes
from pyspark.ml import Transformer

import pyspark.sql.functions as func
from pyspark.sql.functions import lit


class ResourceFlattener(Transformer):
    """
  User Feature transformer for the Streamworx project.
  """

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

    entityName = Param(
        Params._dummy(),
        "entityName",
        "Name of the column to perform aggregation on, together with the "
        + "sliding window.",
        typeConverter=TypeConverters.toString,
    )

    maxResourceCount = Param(
        Params._dummy(),
        "maxResourceCount",
        "Maximum count of resources allowed in the resource list within the "
        + "sliding window.",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(
        self,
        entityName="SM_USERNAME",
        windowLength=900,
        windowStep=900,
        maxResourceCount=-1,
    ):
        """
    def __init__(self, *, window_length = 900, window_step = 900)
    """
        super(ResourceFlattener, self).__init__()
        self._setDefault(
            windowLength=900,
            windowStep=900,
            entityName="SM_USERNAME",
            maxResourceCount=-1,
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        entityName="SM_USERNAME",
        windowLength=900,
        windowStep=900,
        maxResourceCount=-1,
    ):
        """
    setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
              inputCols=None, outputCols=None)
    Sets params for this ResourceFlattener.
    """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setEntityName(self, value):
        """
    Sets the Entity Name
    """
        self._set(entityName=value)

    def setWindowLength(self, value):
        """
    Sets this ResourceFlattener's window length.
    """
        self._set(windowLength=value)

    def setWindowStep(self, value):
        """
    Sets this ResourceFlattener's window step size.
    """
        self._set(windowStep=value)

    def setMaxResourceCount(self, value):
        """
    Sets this ResourceFlattener's maximum resource count.
    """
        self._set(maxResourceCount=value)

    def _transform(self, dataset):
        """
    Overridden function which flattens the input dataset w.r.t URLs
    Input : Siteminder dataframe
    Output : SM_USERNAMEs with flattened URLs merged into lists
    """
        if int(self.getOrDefault("maxResourceCount")) > 0:
            windowSpec = Window.partitionBy(
                "window", str(self.getOrDefault("entityName"))
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "window",
                window(
                    "SM_TIMESTAMP",
                    str(self.getOrDefault("windowLength")) + " seconds",
                    str(self.getOrDefault("windowStep")) + " seconds",
                ),
            ).withColumn("dense_rank", func.dense_rank().over(windowSpec))
            dataset = dataset.filter(
                dataset["dense_rank"] <= int(self.getOrDefault("maxResourceCount"))
            )
        return (
            dataset.groupby(
                str(self.getOrDefault("entityName")),
                window(
                    "SM_TIMESTAMP",
                    str(self.getOrDefault("windowLength")) + " seconds",
                    str(self.getOrDefault("windowStep")) + " seconds",
                ),
            )
            .agg(func.collect_list("SM_RESOURCE").alias("SM_RESOURCE"))
            .orderBy(str(self.getOrDefault("entityName")))
        )
