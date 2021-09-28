from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params

# Importing window module for performing time slicing while grouping
# parquet_data
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.window import Window

# Importing the Transformer class to be extended by Flattener classes
from .sparknativetransformer import SparkNativeTransformer

import pyspark.sql.functions as func


class ResourcesFlattener(SparkNativeTransformer):
    """
    A module for flattening the resources into a list with respect to the
    input pivot column. 
    
    Input: A Spark Dataframe containing SM_RESOURCE and SM_TIMESTAMP (from 
    raw_logs) and the following column (default: "SM_USERNAME"):
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Pivot Column for creating the    |
    | Default("   |          | time window of usage of different|
    | agg_col")   |          | resources with respect to the    |
    |             |          | passed column.                   |
    +-------------+----------+----------------------------------+
    Please refer to README.md for description.
        
    Output: A Spark Dataframe with the following features calcuated on rows
            aggregated by window and agg_col,
            where the window is calculated using:
                - length: how many seconds the window is
                - step: the length of time between the start of
                    successive time window
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | SM_RESOURCE |  array   | A list of resources used by the  |
    |             | <string> | pivot entity within the time     |
    |             |          | window.                          |
    +-------------+----------+----------------------------------+
    """

    window_length = Param(
        Params._dummy(),
        "window_length",
        "Length of the sliding window used for aggregation resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    window_step = Param(
        Params._dummy(),
        "window_step",
        "Length of the sliding window step-size used for aggregation" 
        + " resolution."
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    agg_col = Param(
        Params._dummy(),
        "agg_col",
        "Name of the column to perform aggregation on, together with the "
        + "sliding window.",
        typeConverter=TypeConverters.toString,
    )

    max_resource_count = Param(
        Params._dummy(),
        "max_resource_count",
        "Maximum count of resources allowed in the resource list within the "
        + "sliding window.",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(
        self,
        agg_col="SM_USERNAME",
        window_length=900,
        window_step=900,
        max_resource_count=-1,
    ):
        """
        :param window_length: Length of the sliding window (in seconds)
        :param window_step: Length of the sliding window step-size (in
        seconds) :param agg_col: Name of the column to perform
        aggregation along with the window :param max_resource_count: Maximum
        count of resources allowed in the resource list :type window_length:
        long :type window_step: long :type agg_col: string :type
        max_resource_count: long

        :Example:
        from resourcesflattener import ResourcesFlattener
        flattener = ResourcesFlattener(window_length = 1800, window_step = 1800
        ,agg_col = "SM_USERNAME", max_resource_count = 3)
        datafame_with_CN = flattener.transform(input_dataset)
        """
        super(ResourcesFlattener, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            agg_col="SM_USERNAME",
            max_resource_count=-1,
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
        agg_col="SM_USERNAME",
        window_length=900,
        window_step=900,
        max_resource_count=-1,
    ):
        """
        Sets params for this ResourcesFlattener.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_agg_col(self, value):
        """
        Sets the Entity Name
        """
        self._set(agg_col=value)

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

    def set_max_resource_count(self, value):
        """
        Sets this ResourcesFlattener's maximum resource count.
        """
        self._set(max_resource_count=value)

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_RESOURCE": ["SM_RESOURCE", StringType()],
    }

    def _transform(self, dataset):
        """
        flatten input dataset w.r.t URLs
        Input : Siteminder dataframe
        Output : flattened URLs merged into lists
        """
        if int(self.getOrDefault("max_resource_count")) > 0:
            window_spec = Window.partitionBy(
                "window", str(self.getOrDefault("agg_col"))
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "window",
                window(
                    "SM_TIMESTAMP",
                    str(self.getOrDefault("window_length")) + " seconds",
                    str(self.getOrDefault("window_step")) + " seconds",
                ),
            ).withColumn("dense_rank", func.dense_rank().over(window_spec))
            dataset = dataset.filter(
                dataset["dense_rank"]
                <= int(self.getOrDefault("max_resource_count"))
            )
        return dataset.groupby(
            str(self.getOrDefault("agg_col")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(func.collect_list("SM_RESOURCE").alias("SM_RESOURCE"))
