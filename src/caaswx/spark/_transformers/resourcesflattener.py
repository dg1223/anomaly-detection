"""
A module for Flatenning the resources into a list with respect to the input pivot column.
Input: A Spark dataframe
Expected columns in the input dataframe

Column Name                 Data type                                                          Description
SM_TIMESTAMP                 timestamp               Marks the time at which the entry was made to the Siteminder's database.
SM_RESOURCE                  string                  The resource, for example a web page, that the user is requesting. This column can contain URLs in various formats along with NULL values and abbreviations of various applications separated by "/". It can also encompass GET/POST request parameters related to different activities of user. Some rows also have blank values for SM_RESOURCE.
this.getOrDefault("entity_name")                  string                     Pivot Column for creating the time window of usage of different resources with respect to the passed column


Output: Input dataset with an additional column containing a list of strings containing the utilized resources by the pivot entity.

Additional_Column_Name                                           Description                                                    Datatype
SM_RESOURCE                             A list of resources used by the pivot entity.                              array<string>
"""
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params

# Importing window module for performing time slicing while grouping parquet_data
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.window import Window

# Importing the Transformer class to be extended by Flattener classes
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer

import pyspark.sql.functions as func


class ResourcesFlattener(SparkNativeTransformer):
    """
    User Feature transformer for the Streamworx project.
    """

    window_length = Param(
        Params._dummy(),
        "windowLength",
        "Length of the sliding window used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    window_step = Param(
        Params._dummy(),
        "windowStep",
        "Length of the sliding window step-size used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    entity_name = Param(
        Params._dummy(),
        "entityName",
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
        entity_name="SM_USERNAME",
        window_length=900,
        window_step=900,
        max_resource_count=-1,
    ):
        """
        :param window_length: Length of the sliding window used for entity resolution (in seconds).
        :param window_step: Length of the sliding window step-size used for entity resolution (in seconds).
        :param entity_name: Name of the column to perform aggregation on, together with the sliding window
        :param max_resource_count: Maximum count of resources allowed in the resource list within the sliding window.
        :type window_length: long
        :type window_step: long
        :type entity_name: string
        :type max_resource_count: long

        :Example:
        >>> from resourcesflattener import ResourcesFlattener
        >>> flattener = ResourcesFlattener(window_length = 1800, window_step = 1800, entity_name = "SM_USERNAME", max_resource_count = 3)
        >>> datafame_with_CN = flattener.transform(input_dataset)
        """
        super(ResourcesFlattener, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            entity_name="SM_USERNAME",
            max_resource_count=-1,
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
        entity_name="SM_USERNAME",
        window_length=900,
        window_step=900,
        max_resource_count=-1,
    ):
        """
        Sets params for this ResourcesFlattener.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_entity_name(self, value):
        """
        Sets the Entity Name
        """
        self._set(entity_name=value)

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
                "window", str(self.getOrDefault("entity_name"))
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
                dataset["dense_rank"] <= int(self.getOrDefault("max_resource_count"))
            )
        return dataset.groupby(
            str(self.getOrDefault("entity_name")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(func.collect_list("SM_RESOURCE").alias("SM_RESOURCE"))
