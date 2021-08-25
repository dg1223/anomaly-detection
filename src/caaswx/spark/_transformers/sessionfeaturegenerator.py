"""
A module to generate features regarding to session feature
"""

import pyspark.sql.functions as F

# Import Essential packages
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.window import Window


class SessionFeatureGenerator(Transformer):
    """
    Feature transformer for the swx project.
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

    @keyword_only
    def __init__(self):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super().__init__()
        self._setDefault(entity_name="SM_SESSIONID", window_length=900, window_step=900)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                  inputCols=None, outputCols=None)
        Sets params for this SessionFeatureGenerator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_window_length(self, value):
        """
        Sets this SessionFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this SessionFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    def set_entity_name(self, value):
        """
        Sets this SessionFeatureGenerator's window step size.
        """
        self._set(entity_name=value)

    def _transform(self, dataset):

        ts_window = Window.partitionBy("CN").orderBy("SM_TIMESTAMP")

        dataset = dataset.withColumn(
            "SM_PREV_TIMESTAMP", lag(dataset["SM_TIMESTAMP"]).over(ts_window)
        )

        dataset = dataset.withColumn(
            "SM_CONSECUTIVE_TIME_DIFFERENCE",
            when(
                isnull(
                    dataset["SM_TIMESTAMP"].cast("long")
                    - dataset["SM_PREV_TIMESTAMP"].cast("long")
                ),
                0,
            ).otherwise(
                dataset["SM_TIMESTAMP"].cast("long")
                - dataset["SM_PREV_TIMESTAMP"].cast("long")
            ),
        )

        dataset = dataset.drop("SM_PREV_TIMESTAMP")

        return dataset.groupby(
            str(self.getOrDefault("entity_name")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
            ).alias("SESSION_APPS"),
            F.countDistinct(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)).alias(
                "COUNT_UNIQUE_APPS"
            ),
            F.array_distinct(F.collect_list(col("SM_CN"))).alias("SESSION_USER"),
            F.count(when(col("SM_EVENTID") == 7, True)).alias("COUNT_ADMIN_LOGIN"),
            F.count(when(col("SM_EVENTID") == 8, True)).alias("COUNT_ADMIN_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 3, True)).alias("COUNT_AUTH_ATTEMPT"),
            F.count(when(col("SM_EVENTID") == 2, True)).alias("COUNT_AUTH_REJECT"),
            F.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("COUNT_FAILED"),
            F.count(when(col("SM_EVENTID") == 13, True)).alias("COUNT_VISIT"),
            F.count(when(col("SM_ACTION").contains("GET"), True)).alias("COUNT_GET"),
            F.count(when(col("SM_ACTION").contains("POST"), True)).alias("COUNT_POST"),
            F.count(
                when(
                    (col("SM_ACTION").contains("GET"))
                    | (col("SM_ACTION").contains("POST")),
                    True,
                )
            ).alias("COUNT_HTTP_METHODS"),
            F.count(col("CRA_SEQ")).alias("COUNT_RECORDS"),
            F.countDistinct(col("SM_ACTION")).alias("COUNT_UNIQUE_ACTIONS"),
            F.countDistinct(col("SM_EVENTID")).alias("COUNT_UNIQUE_EVENTS"),
            F.countDistinct(col("SM_CLIENTIP")).alias("COUNT_UNIQUE_IPS"),
            F.countDistinct(col("SM_RESOURCE")).alias("COUNT_UNIQUE_RESOURCES"),
            (
                F.size(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                    )
                )
                - 1
            ).alias("COUNT_UNIQUE_REP"),
            F.array_distinct(F.collect_list(col("SM_ACTION"))).alias(
                "SESSION_SM_ACTION"
            ),
            F.array_distinct(F.collect_list(col("SM_RESOURCE"))).alias(
                "SESSION_RESOURCE"
            ),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
            ).alias("SESSION_REP_APP"),
            F.min(col("SM_TIMESTAMP")).alias("SESSSION_FIRST_TIME_SEEN"),
            F.max(col("SM_TIMESTAMP")).alias("SESSSION_LAST_TIME_SEEN"),
            F.stddev("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("SDV_BT_RECORDS"),
        )
