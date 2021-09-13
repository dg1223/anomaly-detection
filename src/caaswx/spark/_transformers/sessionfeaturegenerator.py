"""
A module to generate features regarding to session feature
Input: A dataframe with a CN row.
Output: A dataframe with the following features extracted:
SESSION_APPS	            A distinct list of root nodes from each record in
                                SM_RESOURCE during time window.
COUNT_UNIQUE_APPS	        A count of distinct root nodes from each record
                                in SM_RESOURCE during time window.
SESSION_USER    	        A distinct list of CNs in CN during time window.
COUNT_ADMIN_LOGIN      	    Count of Admin Login events during the time
                                window, defined by sm_eventid = 7.
COUNT_ADMIN_LOGOUT  	    Count of Admin Logout events during the time
                                window, defined by sm_eventid = 8.
COUNT_ADMIN_REJECT  	    Count of Admin Reject events during the time
                                window, defined by sm_eventid = 2.
COUNT_FAILED        	    Count of all Reject events during the time
                                window, defined by sm_eventid = 2, 6 and 9.
COUNT_VISIT         	    Count of Visit events during the time window,
                                defined by sm_eventid = 13.
COUNT_GET	                Count of all GET HTTP actions in SM_ACTION
                                during the time window.
COUNT_POST	                Count of all POST HTTP actions in SM_ACTION
                                during the time window.
COUNT_HTTP_METHODS	        Count of all GET and POST HTTP actions in
                                SM_ACTION  during the time window.
COUNT_RECORDS	            Counts number of CRA_SEQs (dataset primary key)
COUNT_UNIQUE_ACTIONS   	    Count of distinct HTTP Actions in SM_ACTION
                                during the time window.
COUNT_UNIQUE_EVENTS	        Count of distinct EventIDs in SM_EVENTID during
                                the time window.
COUNT_UNIQUE_USERNAME	    Count of distinct CNs in CN during the time
                                window.
COUNT_UNIQUE_RESOURCES	    Count of distinct Resource Strings in
                                SM_RESOURCE during the time window.
COUNT_UNIQUE_REP            A count of Entries containing “rep”
                                followed by a string ending in “/”
                                in SM_RESOURCE during time window.
SESSION_SM_ACTION	        A distinct list of HTTP Actions in
                                SM_ACTION during time window.
SESSION_RESOURCE	        A distinct list of Resource Strings in
                                SM_RESOURCE during time window.
SESSION_REP_APP	            A distinct list of Entries containing
                                “rep” followed by a string ending
                                in “/” in SM_RESOURCE during time
                                window.
SESSION_FIRST_TIME_SEEN     Minimum time at which a record was
                                logged during the time window.
SESSION_LAST_TIME_SEEN	    Maximum time at which a record was logged
                                during the time window.
SDV_BT_RECORDS	            Standard deviation of timestamp deltas
                                during the time window.
"""

import pyspark.sql.functions as f
# Import Essential packages
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.types import (
    LongType,
    StringType,
    TimestampType,
    DoubleType,
)
from pyspark.sql.window import Window

from src.caaswx.spark._transformers.sparknativetransformer import (
    SparkNativeTransformer,
)


class SessionFeatureGenerator(SparkNativeTransformer):
    """
    Feature transformer for the swx project.
    """

    window_length = Param(
        Params._dummy(),
        "window_length",
        "Length of the sliding window used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    window_step = Param(
        Params._dummy(),
        "window_step",
        "Length of the sliding window step-size used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(self):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super().__init__()
        self._setDefault(window_length=900, window_step=900)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None,
        thresholds=None, inputCols=None, outputCols=None) Sets params for
        this SessionFeatureGenerator.
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

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_EVENTID": ["SM_EVENTID", LongType()],
        "SM_RESOURCE": ["SM_RESOURCE", StringType()],
        "SM_CLIENTIP": ["SM_CLIENTIP", StringType()],
        "CN": ["CN", StringType()],
        "SM_ACTION": ["SM_ACTION", StringType()],
        "CRA_SEQ": ["CRA_SEQ", DoubleType()],
    }

    def _transform(self, dataset):
        ts_window = Window.partitionBy("SM_SESSIONID").orderBy("SM_TIMESTAMP")

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
            "SM_SESSIONID",
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)
                    )
                ),
                "",
            ).alias("SESSION_APPS"),
            f.size(
                f.array_remove(
                    f.array_distinct(
                        f.collect_list(
                            regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)
                        )
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_APPS"),
            f.array_distinct(f.collect_list(col("CN"))).alias("SESSION_USER"),
            f.count(when(col("SM_EVENTID") == 7, True)).alias(
                "COUNT_ADMIN_LOGIN"
            ),
            f.count(when(col("SM_EVENTID") == 8, True)).alias(
                "COUNT_ADMIN_LOGOUT"
            ),
            f.count(when(col("SM_EVENTID") == 3, True)).alias(
                "COUNT_AUTH_ATTEMPT"
            ),
            f.count(when(col("SM_EVENTID") == 2, True)).alias(
                "COUNT_AUTH_REJECT"
            ),
            f.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("COUNT_FAILED"),
            f.count(when(col("SM_EVENTID") == 13, True)).alias("COUNT_VISIT"),
            f.count(when(col("SM_ACTION").contains("GET"), True)).alias(
                "COUNT_GET"
            ),
            f.count(when(col("SM_ACTION").contains("POST"), True)).alias(
                "COUNT_POST"
            ),
            f.count(
                when(
                    (col("SM_ACTION").contains("GET"))
                    | (col("SM_ACTION").contains("POST")),
                    True,
                )
            ).alias("COUNT_HTTP_METHODS"),
            f.count(col("CRA_SEQ")).alias("COUNT_RECORDS"),
            f.countDistinct(col("SM_ACTION")).alias("COUNT_UNIQUE_ACTIONS"),
            f.countDistinct(col("SM_EVENTID")).alias("COUNT_UNIQUE_EVENTS"),
            f.countDistinct(col("SM_CLIENTIP")).alias("COUNT_UNIQUE_IPS"),
            f.countDistinct(col("SM_RESOURCE")).alias(
                "COUNT_UNIQUE_RESOURCES"
            ),
            (
                f.size(
                    f.array_remove(
                        f.array_distinct(
                            f.collect_list(
                                regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                            )
                        ),
                        "",
                    )
                ).alias("COUNT_UNIQUE_REP")
            ),
            f.array_distinct(f.collect_list(col("SM_ACTION"))).alias(
                "SESSION_SM_ACTION"
            ),
            f.array_distinct(f.collect_list(col("SM_RESOURCE"))).alias(
                "SESSION_RESOURCE"
            ),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                    )
                ),
                "",
            ).alias("SESSION_REP_APP"),
            f.min(col("SM_TIMESTAMP")).alias("SESSSION_FIRST_TIME_SEEN"),
            f.max(col("SM_TIMESTAMP")).alias("SESSSION_LAST_TIME_SEEN"),
            f.round(f.stddev("SM_CONSECUTIVE_TIME_DIFFERENCE"), 15).alias(
                "SDV_BT_RECORDS"
            ),
        )
