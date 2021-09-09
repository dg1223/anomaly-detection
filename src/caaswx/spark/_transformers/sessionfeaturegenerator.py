"""
A module to generate features related to session features. This transformer encompasses the Session's behaviour and analytics.
input: A Spark dataframe

Expected columns in the input dataframe (It's okay if the dataframe contains other columns apart from these ones):

Column Name                 Data type                                                          Description
SM_RESOURCE                  string                  The resource, for example a web page, that the user is requesting. This column can contain URLs in various formats along with NULL values and abbreviations of various applications separated by "/". It can also encompass GET/POST request parameters related to different activities of user. Some rows also have blank values for SM_RESOURCE.

SM_EVENTID                   integer                 Marks the particular event that caused the logging to occur.
SM_ACTION                    string                  Records the  HTTP action. Get, Post, and Put. It can contain NULLs.
CRA_SEQ                      long                    Serves as the primary key for the Siteminder data and can be used for counting unique rows via aggregation steps.
SM_CLIENTIP                  string                  The IP address for the client machine that is trying to utilize a protected resource.
SM_TIMESTAMP                 timestamp               Marks the time at which the entry was made to the Siteminder's database.
SM_SESSIONID                 string                  The session identifier for this user’s activity.
CN                           string                  Column expecting the CommonNames for each user to be inputted. It is an alpha-numeric string and it contains NULL values. It is not present by default in the Siteminder's data. CnExtractor class has to be called with SM_USERNAME column as the input for generating the CN.


Output: A dataframe with the following features extracted:

Column_name                                           Description                                                                                   Datatype
SESSION_APPS	            A distinct list of root nodes from each record in SM_RESOURCE during time window.                                      array<string>
COUNT_UNIQUE_APPS	        A count of distinct root nodes from each record in SM_RESOURCE during time window.                                     integer
SESSION_USER    	        A distinct list of CNs in CN during time window.                                                                       array<s tring>
COUNT_ADMIN_LOGIN      	    Count of Admin Login events during the time window, defined by sm_eventid = 7.                                         integer
COUNT_ADMIN_LOGOUT  	    Count of Admin Logout events during the time window, defined by sm_eventid = 8.                                        integer
COUNT_ADMIN_REJECT  	    Count of Admin Reject events during the time window, defined by sm_eventid = 2.                                        integer
COUNT_FAILED        	    Count of all Reject events during the time window, defined by sm_eventid = 2, 6 and 9.                                 integer
COUNT_VISIT         	    Count of Visit events during the time window, defined by sm_eventid = 13.                                              integer
COUNT_GET	                Count of all GET HTTP actions in SM_ACTION during the time window.                                                     integer
COUNT_POST	                Count of all POST HTTP actions in SM_ACTION during the time window.                                                    integer
COUNT_HTTP_METHODS	        Count of all GET and POST HTTP actions in SM_ACTION  during the time window.                                           integer
COUNT_RECORDS	            Counts number of CRA_SEQs (dataset primary key)                                                                        integer
COUNT_UNIQUE_ACTIONS   	    Count of distinct HTTP Actions in SM_ACTION during the time window.                                                    integer
COUNT_UNIQUE_EVENTS	        Count of distinct EventIDs in SM_EVENTID  during the time window.                                                      integer
COUNT_UNIQUE_USERNAME	    Count of distinct CNs in CN during the time window.                                                                    integer
COUNT_UNIQUE_RESOURCES	    Count of distinct Resource Strings in SM_RESOURCE during the time window.                                              integer
COUNT_UNIQUE_REP	        A count of Entries containing “rep” followed by a string ending in “/” in SM_RESOURCE during time window.              integer
SESSION_SM_ACTION	        A distinct list of HTTP Actions in SM_ACTION during time window.                                                       array<string>
SESSION_RESOURCE	        A distinct list of Resource Strings in SM_RESOURCE during time window.                                                 array<string>
SESSION_REP_APP	            A distinct list of Entries containing “rep” followed by a string ending in “/” in SM_RESOURCE during time window.      array<string>
SESSSION_FIRST_TIME_SEEN	Minimum time at which a record was logged during the time window.                                                      timestamp
SESSSION_LAST_TIME_SEEN	    Maximum time at which a record was logged during the time window.                                                      timestamp
SDV_BT_RECORDS	            Standard deviation of timestamp deltas during the time window.                                                         float
"""

import pyspark.sql.functions as F

# Import Essential packages
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.types import (
    StructType,
    ArrayType,
    LongType,
    StringType,
    StructField,
    TimestampType,
    DoubleType,
)
from pyspark.sql.window import Window
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer

class SessionFeatureGenerator(SparkNativeTransformer):
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

    @keyword_only
    def __init__(self):
        """
          :param window_length: Sets this UserFeatureGenerator's window length.
          :param window_step: Sets this UserFeatureGenerator's window step.
          :type window_length: long
          :type window_step: long

          :Example:
          >>> from sessionfeaturegenerator import SessionFeatureGenerator
          >>> feature_generator = SessionFeatureGenerator(window_length = 1800, window_step = 1800)
          >>> features = feature_generator.transform(dataset = input_dataset)
        """
        super().__init__()
        self._setDefault(window_length=900, window_step=900)
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
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
                ),
                "",
            ).alias("SESSION_APPS"),
            F.size(
                F.array_remove(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_APPS"),
            F.array_distinct(F.collect_list(col("CN"))).alias("SESSION_USER"),
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
                    F.array_remove(
                        F.array_distinct(
                            F.collect_list(
                                regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                            )
                        ),
                        "",
                    )
                ).alias("COUNT_UNIQUE_REP")
            ),
            F.array_distinct(F.collect_list(col("SM_ACTION"))).alias(
                "SESSION_SM_ACTION"
            ),
            F.array_distinct(F.collect_list(col("SM_RESOURCE"))).alias(
                "SESSION_RESOURCE"
            ),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                ),
                "",
            ).alias("SESSION_REP_APP"),
            F.min(col("SM_TIMESTAMP")).alias("SESSSION_FIRST_TIME_SEEN"),
            F.max(col("SM_TIMESTAMP")).alias("SESSSION_LAST_TIME_SEEN"),
            F.round(F.stddev("SM_CONSECUTIVE_TIME_DIFFERENCE"), 15).alias(
                "SDV_BT_RECORDS"
            ),
        )
