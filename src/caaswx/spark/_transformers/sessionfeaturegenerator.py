"""
A module to generate features related to session features. This transformer encompasses the Session's behaviour and analytics.
input: A Spark dataframe

Expected columns in the input dataframe (It's okay if the dataframe contains other columns apart from these ones):
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | SM_RESOURCE | string   | The resource, for example a web  |
	|             |          | page that the user is requesting.|
	|             |          | This column can contain URLs in  |
	|             |          | formats along with NULL values   |
	|             |          | and abbreviations of various     |
	|             |          | applications separated by "/".   |
	|             |          | It can also encompass GET/POST   |
	|             |          | request parameters related to    |
	|             |          | different activities of user.    |
	|             |          | Some rows also have blank values |
	|             |          | for SM_RESOURCE.                 |
    +-------------+----------+----------------------------------+
	| SM_EVENTID  | integer  | Marks the particular event that  |
    |             |          | caused the logging to occur.     |
    +-------------+----------+----------------------------------+
	| SM_ACTION   | string   | Records the  HTTP action. Get,   |
	|             |          | Put and Post (can contain NULLs).|
    +-------------+----------+----------------------------------+
	| SM_CLIENTIP | string   | The IP address for the client    |
	|             |          | machine that is trying to utilize|
	|             |          | a protected resource.            |
    +-------------+----------+----------------------------------+
	| SM_TIMESTAMP| timestamp| Marks the time at which the entry|
	|             |          | was made to the database.        |
    +-------------+----------+----------------------------------+
	| SM_SESSIONID| string   | The session identifier for this  |
    |             |          | user’s activity.                 |
    +-------------+----------+----------------------------------+
	| CN          | string   | Column containing the CommonNames|
    |             |          | for each user. It is an alpha-   |
    |             |          | numeric string and it may contain|
    |             |          | NULL values. CNs can be generated|
	|             |          | from SM_USERNAME column through  |
	|             |          | the CnExtractor transformer.     |
    +-------------+----------+----------------------------------+
	| CRA_SEQ     | long     | Serves as the primary key for the|
    |             |          | Siteminder data and can be used  |
	|             |          | for counting unique rows via     |
	|             |          | aggregation steps.               |
    +-------------+----------+----------------------------------+

Output features:

	+-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
	| SESSION_APPS|  array   | A distinct list of main apps     |
    |             | <string> | from each record in SM_RESOURCE  |
	|             |          | during time window.              |
    +-------------+----------+----------------------------------+
	| COUNT_UNIQUE| integer  | Count of distinct Resource       |
    | APPS        |          | strings in SM_RESOURCE during    |
	|             |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| SESSION_    |  array   | A distinct list of CNs           |
    | USER        | <string> | in CN during time window.        |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of Admin Login events      |
    | ADMIN_LOGIN |          | during the time window, defined  |
	|             |          | by sm_eventid = 7.               |
	+-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of Admin Logout events     |
    | ADMIN_LOGOUT|          | during the time window, defined  |
	|             |          | by sm_eventid = 8.               |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of Admin reject events     |
    | ADMIN_REJECT|          | during the time window, defined  |
	|             |          | by sm_eventid = 9.               |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of all Reject events       |
    | FAILED      |          | during the time window, defined  |
	|             |          | by sm_eventid = 2,6 and 9.       |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of Visit events during the |
    | VISIT       |          | time window, defined by          |
	|             |          | sm_eventid = 13.                 |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of all GET HTTP actions    |
    | GET         |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of all POST HTTP actions   |
    | POST        |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of all GET and POST actions|
    | HTTP_METHODS|          | during the time window.          |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Counts number of CRA_SEQs        |
    | RECORDS     |          | (dataset primary key)            |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of distinct HTTP Actions   |
    | UNIQUE_ACTIO|          | in SM_ACTION during the time     |
	| NS          |          | window.                          |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of distinct CNs in CN      |
    | UNIQUE_USERN|          | during the time window.          |
	| AME         |          |                                  |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of distinct Resource       |
    | UNIQUE_RESOU|          | strings in SM_RESOURCE during    |
	| RCES        |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | Count of distinct EventIDs in    |
    | UNIQUE_EVENT|          | SM_EVENTID  during the time      |
	| S           |          | window.                          |
    +-------------+----------+----------------------------------+
	| COUNT_      | integer  | A count of Entries containing    |
    | UNIQUE_REP  |          | “rep” followed by a string ending|
	|             |          | in “/” in SM_RESOURCE during the |
	|             |          | time window.                     |
    +-------------+----------+----------------------------------+
	| SESSION_    |  array   | A distinct list of HTTP Actions  |
    | SM_ACTION   | <string> | in SM_ACTION during time window. |
    +-------------+----------+----------------------------------+
	| SESSION_    |  array   | A distinct list of Resource      |
    | RESOURCE    | <string> | strings in SM_RESOURCE during    |
	|             |          | time window.                     |
    +-------------+----------+----------------------------------+
	| SESSION_    |  array   | A distinct list of Entries       |
    | REP_APP     | <string> | containing “rep” followed by a   |
	|             |          | string ending in “/” in          |
	|             |          | SM_RESOURCE during time window.  |
    +-------------+----------+----------------------------------+
	| SESSION_FIRS| timestamp| Minimum time at which a record   |
    | T_TIME_SEEN |          | was logged during the time window|
    +-------------+----------+----------------------------------+
	| SESSION_LAST| timestamp| Maximum time at which a record   |
    | _TIME_SEEN  |          | was logged during the time window|
    +-------------+----------+----------------------------------+
	| SDV_BT_RECOR| timestamp| Standard deviation of timestamp  |
    | DS          |          | deltas during the time window.   |
    +-------------+----------+----------------------------------+
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
