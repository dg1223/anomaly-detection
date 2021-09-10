"""
A module to generate features related to IP features.
This transformer encompasses the IP addresses' behaviour and analytics
Input: A Spark dataframe
Expected columns in the input dataframe (It's okay if the dataframe contains other columns apart from these ones):
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | SM_ACTION   | string   | Records the  HTTP action. Get,   |
	|             |          | Put and Post (can contain NULLs).|
    +-------------+----------+----------------------------------+
    | SM_TIMESTAMP| timestamp| Marks the time at which the entry|
	|             |          | was made to the database.        |
    +-------------+----------+----------------------------------+
    | SM_CLIENTIP | string   | The IP address for the client    |
	|             |          | machine that is trying to utilize|
	|             |          | a protected resource.            |
    +-------------+----------+----------------------------------+
    | CN          | string   | Column containing the CommonNames|
    |             |          | for each user. It is an alpha-   |
    |             |          | numeric string and it may contain|
    |             |          | NULL values. CNs can be generated|
	|             |          | from SM_USERNAME column through  |
	|             |          | the CnExtractor transformer.     |
    +-------------+----------+----------------------------------+
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
	| SM_USERNAME | string   | The username for the user logged |
    |             |          | in with this session. Usernames  |
	|             |          | encompass CNs along with abstract|
	|             |          | information about CMS and AMS    |
	|             |          | requests. It may contain SAML    |
	|             |          | requests, NULLs and blank values.|
	|             |          | The general format of this column|
	|             |          | includes various abbreviated     |
	|             |          | expressions of various apps      |
	|             |          | separated by "/".                |
    +-------------+----------+----------------------------------+
	| SM_SESSIONID| string   | The session identifier for this  |
    |             |          | user’s activity.                 |
    +-------------+----------+----------------------------------+

	Output features:

	+-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
	| IP_APP      |  array   | A distinct list of main apps     |
    |             | <string> | from each record in SM_RESOURCE  |
	|             |          | during time window.              |
    +-------------+----------+----------------------------------+
	| IP_AVG_TIME_| double   | Average time between records     |
    | BT_RECORDS  |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_MAX_TIME_| double   | Maximum time between records     |
    | BT_RECORDS  |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_MIN_TIME_| double   | Minimum time between records     |
    | BT_RECORDS  |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Admin Login events      |
    | ADMIN_LOGIN |          | during the time window, defined  |
	|             |          | by sm_eventid = 7.               |
	+-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Admin Logout events     |
    | ADMIN_LOGOUT|          | during the time window, defined  |
	|             |          | by sm_eventid = 8.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Admin reject events     |
    | ADMIN_REJECT|          | during the time window, defined  |
	|             |          | by sm_eventid = 9.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Auth accept events      |
    | AUTH_ACCEPT |          | during the time window, defined  |
	|             |          | by sm_eventid = 1.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Admin attempt events    |
    | ADMIN_ATTEMP|          | during the time window, defined  |
	| T           |          | by sm_eventid = 3.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Auth challenge events   |
    | AUTH_CHALLEN|          | during the time window, defined  |
	| GE          |          | by sm_eventid = 4.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Auth Logout events      |
    | AUTH_LOGOUT |          | during the time window, defined  |
	|             |          | by sm_eventid = 10.              |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Auth reject events      |
    | AUTH_REJECT |          | during the time window, defined  |
	|             |          | by sm_eventid = 2.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Az accept events        |
    | AZ_ACCEPT   |          | during the time window, defined  |
	|             |          | by sm_eventid = 5.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Az reject events        |
    | AZ_REJECT   |          | during the time window, defined  |
	|             |          | by sm_eventid = 6.               |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all Reject events       |
    | FAILED      |          | during the time window, defined  |
	|             |          | by sm_eventid = 2,6 and 9.       |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all GET HTTP actions    |
    | GET         |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all POST HTTP actions   |
    | POST        |          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all GET and POST actions|
    | HTTP_METHODS|          | during the time window.          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “ams” or “AMS”      |
    | OU_AMS      |          | occurrences in SM_USERNAME OR    |
	|             |          | SM_RESOURCE during time window.  |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “cra-cp” occurrences|
    | OU_CMS      |          | in SM_USERNAME during the window.|
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “ou=Identity”       |
    | OU_IDENTITY |          | occurrences in SM_USERNAME during|
	|             |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “ou=Identity”       |
    | OU_IDENTITY |          | occurrences in SM_USERNAME during|
	|             |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “ou=Credential”     |
    | OU_CRED     |          | occurrences in SM_USERNAME during|
	|             |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “ou=SecureKey”      |
    | OU_SECUREKEY|          | occurrences in SM_USERNAME during|
	|             |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “mima” occurrences  |
    | PORTAL_MYA  |          | in SM_RESOURCE during the time   |
	|             |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of all “myba” occurrences  |
    | PORTAL_MYbA |          | in SM_RESOURCE during the time   |
	|             |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of distinct HTTP Actions   |
    | UNIQUE_ACTIO|          | in SM_ACTION during the time     |
	| NS          |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of distinct HTTP Actions   |
    | UNIQUE_ACTIO|          | in SM_ACTION during the time     |
	| NS          |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of distinct EventIDs in    |
    | UNIQUE_EVENT|          | SM_EVENTID  during the time      |
	| S           |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of distinct CNs in CN      |
    | UNIQUE_USERN|          | during the time window.          |
	| AME         |          |                                  |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | A distinct list of SessionIDs in |
    | UNIQUE_SESSI|          | SM_SESSIONID during time window. |
	| ON          |          |                                  |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of distinct Resource       |
    | UNIQUE_RESOU|          | strings in SM_RESOURCE during    |
	| RCES        |          | the time window.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | A count of Entries containing    |
    | UNIQUE_PORTA|          | “rep” followed by a string ending|
	| L_RAC       |          | in “/” in SM_RESOURCE during the |
	|             |          | time window.                     |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Counts number of CRA_SEQs        |
    | RECORDS     |          | (dataset primary key)            |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Visit events during the |
    | VISIT       |          | time window, defined by          |
	|             |          | sm_eventid = 13.                 |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Validate Accept events  |
    | VALIDATE_ACC|          | during the time window, defined) |
	| EPT         |          |  by sm_eventid = 11.             |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | integer  | Count of Validate reject events  |
    | VALIDATE_REJ|          | during the time window, defined) |
	| ECT         |          |  by sm_eventid = 12.             |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of HTTP Actions  |
    | SM_ACTIONS  | <string> | in SM_ACTION during time window. |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of CNs           |
    | USERNAME    | <string> | in CN during time window.        |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of SessionIDs    |
    | SM_SESSION  | <string> | in SM_SESSIONID during window.   |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of Resource      |
    | SM_PORTALS  | <string> | strings in SM_RESOURCE during    |
	|             |          | time window.                     |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of Transaction   |
    | SM_TRANSACTI| <string> | Ids in SM_TRANSACTIONID during   |
	| ONS         |          | time window.                     |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of Entries       |
    | USER_OU     | <string> | containing “ou=” and a string    |
	|             |          | ending in “,” in SM_USERNAME     |
	|             |          | during time window.              |
    +-------------+----------+----------------------------------+
	| IP_UNIQUE_  |  array   | A distinct list of Entries       |
    | _REP_APP    | <string> | containing “rep” followed by a   |
	|             |          | string ending in “/” in          |
	|             |          | SM_RESOURCE during time window.  |
    +-------------+----------+----------------------------------+
	| IP_TIMESTAM | timestamp| Earliest timestamp during time   |
    | P           |          | window.                          |
    +-------------+----------+----------------------------------+
	| IP_COUNT_   | iinteger | A count of distinct Entries      |
    | UNIQUE_OU   |          | containing “ou=” and a string    |
	|             |          | in “,” in SM_USERNAME during time|
	|             |          | window                           |
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
    TimestampType,
    LongType,
    StringType,
    StructField,
    DoubleType,
)
from pyspark.sql.window import Window
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer


class IPFeatureGenerator(SparkNativeTransformer):
    """
    IP Feature transformer for the swx project.
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
        :param window_length: Sets this UserFeatureGenerator's window length.
        :param window_step: Sets this UserFeatureGenerator's window step.
        :type window_length: long
        :type window_step: long

        :Example:
        >>> from ipfeaturegenerator import IPFeatureGenerator
        >>> feature_generator = IPFeatureGenerator(window_length = 1800, window_step = 1800)
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
        Sets this IPFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this IPFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_EVENTID": ["SM_EVENTID", LongType()],
        "SM_RESOURCE": ["SM_RESOURCE", StringType()],
        "SM_CLIENTIP": ["SM_CLIENTIP", StringType()],
        "CN": ["CN", StringType()],
        "SM_ACTION": ["SM_ACTION", StringType()],
        "SM_USERNAME": ["SM_USERNAME", StringType()],
        "SM_SESSIONID": ["SM_SESSIONID", StringType()],
        "CRA_SEQ": ["CRA_SEQ", DoubleType()],
        "SM_TRANSACTIONID": ["SM_TRANSACTIONID", StringType()],
    }

    def _transform(self, dataset):
        """
        Transforms the given dataset by deriving IP features
        and returning a new dataframe of those features
        """

        ts_window = Window.partitionBy("SM_CLIENTIP").orderBy("SM_TIMESTAMP")

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
            "SM_CLIENTIP",
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
            ).alias("IP_APP"),
            F.round(F.mean("SM_CONSECUTIVE_TIME_DIFFERENCE"), 15).alias(
                "IP_AVG_TIME_BT_RECORDS"
            ),
            F.max("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("IP_MAX_TIME_BT_RECORDS"),
            F.min("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("IP_MIN_TIME_BT_RECORDS"),
            F.count(when(col("SM_EVENTID") == 7, True)).alias("IP_COUNT_ADMIN_LOGIN"),
            F.count(when(col("SM_EVENTID") == 8, True)).alias("IP_COUNT_ADMIN_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 9, True)).alias("IP_COUNT_ADMIN_REJECT"),
            F.count(when(col("SM_EVENTID") == 1, True)).alias("IP_COUNT_AUTH_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 3, True)).alias("IP_COUNT_ADMIN_ATTEMPT"),
            F.count(when(col("SM_EVENTID") == 4, True)).alias(
                "IP_COUNT_AUTH_CHALLENGE"
            ),
            F.count(when(col("SM_EVENTID") == 10, True)).alias("IP_COUNT_AUTH_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 2, True)).alias("IP_COUNT_AUTH_REJECT"),
            F.count(when(col("SM_EVENTID") == 5, True)).alias("IP_COUNT_AZ_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 6, True)).alias("IP_COUNT_AZ_REJECT"),
            F.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("IP_COUNT_FAILED"),
            F.count(when(col("SM_ACTION").contains("GET"), True)).alias("IP_COUNT_GET"),
            F.count(when(col("SM_ACTION").contains("POST"), True)).alias(
                "IP_COUNT_POST"
            ),
            F.count(
                when(
                    (col("SM_ACTION").contains("GET"))
                    | (col("SM_ACTION").contains("POST")),
                    True,
                )
            ).alias("IP_COUNT_HTTP_METHODS"),
            F.count(
                when(
                    (col("SM_USERNAME").contains("ams"))
                    | (col("SM_RESOURCE").contains("AMS")),
                    True,
                )
            ).alias("IP_COUNT_OU_AMS"),
            F.count(when(col("SM_USERNAME").contains("cra-cp"), True)).alias(
                "IP_COUNT_OU_CMS"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=Identity"), True)).alias(
                "IP_COUNT_OU_IDENTITY"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=Credential"), True)).alias(
                "IP_COUNT_OU_CRED"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=SecureKey"), True)).alias(
                "IP_COUNT_OU_SECUREKEY"
            ),
            F.count(when(col("SM_RESOURCE").contains("mima"), True)).alias(
                "IP_COUNT_PORTAL_MYA"
            ),
            F.count(when(col("SM_RESOURCE").contains("myba"), True)).alias(
                "IP_COUNT_PORTAL_MYBA"
            ),
            F.countDistinct(col("SM_ACTION")).alias("IP_COUNT_UNIQUE_ACTIONS"),
            F.countDistinct(col("SM_EVENTID")).alias("IP_COUNT_UNIQUE_EVENTS"),
            F.countDistinct(col("CN")).alias("IP_COUNT_UNIQUE_USERNAME"),
            F.countDistinct(col("SM_RESOURCE")).alias("IP_COUNT_UNIQUE_RESOURCES"),
            F.countDistinct(col("SM_SESSIONID")).alias("IP_COUNT_UNIQUE_SESSIONS"),
            (
                F.size(
                    F.array_remove(
                        F.array_distinct(
                            F.collect_list(
                                regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                            )
                        ),
                        "",
                    ),
                )
            ).alias("IP_COUNT_PORTAL_RAC"),
            F.count(col("CRA_SEQ")).alias("IP_COUNT_RECORDS"),
            F.count(when(col("SM_EVENTID") == 13, True)).alias("IP_COUNT_VISIT"),
            F.count(when(col("SM_EVENTID") == 11, True)).alias(
                "IP_COUNT_VALIDATE_ACCEPT"
            ),
            F.count(when(col("SM_EVENTID") == 12, True)).alias(
                "IP_COUNT_VALIDATE_REJECT"
            ),
            F.array_distinct(F.collect_list(col("SM_ACTION"))).alias(
                "IP_UNIQUE_SM_ACTIONS"
            ),
            F.array_distinct(F.collect_list(col("CN"))).alias("IP_UNIQUE_USERNAME"),
            F.array_distinct(F.collect_list(col("SM_SESSIONID"))).alias(
                "IP_UNIQUE_SM_SESSION"
            ),
            F.array_distinct(F.collect_list(col("SM_RESOURCE"))).alias(
                "IP_UNIQUE_SM_PORTALS"
            ),
            F.array_distinct(F.collect_list(col("SM_TRANSACTIONID"))).alias(
                "IP_UNIQUE_SM_TRANSACTIONS"
            ),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                ),
                "",
            ).alias("IP_UNIQUE_USER_OU"),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                ),
                "",
            ).alias("IP_UNIQUE_REP_APP"),
            F.min(col("SM_TIMESTAMP")).alias("IP_TIMESTAMP"),
            F.size(
                F.array_remove(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                    ),
                    "",
                )
            ).alias("IP_COUNT_UNIQUE_OU"),
        )
