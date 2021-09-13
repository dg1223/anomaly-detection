
"""
A module to generate features regarding to session feature
Input: A dataframe with CN row.
Output: A dataframe with the following features:

IP_APP                  	A distinct list of root nodes from each record in
SM_RESOURCE during time window. IP_AVG_TIME_BT_RECORDS  	Average time
between records during the time window. IP_MAX_TIME_BT_RECORDS 	    Maximum
time between records during the time window. IP_MIN_TIME_BT_RECORDS
Minimum time between records during the time window. IP_COUNT_ADMIN_LOGIN
Count of Admin Login events during the time window, defined by sm_eventid
= 7. IP_COUNT_ADMIN_LOGOUT  	    Count of Admin Logout events during the
time window, defined by sm_eventid = 8. IP_COUNT_ADMIN_REJECT   	Count of
Admin Reject events during the time window, defined by sm_eventid = 9.
IP_COUNT_AUTH_ACCEPT	    Count of Auth Accept events during the time
window, defined by sm_eventid = 1. IP_COUNT_ADMIN_ATTEMPT  	Count of
Admin Accept events during the time window, defined by sm_eventid = 3.
IP_COUNT_AUTH_CHALLENGE 	Count of Auth Challenge events during the time
window, defined by sm_eventid = 4. IP_COUNT_AUTH_LOGOUT    	Count of Auth
Logout events during the time window, defined by sm_eventid = 10.
IP_COUNT_AUTH_REJECT   	    Count of Auth Reject events during the time
window, defined by sm_eventid = 2. IP_COUNT_AZ_ACCEPT  	    Count of Az
Accept events during the time window, defined by sm_eventid = 5.
IP_COUNT_AZ_REJECT  	    Count of Az Reject events during the time window,
defined by sm_eventid = 6. IP_COUNT_FAILED 	        Count of all Reject
events during the time window, defined by sm_eventid = 2, 6 and 9.
IP_COUNT_GET	            Count of all GET HTTP actions in SM_ACTION during
the time window. IP_COUNT_POST	            Count of all POST HTTP
actions in SM_ACTION during the time window. IP_COUNT_HTTP_METHODS
Count of all GET and POST HTTP actions in SM_ACTION  during the time
window. IP_COUNT_OU_AMS	            Count of all “ams” or “AMS”
occurrences in SM_USERNAME OR SM_RESOURCE during the time window.
IP_COUNT_OU_CMS         	Count of all “cra-cp” occurrences in SM_USERNAME
during the time window. IP_COUNT_OU_IDENTITY       	Count of all
“ou=Identity” occurrences in SM_USERNAME during the time window.
IP_COUNT_OU_CRED        	Count of all “ou=Credential” occurrences in
SM_USERNAME during the time window. IP_COUNT_OU_SECUREKEY   	Count of all
“ou=SecureKey” occurrences in SM_USERNAME during the time window.
IP_COUNT_PORTAL_MYA     	Count of all “mima” occurrences in SM_RESOURCE
during the time window. IP_COUNT_PORTAL_MYBA       	Count of all “myba”
occurrences in SM_RESOURCE during the time window. IP_COUNT_UNIQUE_ACTIONS
Count of distinct HTTP Actions in SM_ACTION during the time window.
IP_COUNT_UNIQUE_EVENTS	    Count of distinct EventIDs in SM_EVENTID
during the time window. IP_COUNT_UNIQUE_USERNAME	Count of distinct CNs
in CN during the time window. IP_COUNT_UNIQUE_RESOURCES  	Count of
distinct Resource Strings in SM_RESOURCE during the time window.
IP_COUNT_UNIQUE_SESSIONS	Count of distinct SessionIDs in SM_SESSIONID
during the time window. IP_COUNT_PORTAL_RAC     	A count of Entries
containing “rep” followed by a string ending in “/” in SM_RESOURCE during
time window. IP_COUNT_RECORDS	        Counts number of CRA_SEQs (
dataset primary key) IP_COUNT_VISIT          	Count of Visit events
during the time window, defined by sm_eventid = 13.
IP_COUNT_VALIDATE_ACCEPT   	Count of Validate Accept events during the
time window, defined by sm_eventid = 11. IP_COUNT_VALIDATE_REJECT	Count
of Validate Reject events during the time window, defined by sm_eventid =
12. IP_UNIQUE_SM_ACTIONS	    A distinct list of HTTP Actions in
SM_ACTION during time window. IP_UNIQUE_USERNAME	        A distinct
list of CNs in CN during time window. IP_UNIQUE_SM_SESSION	    A
distinct list of SessionIDs in SM_SESSIONID during time window.
IP_UNIQUE_SM_PORTALS    	A distinct list of Resource Strings in
SM_RESOURCE during time window. IP_UNIQUE_SM_TRANSACTIONS  	A distinct
list of Transaction Ids in SM_TRANSACTIONID during time window.
IP_UNIQUE_USER_OU       	A distinct list of Entries containing “ou=”
and a string ending in “,” in SM_USERNAME during time window.
IP_UNIQUE_REP_APP       	A distinct list of Entries containing “rep”
followed by a string ending in “/” in SM_RESOURCE during time window.
IP_TIMESTAMP	            Earliest timestamp during time window.
IP_COUNT_UNIQUE_OU      	A count of distinct Entries containing “ou=”
and a string ending in “,” in SM_USERNAME during time window. """

import pyspark.sql.functions as f
# Import Essential packages

from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.types import (
    TimestampType,
    LongType,
    StringType,
    DoubleType,
)
from pyspark.sql.window import Window

from src.caaswx.spark._transformers.sparknativetransformer import \
    SparkNativeTransformer


class IPFeatureGenerator(SparkNativeTransformer):
    """
    A module to generate features related to IP features.
    This transformer encompasses the IP addresses' behaviour and analytics
    Input: A Spark dataframe
    Columns from raw_logs: SM_RESOURCE, SM_EVENTID, SM_ACTION,
    SM_CLIENTIP, SM_TIMESTAMP, SM_SESSIONID, SM_USERNAME
    Please refer to README.md for description.
    List of other required columns:
        +-------------+----------+----------------------------------+
        | Column_Name | Datatype | Description                      |
        +=============+==========+==================================+
        | CN          | string   | Column containing the CommonNames|
        |             |          | for each user. It is an alpha-   |
        |             |          | numeric string and it may contain|
        |             |          | NULL values. CNs can be generated|
        |             |          | from SM_USERNAME column through  |
        |             |          | the CnExtractor transformer.     |
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
        :param window_length: Length of the sliding window (in seconds)
        :param window_step: Length of the sliding window's step-size (in seconds)
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
        set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None,
        thresholds=None, inputCols=None, outputCols=None) Sets params for
        this SessionFeatureGenerator.
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
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
                ),
                "",
            ).alias("IP_APP"),
            f.round(f.mean("SM_CONSECUTIVE_TIME_DIFFERENCE"), 15).alias(
                "IP_AVG_TIME_BT_RECORDS"
            ),
            f.max("SM_CONSECUTIVE_TIME_DIFFERENCE").alias(
                "IP_MAX_TIME_BT_RECORDS"),
            f.min("SM_CONSECUTIVE_TIME_DIFFERENCE").alias(
                "IP_MIN_TIME_BT_RECORDS"),
            f.count(when(col("SM_EVENTID") == 7, True)).alias(
                "IP_COUNT_ADMIN_LOGIN"),
            f.count(when(col("SM_EVENTID") == 8, True)).alias(
                "IP_COUNT_ADMIN_LOGOUT"),
            f.count(when(col("SM_EVENTID") == 9, True)).alias(
                "IP_COUNT_ADMIN_REJECT"),
            f.count(when(col("SM_EVENTID") == 1, True)).alias(
                "IP_COUNT_AUTH_ACCEPT"),
            f.count(when(col("SM_EVENTID") == 3, True)).alias(
                "IP_COUNT_ADMIN_ATTEMPT"),
            f.count(when(col("SM_EVENTID") == 4, True)).alias(
                "IP_COUNT_AUTH_CHALLENGE"
            ),
            f.count(when(col("SM_EVENTID") == 10, True)).alias(
                "IP_COUNT_AUTH_LOGOUT"),
            f.count(when(col("SM_EVENTID") == 2, True)).alias(
                "IP_COUNT_AUTH_REJECT"),
            f.count(when(col("SM_EVENTID") == 5, True)).alias(
                "IP_COUNT_AZ_ACCEPT"),
            f.count(when(col("SM_EVENTID") == 6, True)).alias(
                "IP_COUNT_AZ_REJECT"),
            f.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("IP_COUNT_FAILED"),
            f.count(when(col("SM_ACTION").contains("GET"), True)).alias(
                "IP_COUNT_GET"),
            f.count(when(col("SM_ACTION").contains("POST"), True)).alias(
                "IP_COUNT_POST"
            ),
            f.count(
                when(
                    (col("SM_ACTION").contains("GET"))
                    | (col("SM_ACTION").contains("POST")),
                    True,
                )
            ).alias("IP_COUNT_HTTP_METHODS"),
            f.count(
                when(
                    (col("SM_USERNAME").contains("ams"))
                    | (col("SM_RESOURCE").contains("AMS")),
                    True,
                )
            ).alias("IP_COUNT_OU_AMS"),
            f.count(when(col("SM_USERNAME").contains("cra-cp"), True)).alias(
                "IP_COUNT_OU_CMS"
            ),
            f.count(
                when(col("SM_USERNAME").contains("ou=Identity"), True)).alias(
                "IP_COUNT_OU_IDENTITY"
            ),
            f.count(when(col("SM_USERNAME").contains("ou=Credential"),
                         True)).alias(
                "IP_COUNT_OU_CRED"
            ),
            f.count(
                when(col("SM_USERNAME").contains("ou=SecureKey"), True)).alias(
                "IP_COUNT_OU_SECUREKEY"
            ),
            f.count(when(col("SM_RESOURCE").contains("mima"), True)).alias(
                "IP_COUNT_PORTAL_MYA"
            ),
            f.count(when(col("SM_RESOURCE").contains("myba"), True)).alias(
                "IP_COUNT_PORTAL_MYBA"
            ),
            f.countDistinct(col("SM_ACTION")).alias("IP_COUNT_UNIQUE_ACTIONS"),
            f.countDistinct(col("SM_EVENTID")).alias("IP_COUNT_UNIQUE_EVENTS"),
            f.countDistinct(col("CN")).alias("IP_COUNT_UNIQUE_USERNAME"),
            f.countDistinct(col("SM_RESOURCE")).alias(
                "IP_COUNT_UNIQUE_RESOURCES"),
            f.countDistinct(col("SM_SESSIONID")).alias(
                "IP_COUNT_UNIQUE_SESSIONS"),
            (
                f.size(
                    f.array_remove(
                        f.array_distinct(
                            f.collect_list(
                                regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                            )
                        ),
                        "",
                    ),
                )
            ).alias("IP_COUNT_PORTAL_RAC"),
            f.count(col("CRA_SEQ")).alias("IP_COUNT_RECORDS"),
            f.count(when(col("SM_EVENTID") == 13, True)).alias(
                "IP_COUNT_VISIT"),
            f.count(when(col("SM_EVENTID") == 11, True)).alias(
                "IP_COUNT_VALIDATE_ACCEPT"
            ),
            f.count(when(col("SM_EVENTID") == 12, True)).alias(
                "IP_COUNT_VALIDATE_REJECT"
            ),
            f.array_distinct(f.collect_list(col("SM_ACTION"))).alias(
                "IP_UNIQUE_SM_ACTIONS"
            ),
            f.array_distinct(f.collect_list(col("CN"))).alias(
                "IP_UNIQUE_USERNAME"),
            f.array_distinct(f.collect_list(col("SM_SESSIONID"))).alias(
                "IP_UNIQUE_SM_SESSION"
            ),
            f.array_distinct(f.collect_list(col("SM_RESOURCE"))).alias(
                "IP_UNIQUE_SM_PORTALS"
            ),
            f.array_distinct(f.collect_list(col("SM_TRANSACTIONID"))).alias(
                "IP_UNIQUE_SM_TRANSACTIONS"
            ),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                ),
                "",
            ).alias("IP_UNIQUE_USER_OU"),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                ),
                "",
            ).alias("IP_UNIQUE_REP_APP"),
            f.min(col("SM_TIMESTAMP")).alias("IP_TIMESTAMP"),
            f.size(
                f.array_remove(
                    f.array_distinct(
                        f.collect_list(
                            regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                    ),
                    "",
                )
            ).alias("IP_COUNT_UNIQUE_OU"),
        )
