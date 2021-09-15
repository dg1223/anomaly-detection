
"""
A module to generate features regarding to session feature
Input: A dataframe with a CN row.
Output: A dataframe with the following features extracted:
COUNT_ADMIN_LOGOUT              	Count of Admin Logout events during the
                                        time window, defined by
                                        sm_eventid = 8.
COUNT_AUTH_ACCEPT	                Count of Auth Accept events during the
                                        time window, defined by
                                        sm_eventid = 1.
COUNT_ADMIN_ATTEMPT             	Count of Admin Accept events during the
                                        time window, defined by
                                        sm_eventid = 3.
COUNT_ADMIN_REJECT              	Count of Admin Reject events during the
                                        time window, defined by
                                        sm_eventid = 2.
COUNT_AZ_ACCEPT                    	Count of Az Accept events during the time
                                        window, defined by
                                        sm_eventid = 5.
COUNT_AZ_REJECT                 	Count of Az Reject events during the time
                                        window, defined by
                                        sm_eventid = 6.
COUNT_AUTH_LOGOUT               	Count of Auth Logout events during the
                                        time window, defined by
                                        sm_eventid = 10.
COUNT_VISIT                     	Count of Visit events during the time
                                        window, defined by
                                        sm_eventid = 13.
COUNT_AUTH_CHALLENGE            	Count of Auth Challenge events during
                                        the time window, defined by
                                        sm_eventid = 4.
COUNT_ADMIN_REJECT              	Count of Admin Reject events during
                                        the time window, defined by
                                        sm_eventid = 9.
COUNT_ADMIN_LOGIN               	Count of Admin Login events during
                                        the time window, defined by
                                        sm_eventid = 7.
COUNT_VALIDATE_ACCEPT           	Count of Validate Accept events
                                        during the time window, defined by
                                        sm_eventid = 11.
COUNT_VALIDATE_REJECT           	Count of Validate Reject events
                                        during the time window, defined by
                                        sm_eventid = 12.
COUNT_FAILED                    	Count of all Reject events during
                                        the time window, defined by
                                        sm_eventid = 2, 6 and 9.
COUNT_GET	                        Count of all GET HTTP actions in
                                        SM_ACTION during the time window.
COUNT_POST	                        Count of all POST HTTP actions in
                                        SM_ACTION during the time window.
COUNT_HTTP_METHODS	                Count of all GET and POST HTTP actions
                                        in SM_ACTION  during the time window.
COUNT_OU_AMS	                    Count of all ams or AMS
                                        occurrences in SM_USERNAME OR
                                        SM_RESOURCE during the time window.
COUNT_OU_CMS                    	Count of all cra-cp occurrences
                                        in SM_USERNAME during the time window.
COUNT_OU_IDENTITY               	Count of all ou=Identity occurrences
                                        in SM_USERNAME during the time window.
COUNT_OU_CRED                   	Count of all ou=Credential occurrences
                                        in SM_USERNAME during the time window.
COUNT_OU_SECUREKEY              	Count of all ou=SecureKey occurrences
                                        in SM_USERNAME during the time window.
COUNT_PORTAL_MYA                	Count of all mima occurrences in
                                        SM_RESOURCE during the time window.
COUNT_PORTAL_MYBA               	Count of all myba occurrences in
                                        SM_RESOURCE during the time window.
COUNT_UNIQUE_ACTIONS            	Count of distinct HTTP Actions in
                                        SM_ACTION during the time window.
COUNT_UNIQUE_IPS                	Count of distinct IPs in SM_CLIENTIP
                                        during the time window.
COUNT_UNIQUE_EVENTS	                Count of distinct EventIDs in SM_EVENTID
                                        during the time window.
COUNT_UNIQUE_USERNAME	            Count of distinct CNs in CN during the
                                        time window.
COUNT_UNIQUE_RESOURCES          	Count of distinct Resource Strings in
                                        SM_RESOURCE during the time window.
COUNT_UNIQUE_SESSIONS           	Count of distinct SessionIDs in
                                        SM_SESSIONID during the time window.
COUNT_RECORDS	                    Counts number of CRA_SEQs
                                        (dataset primary key)
UNIQUE_SM_ACTIONS	                A distinct list of HTTP Actions in
                                        SM_ACTION during time window.
UNIQUE_SM_CLIENTIPS	                A distinct list of IPs in SM_CLIENTIPS
                                        during time window.
UNIQUE_SM_PORTALS               	A distinct list of Resource Strings in
                                        SM_RESOURCE during time window.
UNIQUE_SM_TRANSACTIONS          	A distinct list of Transaction Ids in
                                        SM_TRANSACTIONID during time window.
SM_SESSION_IDS                  	A distinct list of SessionIDs in
                                        SM_SESSIONID during the time window.
COUNT_UNIQUE_OU                 	A count of distinct Entries containing
                                        ou= and a string ending in ,
                                        in SM_USERNAME during time window.
UNIQUE_USER_OU                  	A distinct list of Entries containing
                                        ou= and a string ending in ,
                                        in SM_USERNAME during time window.
COUNT_PORTAL_RAC                	A count of Entries containing rep
                                        followed by a string ending in /
                                        in SM_RESOURCE during time window.
UNIQUE_PORTAL_RAC               	A distinct list of Entries containing
                                        rep followed by a string ending
                                        in / in SM_RESOURCE during time
                                        window.
UNIQUE_USER_APPS                	A distinct list of root nodes from each
                                        record in SM_RESOURCE during time
                                        window.
COUNTUNIQUE_USER_APPS           	A count of distinct root nodes from each
                                        record in SM_RESOURCE during time
                                        window.
USER_TIMESTAMP	                    Minimum timestamp in SM_TIMESTAMP during
                                        time window.
AVG_TIME_BT_RECORDS             	Average time between records during the
                                        time window.
MAX_TIME_BT_RECORDS	                Maximum time between records during the
                                        time window.
MIN_TIME_BT_RECORDS	                Minimum time between records during the
                                        time window.
UserLoginAttempts	                Total number of login attempts from the
                                        user within the specified time window
UserAvgFailedLoginsWithSameIPs	    Average number of failed logins with same
                                        IPs from the user (Note: the user may
                                        use multiple IPs; for each of the IPs,
                                        count the failed logins; then compute
                                        the average values of failed logins
                                        from all the IPs used by the same
                                        user)
UserNumOfAccountsLoginWithSameIPs	Total number of accounts visited by the
                                        IPs used by this user (this might be
                                        tricky to implement and expensive to
                                        compute, open to nixing).
UserNumOfPasswordChange	            Total number of requests for changing
                                        passwords by the user (See Seeing
                                        a password change from the events
                                        in `raw_logs` #65)
UserIsUsingUnusualBrowser	        Whether or not the browser used by the
                                        user in current time window is same
                                        as that in the previous time window,
                                        or any change within the current
                                        time window
"""

import pyspark.sql.functions as f

# Import Essential packages

from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.types import (
    TimestampType,
    StringType,
    LongType,
)
from pyspark.sql.window import Window

from src.caaswx.spark._transformers.sparknativetransformer import (
    SparkNativeTransformer,
)



class UserFeatureGenerator(SparkNativeTransformer):
    """
    A module to generate features related to users. It encompasses the features of user behavioural analytics

    Input: A Spark dataframe
    Expected columns in the input dataframe:
    Columns from raw_logs: CRA_SEQ, SM_ACTION, SM_RESOURCE, SM_CATEGORYID
    ,SM_EVENTID, SM_TIMESTAMP, SM_USERNAME, SM_CLIENTIP, SM_SESSIONID,
    SM_AGENTNAME, SM_TRANSACTION. Please refer to README.md for description.
    List of other required columns:
        +-------------+----------+----------------------------------+
        | Column_Name | Datatype | Description                      |
        +=============+==========+==================================+
        | self.getOr  | string   | Pivot Column containing the      |
        | Default("en |          | CommonNames for each user. It has|
        | tityName")  |          | to be an alpha-numeric string and|
        |             |          | it may contain  NULL values.     |
        +-------------+----------+----------------------------------+
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
            | COUNT_      | integer  | Count of Admin Login events      |
            | ADMIN_LOGIN |          | during the time window, defined  |
            |             |          | by sm_eventid = 7.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Auth accept events      |
            | AUTH_ACCEPT |          | during the time window, defined  |
            |             |          | by sm_eventid = 1.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Admin Logout events     |
            | ADMIN_LOGOUT|          | during the time window, defined  |
            |             |          | by sm_eventid = 8.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Admin reject events     |
            | ADMIN_REJECT|          | during the time window, defined  |
            |             |          | by sm_eventid = 9.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Admin attempt events    |
            | ADMIN_ATTEMP|          | during the time window, defined  |
            | T           |          | by sm_eventid = 3.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all Reject events       |
            | FAILED      |          | during the time window, defined  |
            |             |          | by sm_eventid = 2,6 and 9.       |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Visit events during the |
            | VISIT       |          | time window, defined by          |
            |             |          | sm_eventid = 13.                 |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Auth challenge events   |
            | AUTH_CHALLEN|          | during the time window, defined  |
            | GE          |          | by sm_eventid = 4.               |
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
            | COUNT_      | integer  | Count of Auth Logout events      |
            | AUTH_LOGOUT |          | during the time window, defined  |
            |             |          | by sm_eventid = 10.              |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Counts number of CRA_SEQs        |
            | RECORDS     |          | (dataset primary key)            |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct HTTP Actions   |
            | UNIQUE_ACTIO|          | in SM_ACTION during the time     |
            | NS          |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | A count of distinct root nodes   |
            | UNIQUE_USER_|          | from each record in SM_RESOURCE  |
            | APPS        |          | during time window.              |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Auth reject events      |
            | AUTH_REJECT |          | during the time window, defined  |
            |             |          | by sm_eventid = 2.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Az accept events        |
            | AZ_ACCEPT   |          | during the time window, defined  |
            |             |          | by sm_eventid = 5.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Az reject events        |
            | AZ_REJECT   |          | during the time window, defined  |
            |             |          | by sm_eventid = 6.               |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all ams or AMS          |
            | OU_AMS      |          | occurrences in SM_USERNAME OR    |
            |             |          | SM_RESOURCE during time window.  |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all cra-cp occurrences  |
            | OU_CMS      |          | in SM_USERNAME during the window.|
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Validate Accept events  |
            | VALIDATE_ACC|          | during the time window, defined) |
            | EPT         |          |  by sm_eventid = 11.             |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of Validate reject events  |
            | VALIDATE_REJ|          | during the time window, defined) |
            | ECT         |          |  by sm_eventid = 12.             |
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
            | COUNT_      | integer  | Count of all ou=Identity         |
            | OU_IDENTITY |          | occurrences in SM_USERNAME during|
            |             |          | the time window.                 |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all ou=Credential       |
            | OU_CRED     |          | occurrences in SM_USERNAME during|
            |             |          | the time window.                 |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of allou=SecureKey         |
            | OU_SECUREKEY|          | occurrences in SM_USERNAME during|
            |             |          | the time window.                 |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all                     |
            | PORTAL_MYA  |          | in SM_RESOURCE during the time   |
            |             |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of all                     |
            | PORTAL_MYBA |          | in SM_RESOURCE during the time   |
            |             |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct HTTP Actions   |
            | UNIQUE_ACTIO|          | in SM_ACTION during the time     |
            | NS          |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct EventIDs in    |
            | UNIQUE_EVENT|          | SM_EVENTID  during the time      |
            | S           |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct CNs in CN      |
            | UNIQUE_USERN|          | during the time window.          |
            | AME         |          |                                  |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | A distinct list of SessionIDs in |
            | UNIQUE_SESSI|          | SM_SESSIONID during time window. |
            | ON          |          |                                  |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct Resource       |
            | UNIQUE_RESOU|          | strings in SM_RESOURCE during    |
            | RCES        |          | the time window.                 |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | A count of Entries containing    |
            | UNIQUE_PORTA|          | rep followed by a string ending  |
            | L_RAC       |          | in "/"in SM_RESOURCE during the  |
            |             |          | time window.                     |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Count of distinct IPs in         |
            | UNIQUE_IPS  |          | SM_CLIENTIP during the time      |
            |             |          | window.                          |
            +-------------+----------+----------------------------------+
            | COUNT_      | integer  | Counts number of CRA_SEQs        |
            | RECORDS     |          | (dataset primary key)            |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of HTTP Actions  |
            | SM_ACTIONS  | <string> | in SM_ACTION during time window. |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of CNs           |
            | USERNAME    | <string> | in CN during time window.        |
            +-------------+----------+----------------------------------+
            | SM_SESSIONS |  array   | A distinct list of SessionIDs    |
            |             | <string> | in SM_SESSIONID during window.   |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of Resource      |
            | SM_PORTALS  | <string> | strings in SM_RESOURCE during    |
            |             |          | time window.                     |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of Transaction   |
            | SM_TRANSACTI| <string> | Ids in SM_TRANSACTIONID during   |
            | ONS         |          | time window.                     |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of Entries       |
            | USER_OU     | <string> | containing "ou="and a string     |
            |             |          | ending in "/" in SM_USERNAME     |
            |             |          | during time window.              |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of Entries       |
            | PORTAL_RAC  | <string> | containing rep followed by a     |
            |             |          | string ending in  "/" in         |
            |             |          | SM_RESOURCE during time window.  |
            +-------------+----------+----------------------------------+
            | UNIQUE_     |  array   | A distinct list of main apps     |
            | USER_APPS   | <string> | from each record in SM_RESOURCE  |
            |             |          | during time window.              |
            +-------------+----------+----------------------------------+
            | USER_TIMESTA| timestamp| Earliest timestamp during time   |
            | MP          |          | window.                          |
            +-------------+----------+----------------------------------+
            | AVG_TIME_   | double   | Average time between records     |
            | BT_RECORDS  |          | during the time window.          |
            +-------------+----------+----------------------------------+
            | MAX_TIME_   | double   | Maximum time between records     |
            | BT_RECORDS  |          | during the time window.          |
            +-------------+----------+----------------------------------+
            | MIN_TIME_   | double   | Minimum time between records     |
            | BT_RECORDS  |          | during the time window.          |
            +-------------+----------+----------------------------------+
            | UserLogin   | integer  | Total number of login attempts   |
            | Attempts    |          | from the user within the window. |
            +-------------+----------+----------------------------------+
            | UserAvgFaile| integer  | Average number of failed logins  |
            | dLoginsWithS|          | with same IPs from the user.     |
            | ameIPs      |          |                                  |
            +-------------+----------+----------------------------------+
            | UserNumOfAcc| integer  | Total number of accounts visited |
            | ountsLoginWi|          | by the IPs used by this user     |
            | thSameIPs   |          |                                  |
            +-------------+----------+----------------------------------+
            | UserNumOfPas| integer  | Total number of requests for     |
            | swordChange |          | changing passwords by the user.  |
            +-------------+----------+----------------------------------+
            | UserIsUsing | integer  | Whether or not the browser used  |
            | UnusualBrows|          | by user in current time  window  |
            | er          |          | is same as that in the previous  |
            |             |          | time window                      |
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

    entity_name = Param(
        Params._dummy(),
        "entity_name",
        "Name of the column to perform aggregation on, together with the "
        + "sliding window.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(self):
        """
        :param entity_name: Pivot Column which should contain the CommonNames for each user.
        :param window_length: Length of the sliding window (in seconds)
        :param window_step: Length of the sliding window's step-size (in seconds)
        :type entity_name: string
        :type window_length: long
        :type window_step: long

        :Example:
        >>> from userfeaturegenerator import UserFeatureGenerator
        >>> feature_generator = UserFeatureGenerator(entity_name = "CN",
                window_length = 1800, window_step = 1800)
        >>> features = feature_generator.transform(dataset = input_dataset)
        """
        super().__init__()
        self._setDefault(entity_name="CN", window_length=900, window_step=900)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                  inputCols=None, outputCols=None)
        Sets params for this UserFeatureGenerator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_window_length(self, value):
        """
        Sets this UserFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def set_entity_name(self, value):
        """
        Sets this UserFeatureGenerator's entity name.
        """
        self._set(entity_name=value)

    def set_window_step(self, value):
        """
        Sets this UserFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_EVENTID": ["SM_EVENTID", LongType()],
        "SM_RESOURCE": ["SM_RESOURCE", StringType()],
        "SM_CLIENTIP": ["SM_CLIENTIP", StringType()],
        "SM_USERNAME": ["SM_USERNAME", StringType()],
        "SM_ACTION": ["SM_ACTION", StringType()],
        "SM_SESSIONID": ["SM_SESSIONID", StringType()],
        "SM_TRANSACTIONID": ["SM_TRANSACTIONID", StringType()],
    }

    def _transform(self, dataset):

        pivot = str(self.getOrDefault("entity_name"))
        dataset_copy = dataset
        ts_window = Window.partitionBy(
            str(self.getOrDefault("entity_name"))
        ).orderBy("SM_TIMESTAMP")
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
        ip_counts_df = dataset.groupBy("SM_CLIENTIP").agg(
            f.countDistinct("SM_USERNAME").alias("distinct_usernames_for_ip")
        )
        dataset = dataset.join(ip_counts_df, on="SM_CLIENTIP")
        dataset = dataset.groupby(
            pivot,
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            f.count(when(col("SM_EVENTID") == 8, True)).alias(
                "COUNT_ADMIN_LOGOUT"
            ),
            f.count(when(col("SM_EVENTID") == 1, True)).alias(
                "COUNT_AUTH_ACCEPT"
            ),
            f.count(when(col("SM_EVENTID") == 3, True)).alias(
                "COUNT_ADMIN_ATTEMPT"
            ),
            f.count(when(col("SM_EVENTID") == 2, True)).alias(
                "COUNT_AUTH_REJECT"
            ),
            f.count(when(col("SM_EVENTID") == 5, True)).alias(
                "COUNT_AZ_ACCEPT"
            ),
            f.count(when(col("SM_EVENTID") == 6, True)).alias(
                "COUNT_AZ_REJECT"
            ),
            f.count(when(col("SM_EVENTID") == 10, True)).alias(
                "COUNT_AUTH_LOGOUT"
            ),
            f.count(when(col("SM_EVENTID") == 13, True)).alias("COUNT_VISIT"),
            f.count(when(col("SM_EVENTID") == 4, True)).alias(
                "COUNT_AUTH_CHALLENGE"
            ),
            f.count(when(col("SM_EVENTID") == 9, True)).alias(
                "COUNT_ADMIN_REJECT"
            ),
            f.count(when(col("SM_EVENTID") == 7, True)).alias(
                "COUNT_ADMIN_LOGIN"
            ),
            f.count(when(col("SM_EVENTID") == 11, True)).alias(
                "COUNT_VALIDATE_ACCEPT"
            ),
            f.count(when(col("SM_EVENTID") == 12, True)).alias(
                "COUNT_VALIDATE_REJECT"
            ),
            f.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("COUNT_FAILED"),
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
            f.count(
                when(
                    (col("SM_USERNAME").contains("ams"))
                    | (col("SM_RESOURCE").contains("AMS")),
                    True,
                )
            ).alias("COUNT_OU_AMS"),
            f.count(when(col("SM_USERNAME").contains("cra-cp"), True)).alias(
                "COUNT_OU_CMS"
            ),
            f.count(
                when(col("SM_USERNAME").contains("ou=Identity"), True)
            ).alias("COUNT_OU_IDENTITY"),
            f.count(
                when(col("SM_USERNAME").contains("ou=Credential"), True)
            ).alias("COUNT_OU_CRED"),
            f.count(
                when(col("SM_USERNAME").contains("ou=SecureKey"), True)
            ).alias("COUNT_OU_SECUREKEY"),
            f.count(when(col("SM_RESOURCE").contains("mima"), True)).alias(
                "COUNT_PORTAL_MYA"
            ),
            f.count(when(col("SM_RESOURCE").contains("myba"), True)).alias(
                "COUNT_PORTAL_MYBA"
            ),
            f.countDistinct(col("SM_ACTION")).alias("COUNT_UNIQUE_ACTIONS"),
            f.countDistinct(col("SM_CLIENTIP")).alias("COUNT_UNIQUE_IPS"),
            f.countDistinct(col("SM_EVENTID")).alias("COUNT_UNIQUE_EVENTS"),
            f.countDistinct(col("SM_RESOURCE")).alias(
                "COUNT_UNIQUE_RESOURCES"
            ),
            f.countDistinct(col("SM_SESSIONID")).alias(
                "COUNT_UNIQUE_SESSIONS"
            ),
            f.countDistinct(col("CN")).alias("COUNT_UNIQUE_USERNAME"),
            f.count(col("CRA_SEQ")).alias("COUNT_RECORDS"),
            f.array_distinct(f.collect_list(col("SM_ACTION"))).alias(
                "UNIQUE_SM_ACTIONS"
            ),
            f.array_distinct(f.collect_list(col("SM_CLIENTIP"))).alias(
                "UNIQUE_SM_CLIENTIPS"
            ),
            f.array_distinct(f.collect_list(col("SM_RESOURCE"))).alias(
                "UNIQUE_SM_PORTALS"
            ),
            f.array_distinct(f.collect_list(col("SM_TRANSACTIONID"))).alias(
                "UNIQUE_SM_TRANSACTIONS"
            ),
            f.array_distinct(f.collect_list(col("SM_SESSIONID"))).alias(
                "SM_SESSION_IDS"
            ),
            f.size(
                f.array_remove(
                    f.array_distinct(
                        f.collect_list(
                            regexp_extract("SM_USERNAME", r"ou=(.*?),", 0)
                        )
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_OU"),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_USERNAME", r"ou=(.*?),", 0)
                    )
                ),
                "",
            ).alias("UNIQUE_USER_OU"),
            f.size(
                f.array_remove(
                    f.array_distinct(
                        f.collect_list(
                            regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                        )
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_REP"),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)
                    )
                ),
                "",
            ).alias("UNIQUE_PORTAL_RAC"),
            f.array_remove(
                f.array_distinct(
                    f.collect_list(
                        regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)
                    )
                ),
                "",
            ).alias("UNIQUE_USER_APPS"),
            f.size(
                f.array_remove(
                    f.array_distinct(
                        f.collect_list(
                            regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)
                        )
                    ),
                    "",
                )
            ).alias("COUNTUNIQUE_USER_APPS"),
            f.min(col("SM_TIMESTAMP")).alias("USER_TIMESTAMP"),
            f.max("SM_CONSECUTIVE_TIME_DIFFERENCE").alias(
                "MAX_TIME_BT_RECORDS"
            ),
            f.min("SM_CONSECUTIVE_TIME_DIFFERENCE").alias(
                "MIN_TIME_BT_RECORDS"
            ),
            f.round(f.mean("SM_CONSECUTIVE_TIME_DIFFERENCE"), 5).alias(
                "AVG_TIME_BT_RECORDS"
            ),
            f.count(
                when(
                    (dataset["SM_EVENTID"] >= 1)
                    & (dataset["SM_EVENTID"] <= 6),
                    True,
                )
            ).alias("UserLoginAttempts"),
            f.count(
                when(dataset["SM_RESOURCE"].contains("changePassword"), True)
            ).alias("UserNumOfPasswordChange"),
            f.sum("distinct_usernames_for_ip").alias(
                "UserNumOfAccountsLoginWithSameIPs"
            ),
            f.sort_array(f.collect_set("SM_AGENTNAME")).alias("browsersList"),
        )

        agent_window = Window.partitionBy(pivot).orderBy("window")
        dataset = dataset.withColumn(
            "SM_PREVIOUS_AGENTNAME",
            f.lag(dataset["browsersList"]).over(agent_window),
        )
        dataset = dataset.withColumn(
            "UserIsUsingUnusualBrowser",
            f.when(
                (f.isnull("SM_PREVIOUS_AGENTNAME"))
                | (
                    dataset["browsersList"] == dataset["SM_PREVIOUS_AGENTNAME"]
                ),
                0,
            ).otherwise(1),
        )
        dataset = dataset.drop("browsersList")
        dataset = dataset.drop("SM_PREVIOUS_AGENTNAME")
        user_avg_failed_logins_with_same_ips_df = (
            dataset_copy.groupby(
                pivot,
                "SM_CLIENTIP",
                window(
                    "SM_TIMESTAMP",
                    str(self.getOrDefault("window_length")) + " seconds",
                    str(self.getOrDefault("window_step")) + " seconds",
                ),
            )
            .agg(
                f.count(
                    when(
                        (dataset_copy["SM_EVENTID"] == 2)
                        | (dataset_copy["SM_EVENTID"] == 6)
                        | (dataset_copy["SM_EVENTID"] == 9)
                        | (dataset_copy["SM_EVENTID"] == 12),
                        True,
                    )
                ).alias("countOfFailedLogins")
            )
            .groupBy(pivot, "window")
            .agg(
                f.avg("countOfFailedLogins").alias(
                    "UserAvgFailedLoginsWithSameIPs"
                )
            )
        )

        dataset = dataset.join(
            user_avg_failed_logins_with_same_ips_df, [pivot, "window"]
        )

        return dataset
