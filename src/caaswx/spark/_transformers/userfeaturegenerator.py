import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.types import (
    ArrayType,
    TimestampType,
    StringType,
    LongType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer


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
        setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
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
        ts_window = Window.partitionBy(str(self.getOrDefault("entity_name"))).orderBy(
            "SM_TIMESTAMP"
        )
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
            F.countDistinct("SM_USERNAME").alias("distinct_usernames_for_ip")
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
            F.count(when(col("SM_EVENTID") == 8, True)).alias("COUNT_ADMIN_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 1, True)).alias("COUNT_AUTH_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 3, True)).alias("COUNT_ADMIN_ATTEMPT"),
            F.count(when(col("SM_EVENTID") == 2, True)).alias("COUNT_AUTH_REJECT"),
            F.count(when(col("SM_EVENTID") == 5, True)).alias("COUNT_AZ_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 6, True)).alias("COUNT_AZ_REJECT"),
            F.count(when(col("SM_EVENTID") == 10, True)).alias("COUNT_AUTH_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 13, True)).alias("COUNT_VISIT"),
            F.count(when(col("SM_EVENTID") == 4, True)).alias("COUNT_AUTH_CHALLENGE"),
            F.count(when(col("SM_EVENTID") == 9, True)).alias("COUNT_ADMIN_REJECT"),
            F.count(when(col("SM_EVENTID") == 7, True)).alias("COUNT_ADMIN_LOGIN"),
            F.count(when(col("SM_EVENTID") == 11, True)).alias("COUNT_VALIDATE_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 12, True)).alias("COUNT_VALIDATE_REJECT"),
            F.count(
                when(
                    (col("SM_EVENTID") == 2)
                    | (col("SM_EVENTID") == 6)
                    | (col("SM_EVENTID") == 9),
                    True,
                )
            ).alias("COUNT_FAILED"),
            F.count(when(col("SM_ACTION").contains("GET"), True)).alias("COUNT_GET"),
            F.count(when(col("SM_ACTION").contains("POST"), True)).alias("COUNT_POST"),
            F.count(
                when(
                    (col("SM_ACTION").contains("GET"))
                    | (col("SM_ACTION").contains("POST")),
                    True,
                )
            ).alias("COUNT_HTTP_METHODS"),
            F.count(
                when(
                    (col("SM_USERNAME").contains("ams"))
                    | (col("SM_RESOURCE").contains("AMS")),
                    True,
                )
            ).alias("COUNT_OU_AMS"),
            F.count(when(col("SM_USERNAME").contains("cra-cp"), True)).alias(
                "COUNT_OU_CMS"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=Identity"), True)).alias(
                "COUNT_OU_IDENTITY"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=Credential"), True)).alias(
                "COUNT_OU_CRED"
            ),
            F.count(when(col("SM_USERNAME").contains("ou=SecureKey"), True)).alias(
                "COUNT_OU_SECUREKEY"
            ),
            F.count(when(col("SM_RESOURCE").contains("mima"), True)).alias(
                "COUNT_PORTAL_MYA"
            ),
            F.count(when(col("SM_RESOURCE").contains("myba"), True)).alias(
                "COUNT_PORTAL_MYBA"
            ),
            F.countDistinct(col("SM_ACTION")).alias("COUNT_UNIQUE_ACTIONS"),
            F.countDistinct(col("SM_CLIENTIP")).alias("COUNT_UNIQUE_IPS"),
            F.countDistinct(col("SM_EVENTID")).alias("COUNT_UNIQUE_EVENTS"),
            F.countDistinct(col("SM_RESOURCE")).alias("COUNT_UNIQUE_RESOURCES"),
            F.countDistinct(col("SM_SESSIONID")).alias("COUNT_UNIQUE_SESSIONS"),
            F.countDistinct(col("CN")).alias("COUNT_UNIQUE_USERNAME"),
            F.count(col("CRA_SEQ")).alias("COUNT_RECORDS"),
            F.array_distinct(F.collect_list(col("SM_ACTION"))).alias(
                "UNIQUE_SM_ACTIONS"
            ),
            F.array_distinct(F.collect_list(col("SM_CLIENTIP"))).alias(
                "UNIQUE_SM_CLIENTIPS"
            ),
            F.array_distinct(F.collect_list(col("SM_RESOURCE"))).alias(
                "UNIQUE_SM_PORTALS"
            ),
            F.array_distinct(F.collect_list(col("SM_TRANSACTIONID"))).alias(
                "UNIQUE_SM_TRANSACTIONS"
            ),
            F.array_distinct(F.collect_list(col("SM_SESSIONID"))).alias(
                "SM_SESSION_IDS"
            ),
            F.size(
                F.array_remove(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_OU"),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
                ),
                "",
            ).alias("UNIQUE_USER_OU"),
            F.size(
                F.array_remove(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                    ),
                    "",
                )
            ).alias("COUNT_UNIQUE_REP"),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
                ),
                "",
            ).alias("UNIQUE_PORTAL_RAC"),
            F.array_remove(
                F.array_distinct(
                    F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
                ),
                "",
            ).alias("UNIQUE_USER_APPS"),
            F.size(
                F.array_remove(
                    F.array_distinct(
                        F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
                    ),
                    "",
                )
            ).alias("COUNTUNIQUE_USER_APPS"),
            F.min(col("SM_TIMESTAMP")).alias("USER_TIMESTAMP"),
            F.max("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("MAX_TIME_BT_RECORDS"),
            F.min("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("MIN_TIME_BT_RECORDS"),
            F.round(F.mean("SM_CONSECUTIVE_TIME_DIFFERENCE"), 5).alias(
                "AVG_TIME_BT_RECORDS"
            ),
            F.count(
                when((dataset["SM_EVENTID"] >= 1) & (dataset["SM_EVENTID"] <= 6), True)
            ).alias("UserLoginAttempts"),
            F.count(
                when(dataset["SM_RESOURCE"].contains("changePassword"), True)
            ).alias("UserNumOfPasswordChange"),
            F.sum("distinct_usernames_for_ip").alias(
                "UserNumOfAccountsLoginWithSameIPs"
            ),
            F.sort_array(F.collect_set("SM_AGENTNAME")).alias("browsersList"),
        )

        agent_window = Window.partitionBy(pivot).orderBy("window")
        dataset = dataset.withColumn(
            "SM_PREVIOUS_AGENTNAME", F.lag(dataset["browsersList"]).over(agent_window)
        )
        dataset = dataset.withColumn(
            "UserIsUsingUnusualBrowser",
            F.when(
                (F.isnull("SM_PREVIOUS_AGENTNAME"))
                | (dataset["browsersList"] == dataset["SM_PREVIOUS_AGENTNAME"]),
                0,
            ).otherwise(1),
        )
        dataset = dataset.drop("browsersList")
        dataset = dataset.drop("SM_PREVIOUS_AGENTNAME")
        UserAvgFailedLoginsWithSameIPs_df = (
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
                F.count(
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
            .agg(F.avg("countOfFailedLogins").alias("UserAvgFailedLoginsWithSameIPs"))
        )

        dataset = dataset.join(UserAvgFailedLoginsWithSameIPs_df, [pivot, "window"])

        return dataset
