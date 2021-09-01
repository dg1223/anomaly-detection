"""
A module to generate features regarding to session feature
Input: A dataframe with a CN row.
Output: A dataframe with the following features extracted:
COUNT_ADMIN_LOGOUT              	Count of Admin Logout events during the time window, defined by sm_eventid = 8.
COUNT_AUTH_ACCEPT	                Count of Auth Accept events during the time window, defined by sm_eventid = 1.
COUNT_ADMIN_ATTEMPT             	Count of Admin Accept events during the time window, defined by sm_eventid = 3.
COUNT_ADMIN_REJECT              	Count of Admin Reject events during the time window, defined by sm_eventid = 2.
COUNT_AZ_ACCEPT                    	Count of Az Accept events during the time window, defined by sm_eventid = 5.
COUNT_AZ_REJECT                 	Count of Az Reject events during the time window, defined by sm_eventid = 6.
COUNT_AUTH_LOGOUT               	Count of Auth Logout events during the time window, defined by sm_eventid = 10.
COUNT_VISIT                     	Count of Visit events during the time window, defined by sm_eventid = 13.
COUNT_AUTH_CHALLENGE            	Count of Auth Challenge events during the time window, defined by sm_eventid = 4.
COUNT_ADMIN_REJECT              	Count of Admin Reject events during the time window, defined by sm_eventid = 9.
COUNT_ADMIN_LOGIN               	Count of Admin Login events during the time window, defined by sm_eventid = 7.
COUNT_VALIDATE_ACCEPT           	Count of Validate Accept events during the time window, defined by sm_eventid = 11.
COUNT_VALIDATE_REJECT           	Count of Validate Reject events during the time window, defined by sm_eventid = 12.
COUNT_FAILED                    	Count of all Reject events during the time window, defined by sm_eventid = 2, 6 and 9.
COUNT_GET	                        Count of all GET HTTP actions in SM_ACTION during the time window.
COUNT_POST	                        Count of all POST HTTP actions in SM_ACTION during the time window.
COUNT_HTTP_METHODS	                Count of all GET and POST HTTP actions in SM_ACTION  during the time window.
COUNT_OU_AMS	                    Count of all ams or AMS occurrences in SM_USERNAME OR SM_RESOURCE during the time window.
COUNT_OU_CMS                    	Count of all cra-cp occurrences in SM_USERNAME during the time window.
COUNT_OU_IDENTITY               	Count of all ou=Identity occurrences in SM_USERNAME during the time window.
COUNT_OU_CRED                   	Count of all ou=Credential occurrences in SM_USERNAME during the time window.
COUNT_OU_SECUREKEY              	Count of all ou=SecureKey occurrences in SM_USERNAME during the time window.
COUNT_PORTAL_MYA                	Count of all mima occurrences in SM_RESOURCE during the time window.
COUNT_PORTAL_MYBA               	Count of all myba occurrences in SM_RESOURCE during the time window.
COUNT_UNIQUE_ACTIONS            	Count of distinct HTTP Actions in SM_ACTION during the time window.
COUNT_UNIQUE_IPS                	Count of distinct IPs in SM_CLIENTIP during the time window.
COUNT_UNIQUE_EVENTS	                Count of distinct EventIDs in SM_EVENTID  during the time window.
COUNT_UNIQUE_USERNAME	            Count of distinct CNs in CN during the time window.
COUNT_UNIQUE_RESOURCES          	Count of distinct Resource Strings in SM_RESOURCE during the time window.
COUNT_UNIQUE_SESSIONS           	Count of distinct SessionIDs in SM_SESSIONID during the time window.
COUNT_RECORDS	                    Counts number of CRA_SEQs (dataset primary key)
UNIQUE_SM_ACTIONS	                A distinct list of HTTP Actions in SM_ACTION during time window.
UNIQUE_SM_CLIENTIPS	                A distinct list of IPs in SM_CLIENTIPS during time window.
UNIQUE_SM_PORTALS               	A distinct list of Resource Strings in SM_RESOURCE during time window.
UNIQUE_SM_TRANSACTIONS          	A distinct list of Transaction Ids in SM_TRANSACTIONID during time window.
SM_SESSION_IDS                  	A distinct list of SessionIDs in SM_SESSIONID during the time window.
COUNT_UNIQUE_OU                 	A count of distinct Entries containing ou= and a string ending in , in SM_USERNAME during time window.
UNIQUE_USER_OU                  	A distinct list of Entries containing ou= and a string ending in , in SM_USERNAME during time window.
COUNT_PORTAL_RAC                	A count of Entries containing rep followed by a string ending in / in SM_RESOURCE during time window.
UNIQUE_PORTAL_RAC               	A distinct list of Entries containing rep followed by a string ending in / in SM_RESOURCE during time window.
UNIQUE_USER_APPS                	A distinct list of root nodes from each record in SM_RESOURCE during time window.
COUNTUNIQUE_USER_APPS           	A count of distinct root nodes from each record in SM_RESOURCE during time window.
USER_TIMESTAMP	                    Minimum timestamp in SM_TIMESTAMP during time window.
AVG_TIME_BT_RECORDS             	Average time between records during the time window.
MAX_TIME_BT_RECORDS	                Maximum time between records during the time window.
MIN_TIME_BT_RECORDS	                Minimum time between records during the time window.
UserLoginAttempts	                Total number of login attempts from the user within the specified time window
UserAvgFailedLoginsWithSameIPs	    Average number of failed logins with same IPs from the user (Note: the user may use multiple IPs; for each of the IPs, count the failed logins; then compute the average values of failed logins from all the IPs used by the same user)
UserNumOfAccountsLoginWithSameIPs	Total number of accounts visited by the IPs used by this user (this might be tricky to implement and expensive to compute, open to nixing).
UserNumOfPasswordChange	            Total number of requests for changing passwords by the user (See Seeing a password change from the events in `raw_logs` #65)
UserIsUsingUnusualBrowser	        Whether or not the browser used by the user in current time window is same as that in the previous time window, or any change within the current time window
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

# Feature generator based on Users (SM_CN or the column name you named)
# Execute cn_extractor before this transformer
# Otherwise, we have no SM_CN feature
class UserFeatureGenerator(Transformer):
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
        def __init__(self, *, window_length = 900, window_step = 900)
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
