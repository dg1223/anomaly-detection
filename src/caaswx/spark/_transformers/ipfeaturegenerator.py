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


class IPFeatureGenerator(Transformer):
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
        Sets this IPFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this IPFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    def set_entity_name(self, value):
        """
        Sets this IPFeatureGenerator's window step size.
        """
        self._set(entity_name=value)

    def _transform(self, dataset):
        """
        Transforms the given dataset by deriving IP features
        and returning a new dataframe of those features
        """
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
            ).alias("IP_APP"),
            F.mean("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("IP_AVG_TIME_BT_RECORDS"),
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
            F.count(when(col("SM_EVENTID") == 2, True)).alias("IP_COUNT_ADMIN_REJECT"),
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
            F.countDistinct(col("SM_CN")).alias("IP_COUNT_UNIQUE_USERNAME"),
            F.countDistinct(col("SM_RESOURCE")).alias("IP_COUNT_UNIQUE_RESOURCES"),
            F.countDistinct(col("SM_SESSIONID")).alias("IP_COUNT_UNIQUE_SESSIONS"),
            (F.size(F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
            ))-1).alias(
                "IP_COUNT_PORTAL_RAC"
            ),
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
            F.array_distinct(F.collect_list(col("SM_CN"))).alias("IP_UNIQUE_USERNAME"),
            F.array_distinct(F.collect_list(col("SM_SESSIONID"))).alias(
                "IP_UNIQUE_SM_SESSION"
            ),
            F.array_distinct(F.collect_list(col("SM_RESOURCE"))).alias(
                "IP_UNIQUE_SM_PORTALS"
            ),
            F.array_distinct(F.collect_list(col("SM_TRANSACTIONID"))).alias(
                "IP_UNIQUE_SM_TRANSACTIONS"
            ),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
            ).alias("IP_UNIQUE_USER_OU"),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
            ).alias("IP_UNIQUE_REP_APP"),
            F.min(col("SM_TIMESTAMP")).alias("IP_TIMESTAMP"),
            F.countDistinct(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0)).alias(
                "IP_COUNT_UNIQUE_OU"
            ),
        )
