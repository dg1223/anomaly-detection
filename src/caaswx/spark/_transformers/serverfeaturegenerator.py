import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import (
    when,
    lag,
    isnull,
)
from pyspark.sql.functions import window
from pyspark.sql.types import (
    LongType,
    StringType,
    TimestampType,
    StructType,
    StructField,
    ArrayType,
)
from pyspark.sql.window import Window
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer


class ServerFeatureGenerator(SparkNativeTransformer):
    """
    A module to generate features related to server.
    The ServerFeatureGenerator will aggregate simply on time window
    and encompass details about login attempts in different time window
    along with users' behaviours.

        Input: A Spark dataframe.
        Columns from raw_logs: SM_EVENTID, SM_CLIENTIP, SM_TIMESTAMP,
        SM_AGENTNAME, SM_RESOURCE.
        Please refer to README.md for description.
        List of other required columns:
        +-------------+----------+----------------------------------+
        | Column_Name | Datatype | Description                      |
        +=============+==========+==================================+
        | self.getOr  | string   | the column to be targeted for    |
        | Default("pa |          | aggregation for generating the   |
        | rtioning_e  |          | immediate preious timestamp. It  |
        | ntity       |          | (by default set to "SM_USERNAME")|
        +-------------+----------+----------------------------------+

        Output features:

        +-------------+----------+----------------------------------+
        | Column_Name | Datatype | Description                      |
        +=============+==========+==================================+
        | StartTime   | timestamp| The beginning of a time window   |
        +-------------+----------+----------------------------------+
        | EndTime     | timestamp| The end of a time window         |
        +-------------+----------+----------------------------------+
        | VolOfAllLogi| integer  | Number of all login attempts     |
        | nAttempts   |          | in the specified time window.    |
        +-------------+----------+----------------------------------+
        | VolOfAllFail| integer  |  Number of all failed login      |
        | edLogins    |          | attempts in the  time window.    |
        +-------------+----------+----------------------------------+
        | MaxOfFailed | integer  | Maximum Number of all failed     |
        | LoginsWithSa|          | login attempts with same IPs.    |
        | meIPs       |          |                                  |
        +-------------+----------+----------------------------------+
        | NumOfIPsLogi| integer  | Number of IPs used for logging   |
        | nMultiAccoun|          | into multiple accounts.          |
        | ts          |          |                                  |
        +-------------+----------+----------------------------------+
        | NumOfReqsTo | integer  | Number of requests to change     |
        | ChangePasswo|          | passwords in the time window.    |
        | rds         |          |                                  |
        +-------------+----------+----------------------------------+
        | NumOfUsersWi| integer  | Number of users with at least    |
        | thEqualInter|          | "interval_threshold" intervals   |
        | valBtnReqs  |          | between consecutive requests that|
        |             |          | are equal up to precision        |
        |             |          | "interval_epsilon".              |
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

    interval_threshold = Param(
        Params._dummy(),
        "interval_threshold",
        "an integer used to define feature NumOfUsersWithEqualIntervalBtnReqs",
        typeConverter=TypeConverters.toInt,
    )

    interval_epsilon = Param(
        Params._dummy(),
        "interval_epsilon",
        "a float used to define feature NumOfUsersWithEqualIntervalBtnReqs",
        typeConverter=TypeConverters.toFloat,
    )

    partioning_entity = Param(
        Params._dummy(),
        "partioning_entity",
        "a string used to define the pivot column for partitioning the time window",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        window_length=900,
        window_step=900,
        interval_threshold=2,
        interval_epsilon=0.2,
        partioning_entity="SM_USERNAME",
    ):
        """
        :param window_length: Length of the sliding window (in seconds)
        :param window_step: Length of the sliding window's step-size (in seconds)
        :param interval_threshold: An integer used to define feature NumOfUsersWithEqualIntervalBtnReqs
        :param interval_epsilon: A float used to define feature NumOfUsersWithEqualIntervalBtnReqs
        :param partioning_entity: A string used to define the pivot column for partitioning the time window
        :type window_length: long
        :type window_step: long
        :type interval_threshold: integer
        :type interval_epsilon: float
        :type partioning_entity: string

        :Example:
        >>> from serverfeaturegenerator import ServerFeatureGenerator
        >>> feature_generator = ServerFeatureGenerator(window_length = 1800,
            window_step = 1800, interval_threshold = 4, interval_epsilon = 0.3, partioning_entity = "CN")
        >>> features = feature_generator.transform(dataset = input_dataset)
        """
        super(ServerFeatureGenerator, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            interval_threshold=2,
            interval_epsilon=0.2,
            partioning_entity="SM_USERNAME",
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        window_length=900,
        window_step=900,
        interval_threshold=2,
        interval_epsilon=0.2,
        partioning_entity="SM_USERNAME",
    ):
        """
    setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
              inputCols=None, outputCols=None)
    Sets params for this ServerFeatureGenerator.
    """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setwindow_length(self, value):
        """
        Sets this ServerFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def setwindow_step(self, value):
        """
        Sets this ServerFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    def setinterval_threshold(self, value):
        """
        Sets this ServerFeatureGenerator's interval threshold
        """
        self._set(interval_threshold=value)

    def setinterval_epsilon(self, value):
        """
        Sets this ServerFeatureGenerator's interval epsilon
        """
        self._set(interval_epsilon=value)

    def setpartioning_entity(self, value):
        """
        Sets this ServerFeatureGenerator's interval epsilon
        """
        self._set(partioning_entity=value)

    def process_DataFrame_with_Window(self, dataset):
        pivot_entity = str(self.getOrDefault("partioning_entity"))
        ts_window = Window.partitionBy(pivot_entity).orderBy("SM_TIMESTAMP")

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
        return dataset

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_EVENTID": ["SM_EVENTID", LongType()],
        "SM_RESOURCE": ["SM_RESOURCE", StringType()],
        "SM_CLIENTIP": ["SM_CLIENTIP", StringType()],
    }

    def _transform(self, dataset):

        dataset = self.process_DataFrame_with_Window(dataset)

        first_five_features_df = dataset.groupby(
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            )
        ).agg(
            F.col("window")["start"].alias("StartTime"),
            F.col("window")["end"].alias("EndTime"),
            F.count(
                when((dataset["SM_EVENTID"] >= 1) & (dataset["SM_EVENTID"] <= 6), True)
            ).alias("VolOfAllLoginAttempts"),
            F.count(
                when(
                    (dataset["SM_EVENTID"] == 2)
                    | (dataset["SM_EVENTID"] == 6)
                    | (dataset["SM_EVENTID"] == 9)
                    | (dataset["SM_EVENTID"] == 12),
                    True,
                )
            ).alias("VolOfAllFailedLogins"),
            F.count(
                when(dataset["SM_RESOURCE"].contains("changePassword"), True)
            ).alias("NumOfReqsToChangePasswords"),
        )

        MaxOfFailedLoginsWithSameIPs_df = (
            dataset.groupby(
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
                        (dataset["SM_EVENTID"] == 2)
                        | (dataset["SM_EVENTID"] == 6)
                        | (dataset["SM_EVENTID"] == 9)
                        | (dataset["SM_EVENTID"] == 12),
                        True,
                    )
                ).alias("temp")
            )
            .groupBy("window")
            .agg(F.max("temp").alias("MaxOfFailedLoginsWithSameIPs"))
        )

        NumOfIPsLoginMultiAccounts_df = dataset.groupby(
            "SM_CLIENTIP",
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            F.countDistinct(str(self.getOrDefault("partioning_entity"))).alias(
                "UNIQUE_USERS_COUNT"
            )
        )

        NumOfIPsLoginMultiAccounts_df = NumOfIPsLoginMultiAccounts_df.groupBy(
            "window"
        ).agg(
            F.count(
                when(NumOfIPsLoginMultiAccounts_df["UNIQUE_USERS_COUNT"] > 1, True)
            ).alias("NumOfIPsLoginMultiAccounts")
        )

        temp_df = dataset.groupBy(
            str(self.getOrDefault("partioning_entity")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            F.count(
                F.when(
                    (
                        dataset["SM_CONSECUTIVE_TIME_DIFFERENCE"]
                        >= self.getOrDefault("interval_threshold")
                        - self.getOrDefault("interval_epsilon")
                    ),
                    True,
                )
            ).alias("temporary_column")
        )

        NumOfUsersWithEqualIntervalBtnReqs_df = temp_df.groupBy("window").agg(
            F.count(when(temp_df["temporary_column"] != 0, True)).alias(
                "NumOfUsersWithEqualIntervalBtnReqs"
            )
        )

        result_df = first_five_features_df.join(
            MaxOfFailedLoginsWithSameIPs_df, on="window"
        )
        result_df = result_df.join(NumOfIPsLoginMultiAccounts_df, on="window")
        result_df = result_df.join(NumOfUsersWithEqualIntervalBtnReqs_df, on="window")

        return result_df
