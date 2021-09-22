import pyspark.sql.functions as f
from pyspark import keyword_only
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
)
from pyspark.sql.window import Window

from .sparknativetransformer import SparkNativeTransformer


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
        "a string used to define the pivot column for partitioning the time "
        "window",
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
        :param window_step: Length of the sliding window's step-size (in
        seconds) :param interval_threshold: An integer used to define
        feature NumOfUsersWithEqualIntervalBtnReqs :param interval_epsilon:
        A float used to define feature NumOfUsersWithEqualIntervalBtnReqs
        :param partioning_entity: A string used to define the pivot column
        for partitioning the time window :type window_length: long :type
        window_step: long :type interval_threshold: integer :type
        interval_epsilon: float :type partioning_entity: string

        :Example:
        from serverfeaturegenerator import ServerFeatureGenerator
        feature_generator = ServerFeatureGenerator(window_length = 1800,
            window_step = 1800, interval_threshold = 4, interval_epsilon = 0.3,
             partioning_entity = "CN")
        features = feature_generator.transform(dataset = input_dataset)
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
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
        window_length=900,
        window_step=900,
        interval_threshold=2,
        interval_epsilon=0.2,
        partioning_entity="SM_USERNAME",
    ):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None,
        thresholds=None, inputCols=None, outputCols=None) Sets params for
        this ServerFeatureGenerator.
        """

        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_window_length(self, value):
        """
        Sets this ServerFeatureGenerator's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this ServerFeatureGenerator's window step size.
        """
        self._set(window_step=value)

    def setinterval_threshold(self, value):
        """
        Sets this ServerFeatureGenerator's interval threshold
        """
        self._set(interval_threshold=value)

    def set_interval_epsilon(self, value):
        """
        Sets this ServerFeatureGenerator's interval epsilon
        """
        self._set(interval_epsilon=value)

    def set_partioning_entity(self, value):
        """
        Sets this ServerFeatureGenerator's interval epsilon
        """
        self._set(partioning_entity=value)

    def process_data_frame_with_window(self, dataset):
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
        dataset = self.process_data_frame_with_window(dataset)

        first_five_features_df = dataset.groupby(
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            )
        ).agg(
            f.col("window")["start"].alias("StartTime"),
            f.col("window")["end"].alias("EndTime"),
            f.count(
                when(
                    (dataset["SM_EVENTID"] >= 1)
                    & (dataset["SM_EVENTID"] <= 6),
                    True,
                )
            ).alias("VolOfAllLoginAttempts"),
            f.count(
                when(
                    (dataset["SM_EVENTID"] == 2)
                    | (dataset["SM_EVENTID"] == 6)
                    | (dataset["SM_EVENTID"] == 9)
                    | (dataset["SM_EVENTID"] == 12),
                    True,
                )
            ).alias("VolOfAllFailedLogins"),
            f.count(
                when(dataset["SM_RESOURCE"].contains("changePassword"), True)
            ).alias("NumOfReqsToChangePasswords"),
        )

        max_of_failed_logins_with_same_ips_df = (
            dataset.groupby(
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
                        (dataset["SM_EVENTID"] == 2)
                        | (dataset["SM_EVENTID"] == 6)
                        | (dataset["SM_EVENTID"] == 9)
                        | (dataset["SM_EVENTID"] == 12),
                        True,
                    )
                ).alias("temp")
            )
            .groupBy("window")
            .agg(f.max("temp").alias("MaxOfFailedLoginsWithSameIPs"))
        )

        num_of_ips_login_multi_accounts_df = dataset.groupby(
            "SM_CLIENTIP",
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            f.countDistinct(str(self.getOrDefault("partioning_entity"))).alias(
                "UNIQUE_USERS_COUNT"
            )
        )

        num_of_ips_login_multi_accounts_df = (
            num_of_ips_login_multi_accounts_df.groupBy("window").agg(
                f.count(
                    when(
                        num_of_ips_login_multi_accounts_df[
                            "UNIQUE_USERS_COUNT"
                        ]
                        > 1,
                        True,
                    )
                ).alias("NumOfIPsLoginMultiAccounts")
            )
        )

        temp_df = dataset.groupBy(
            str(self.getOrDefault("partioning_entity")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            f.count(
                f.when(
                    (
                        dataset["SM_CONSECUTIVE_TIME_DIFFERENCE"]
                        >= self.getOrDefault("interval_threshold")
                        - self.getOrDefault("interval_epsilon")
                    ),
                    True,
                )
            ).alias("temporary_column")
        )

        num_of_users_with_equal_interval_btn_reqs_df = temp_df.groupBy(
            "window"
        ).agg(
            f.count(when(temp_df["temporary_column"] != 0, True)).alias(
                "NumOfUsersWithEqualIntervalBtnReqs"
            )
        )

        result_df = first_five_features_df.join(
            max_of_failed_logins_with_same_ips_df, on="window"
        )
        result_df = result_df.join(
            num_of_ips_login_multi_accounts_df, on="window"
        )
        result_df = result_df.join(
            num_of_users_with_equal_interval_btn_reqs_df, on="window"
        )

        return result_df
