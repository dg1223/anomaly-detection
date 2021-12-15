import httpagentparser
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    TypeConverters,
    Param,
    Params,
)
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    when,
    lag,
    isnull,
    udf,
    regexp_replace,
    window,
    max as sparkmax,
    count,
    countDistinct,
)
from pyspark.sql.types import StringType, TimestampType, IntegerType
import src.caaswx.spark.features as ft
from src.caaswx.spark.base import GroupbyTransformer


class AgentStringParser(Transformer, HasOutputCol):
    """
     A transformer that parses a target Flattened_SM_AGENTNAME column of a
     spark dataframe.
    Input: A Spark dataframe containing Flattened_SM_AGENTNAMESM_AGENTNAME,
    Output: A Spark Dataframe with the following feature appended to it.
     +-------------+----------+----------------------------------+
     | Column_Name | Datatype | Description                      |
     +=============+==========+==================================+
     | self.getOrD |  array   | Contains a list of parsed        |
     | efault("out | <string> | agentnames                       |
     | putCol")    |          |                                  |
     +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(
        self,
    ):
        """
        :param outputCol: Name of parsed agent string column
        :Example:
        >>> from transformers import AgentStringParser
        >>> flattener = AgentStringParser()
        >>> features = flattener.transform(input_dataset)
        """
        super(AgentStringParser, self).__init__()
        self._setDefault(
            outputCol="Parsed_Agent_String",
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
    ):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None,
        outputCol=None,
        thresholds=None, inputCols=None, outputCols=None)
        Sets params for this AgentStringParser.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def http_parser(self, value):

        base = []
        for string in value:
            if len(string.split(" ")) == 1:
                if None not in base:
                    base.append(None)
            else:
                parsed_string = httpagentparser.detect(string)
                if parsed_string not in base:
                    base.append(parsed_string)

        return base

    sch_dict = {}

    def _transform(self, dataset):
        """
        Overridden function which flattens the input dataset w.r.t URLs
        Input : Siteminder dataframe with a column with Flattened
        URLs merged into lists
        Output : Pasrsed URLs merged into lists
        """
        http_parser_udf = udf(self.http_parser, StringType())
        df = dataset.withColumn(
            self.getOrDefault("outputCol"),
            http_parser_udf(col("Flattened_SM_AGENTNAME")),
        ).drop("Flattened_SM_AGENTNAME")
        return df


class CnExtractor(Transformer, HasInputCol, HasOutputCol):
    """
    Creates an Output Column (Default="CN") using the Input Column
    (Default="SM_USERNAME) by:
    - Removing all characters before "cn="
    - Removing the characters after the first comma (including the comma)
    Notes:
    - Assumes the "cn=" and its contents are not at the end of the SM_USERNAME
    - Reminder that dict must change if SM_USERNAME is no longer used
    Input: A Spark dataframe the following column:
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | The username for the user        |
    | Default("   |          | currently logged in with this    |
    | inputCol")  |          | session. SM_USERNAME in          |
    |             |          | raw_logs for reference.          |
    +-------------+----------+----------------------------------+
    Please refer to README.md for further description of raw_logs.
    Output: A Spark Dataframe with the following features calculated:
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Column containing the CommonNames|
    | Default("   |          | for each user. It is an alpha-   |
    | outputCol") |          | numeric string and it may contain|
    |             |          | NULL values.                     |
    +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(self):
        """
        :param setInputCol: Input column to be processed within the
        transformer which must contain "CN" strings like
        "cn=<AN_ALPHANUMERIC_STRING>"
        :param OutputCol: Name of the output
        column to be set after extracting the CN from the SM_USERNAME
        column's comma separated strings
        :type setInputCol: string
        :type OutputCol: string
        :Example:
        >>> from cnextractor import CnExtractor
        >>> cne = CnExtractor(setInputCol="SM_USERNAME", OutputCol="CN")
        >>> datafame_with_CN = cne.transform(input_dataset)
        """
        super(CnExtractor, self).__init__()
        self._setDefault(inputCol="SM_USERNAME", outputCol="CN")
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        Sets params for the CN extractor
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    sch_dict = {
        "SM_USERNAME": ["SM_USERNAME", StringType()],
    }

    def _transform(self, dataset):
        """
        Transform the new CN column
        Params:
        - dataset: dataframe containing SM_USERNAME, to have CN extracted
        Returns:
        - dataset with CN appended
        """
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("inputCol")], r".*(cn=)", ""
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"(,.*)$", ""
            ),
        )

        return dataset


class SMResourceCleaner(Transformer, HasInputCol, HasOutputCol):
    """
    Consolidates SM_RESOURCE elements to simplify redundant data, based
    off of the following criteria:
    1) SAML Requests
      Suggested Categorization: Strings containing the prefix '/cmsws' and
      substrings 'redirect' and 'SAML'. The URLs starting with '/SAMLRequest'.
      Action: Replace with the string '<SAML request>'
    2) Query strings
      Suggested Categorization: Strings containing the character '?' after the
      last occurrence of '/'.
      Action: Replace everything after the relevant '?' by '*'.
    3) URLs ending with '%'
      Strip off the trailing '%'
    4) URLs which start with 'SMASSERTIONREF' are quite long and contain the
    substring '/cmsws/public/saml2sso'.
      To cleanup these long URLs, replace the entire string with
      '/cmsws/public/saml2sso'.
    5) Other strings
      Suggested Categorization: Take whatever is left over from the previous
      two categories that isn't null.
      Action: Do nothing.
    Input: A Spark dataframe containing the following column:
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | This is the resource, for example|
    | Default("   |          | a web page, that the user is     |
    | inputCol")  |          | requesting. SM_RESOURCE in       |
    |             |          | raw_logs for reference.          |
    +-------------+----------+----------------------------------+
    Output: A Spark Dataframe with the following features calculated:
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Column containing the cleaned    |
    | Default("   |          | forms of different URLs with     |
    | outputCol") |          | respect to the aforementioned    |
    |             |          | cleaning strategies.             |
    +-------------+----------+----------------------------------+
    """

    @keyword_only
    def __init__(self):
        """
        :param inputCol: Input column to be processed within the transformer
        :param outputCol: Name of the output column
        :type inputCol: string
        :type outputCol: string
        """
        super(SMResourceCleaner, self).__init__()
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="Cleaned_SM_RESOURCE"
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self):
        """
        Sets params for the SM_RESOURCE Cleaner
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    sch_dict = {"SM_RESOURCE": ["SM_RESOURCE", StringType()]}

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("inputCol")],
                r"((\/cmsws).*((redirect).*(SAML)|(SAML).*(redirect))).*|\/("
                r"SAMLRequest).*",
                "<SAML Request>",
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"\?.*$", "?*"
            ),
        )

        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")], r"\%$", ""
            ),
        )
        dataset = dataset.withColumn(
            self.getOrDefault("outputCol"),
            regexp_replace(
                dataset[self.getOrDefault("outputCol")],
                r".*\%.*(\/cmsws\/public\/saml2sso).*",
                "/cmsws/public/saml2sso",
            ),
        )

        return dataset


class ServerFeatureGenerator(Transformer):
    """
    NOTE: This feature generator has not yet been refactored to use Groupby
    Feature

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
        "Length of the sliding window. " + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    window_step = Param(
        Params._dummy(),
        "window_step",
        "Length of time between start of successive time windows. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    interval_threshold = Param(
        Params._dummy(),
        "interval_threshold",
        "An integer used to define feature NumOfUsersWithEqualIntervalBtnReqs",
        typeConverter=TypeConverters.toInt,
    )

    interval_epsilon = Param(
        Params._dummy(),
        "interval_epsilon",
        "A float used to define feature NumOfUsersWithEqualIntervalBtnReqs",
        typeConverter=TypeConverters.toFloat,
    )

    agg_col = Param(
        Params._dummy(),
        "agg_col",
        "Name of the partition column to pivot off of.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        window_length=900,
        window_step=900,
        interval_threshold=2,
        interval_epsilon=0.2,
        agg_col="SM_USERNAME",
    ):
        """
        :param window_length: Length of the sliding window (in seconds)
        :param window_step: Length of the sliding window's step-size (in
        seconds) :param interval_threshold: An integer used to define
        feature NumOfUsersWithEqualIntervalBtnReqs :param interval_epsilon:
        A float used to define feature NumOfUsersWithEqualIntervalBtnReqs
        :param agg_col: A string used to define the pivot column
        for partitioning the time window :type window_length: long :type
        window_step: long :type interval_threshold: integer :type
        interval_epsilon: float :type agg_col: string

        :Example:
        from serverfeaturegenerator import ServerFeatureGenerator
        feature_generator = ServerFeatureGenerator(window_length = 1800,
            window_step = 1800, interval_threshold = 4, interval_epsilon = 0.3,
             agg_col = "CN")
        features = feature_generator.transform(dataset = input_dataset)
        """
        super(ServerFeatureGenerator, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            interval_threshold=2,
            interval_epsilon=0.2,
            agg_col="SM_USERNAME",
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
        agg_col="SM_USERNAME",
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

    def set_interval_threshold(self, value):
        """
        Sets this ServerFeatureGenerator's interval threshold
        """
        self._set(interval_threshold=value)

    def set_interval_epsilon(self, value):
        """
        Sets this ServerFeatureGenerator's interval epsilon
        """
        self._set(interval_epsilon=value)

    def set_agg_col(self, value):
        """
        Sets this ServerFeatureGenerator's aggregation column
        """
        self._set(agg_col=value)

    def process_data_frame_with_window(self, dataset):
        pivot_entity = str(self.getOrDefault("agg_col"))
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
        "SM_EVENTID": ["SM_EVENTID", IntegerType()],
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
            col("window")["start"].alias("StartTime"),
            col("window")["end"].alias("EndTime"),
            count(
                when(
                    (dataset["SM_EVENTID"] >= 1)
                    & (dataset["SM_EVENTID"] <= 6),
                    True,
                )
            ).alias("VolOfAllLoginAttempts"),
            count(
                when(
                    (dataset["SM_EVENTID"] == 2)
                    | (dataset["SM_EVENTID"] == 6)
                    | (dataset["SM_EVENTID"] == 9)
                    | (dataset["SM_EVENTID"] == 12),
                    True,
                )
            ).alias("VolOfAllFailedLogins"),
            count(
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
                count(
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
            .agg(sparkmax("temp").alias("MaxOfFailedLoginsWithSameIPs"))
        )

        num_of_ips_login_multi_accounts_df = dataset.groupby(
            "SM_CLIENTIP",
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            countDistinct(str(self.getOrDefault("agg_col"))).alias(
                "UNIQUE_USERS_COUNT"
            )
        )

        num_of_ips_login_multi_accounts_df = (
            num_of_ips_login_multi_accounts_df.groupBy("window").agg(
                count(
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
            str(self.getOrDefault("agg_col")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            count(
                when(
                    dataset["SM_CONSECUTIVE_TIME_DIFFERENCE"]
                    >= self.getOrDefault("interval_threshold")
                    - self.getOrDefault("interval_epsilon"),
                    True,
                )
            ).alias("temporary_column")
        )

        num_of_users_with_equal_interval_btn_reqs_df = temp_df.groupBy(
            "window"
        ).agg(
            count(when(temp_df["temporary_column"] != 0, True)).alias(
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


class UserFeatureGenerator(GroupbyTransformer):
    """
    Base Implementation of the UserFeatureGenerator.

    To add a feature implement the feature as subclass of GroupbyFeature and
    include feature in features variable in the constructor and in super
    constructor.
    """

    def __init__(self):
        group_keys = ["CN"]
        features = [
            ft.CountAuthAccept(),
            ft.CountAuthReject(),
            ft.CountAdminAttempt(),
            ft.CountAuthChallenge(),
            ft.CountAZAccept(),
            ft.CountAZReject(),
            ft.CountAdminLogin(),
            ft.CountAdminLogout(),
            ft.CountAdminReject(),
            ft.CountAuthLogout(),
            ft.CountValidateAccept(),
            ft.CountValidateReject(),
            ft.CountVisit(),
            ft.CountFailed(),
            ft.CountOUAms(),
            ft.CountOUCms(),
            ft.CountGet(),
            ft.CountPost(),
            ft.CountHTTPMethod(),
            ft.CountUniqueActions(),
            ft.CountUniqueUsername(),
            ft.CountUniqueEvents(),
            ft.CountUniqueSessions(),
            ft.CountOUIdentity(),
            ft.CountOUCred(),
            ft.CountOUSecurekey(),
            ft.CountPortalMya(),
            ft.CountPortalMyba(),
            ft.CountUniqueOU(),
            ft.UniqueUserOU(),
            ft.UniquePortalRep(),
            ft.CountUniqueRep(),
            ft.UniqueUserApps(),
            ft.CountUniqueUserApps(),
            ft.UniqueSMSessionIds(),
            ft.UniqueSMActions(),
            ft.UniqueSMPortals(),
            ft.UniqueSMTransactions(),
            ft.AvgTimeBtRecords(),
            ft.StdBtRecords(),
            ft.UserNumOfAccountsLoginWithSameIPs(),
            ft.MinUserTimestamp(),
            ft.MaxUserTimestamp(),
            ft.MinTimeBtRecords(),
            ft.MaxTimeBtRecords(),
            ft.CountUniqueResources(),
            ft.CountUniqueIps(),
            ft.CountRecords(),
            ft.UserLoginAttempts(),
            ft.UserNumOfPasswordChange(),
            # ft.UserIsUsingUnusualBrowser(),
        ]
        super(UserFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )


class SessionFeatureGenerator(GroupbyTransformer):
    """
    Base Implementation of the SessionFeatureGenerator.

    To add a feature implement the feature as subclass of GroupbyFeature and
    include feature in features variable in the constructor and in super
    constructor.
    """

    def __init__(self):
        group_keys = ["CN"]
        features = [
            ft.UniqueUserApps(),
            ft.CountUniqueUserApps(),
            ft.UniqueCN(),
            ft.CountAuthReject(),
            ft.CountAdminAttempt(),
            ft.CountAdminLogin(),
            ft.CountAdminLogout(),
            ft.CountVisit(),
            ft.CountFailed(),
            ft.CountGet(),
            ft.CountPost(),
            ft.CountHTTPMethod(),
            ft.CountRecords(),
            ft.CountUniqueActions(),
            ft.CountUniqueEvents(),
            ft.CountUniqueIps(),
            ft.CountUniqueResources(),
            ft.CountUniqueRep(),
            ft.UniqueSMActions(),
            ft.UniqueSMPortals(),
            ft.UniquePortalRep(),
            ft.MinUserTimestamp(),
            ft.MaxUserTimestamp(),
            ft.StdBtRecords(),
        ]
        super(SessionFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )


class IPFeatureGenerator(GroupbyTransformer):
    """
    Base Implementation of the IPFeatureGenerator.

    To add a feature implement the feature as subclass of GroupbyFeature and
    include feature in features variable in the constructor and in super
    constructor.
    """

    def __init__(self):
        group_keys = ["CN"]
        features = [
            ft.UniqueUserApps(),
            ft.AvgTimeBtRecords(),
            ft.MaxTimeBtRecords(),
            ft.MinTimeBtRecords(),
            ft.CountAuthAccept(),
            ft.CountAuthReject(),
            ft.CountAdminAttempt(),
            ft.CountAuthChallenge(),
            ft.CountAZAccept(),
            ft.CountAZReject(),
            ft.CountAdminLogin(),
            ft.CountAdminLogout(),
            ft.CountAdminReject(),
            ft.CountAuthLogout(),
            ft.CountFailed(),
            ft.CountGet(),
            ft.CountPost(),
            ft.CountHTTPMethod(),
            ft.CountOUAms(),
            ft.CountOUCms(),
            ft.CountOUIdentity(),
            ft.CountOUCred(),
            ft.CountOUSecurekey(),
            ft.CountPortalMya(),
            ft.CountPortalMyba(),
            ft.CountUniqueActions(),
            ft.CountUniqueEvents(),
            ft.CountUniqueUsername(),
            ft.CountUniqueResources(),
            ft.CountUniqueSessions(),
            ft.CountUniqueRep(),
            ft.CountRecords(),
            ft.CountVisit(),
            ft.CountValidateAccept(),
            ft.CountValidateReject(),
            ft.UniqueSMActions(),
            ft.UniqueSMSessionIds(),
            ft.UniqueSMPortals(),
            ft.UniqueSMTransactions(),
            ft.UniqueUserOU(),
            ft.UniquePortalRep(),
            ft.MinUserTimestamp(),
            ft.CountUniqueOU(),
        ]
        super(IPFeatureGenerator, self).__init__(
            group_keys=["CN"],
            features=features,
        )
