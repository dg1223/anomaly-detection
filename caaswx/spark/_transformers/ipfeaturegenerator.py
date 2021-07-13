import pandas as pd
import numpy as np

from pyspark.sql.functions import pandas_udf
from pyspark import keyword_only

from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import window, col, pandas_udf, PandasUDFType, max, min
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    StructType,
    StructField,
    DateType,
    FloatType,
)


class IPFeatureGeneratorPandas(Transformer):
    """
      IP feature transformer for the Streamworx project.
      """

    windowLength = Param(
        Params._dummy(),
        "windowLength",
        "Length of the sliding window used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    windowStep = Param(
        Params._dummy(),
        "windowStep",
        "Length of the sliding window step-size used for entity resolution. "
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    entityName = Param(
        Params._dummy(),
        "entityName",
        "Name of the column to perform aggregation on, together with the "
        + "sliding window.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(self, entityName="SM_CLIENTIP", windowLength=900, windowStep=900):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super(IPFeatureGeneratorPandas, self).__init__()
        self._setDefault(windowLength=900, windowStep=900, entityName="SM_CLIENTIP")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, entityName="SM_CLIENTIP", windowLength=900, windowStep=900):
        """
        setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                  inputCols=None, outputCols=None)
        Sets params for this SWXUserFeatureGenerator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setWindowLength(self, value):
        """
        Sets this SWXUserFeatureGenerator's window length.
        """
        self._set(windowLength=value)

    def setWindowStep(self, value):
        """
        Sets this SWXUserFeatureGenerator's window step size.
        """
        self._set(windowStep=value)

    def setEntityName(self, value):
        """
        Sets the entity name
        """
        self._set(entityName=value)

    @staticmethod
    def __generateIPFeatures(self, input_df):
        """
        Generates IP features from the given dataframe of SM_CLIENTIP generateData2
        Input: pandas.DataFrame of IP generateData2
        Output: pandas.DataFrame of IP features
        """

        # Columns to be used extract IP features
        key = input_df["SM_CLIENTIP"][0]
        resources = input_df["SM_RESOURCE"]
        timestamps = input_df["SM_TIMESTAMP"]
        events = input_df["SM_EVENTID"]
        actions = input_df["SM_ACTION"]
        cn = input_df["SM_USERNAME"]
        users = input_df["SM_USERNAME"]
        sessions = input_df["SM_SESSIONID"]
        cra_seq_group = input_df["CRA_SEQ"]
        transactions = input_df["SM_TRANSACTIONID"]

        # --FEATURES---------------------------------------------------------------------------------

        ip_apps = ",".join(
            resources.str.extractall(r"/([a-z]{1,4})/").iloc[:, 0].unique()
        )
        ip_avg_time_bt_records = abs(
            timestamps.diff().dt.total_seconds().fillna(0).mean()
        )
        ip_max_time_bt_records = abs(
            timestamps.diff().dt.total_seconds().fillna(0).max()
        )
        ip_min_time_bt_records = abs(
            timestamps.diff().dt.total_seconds().fillna(0).min()
        )
        ip_count_admin_login = (events == 7).sum()
        ip_count_admin_logout = (events == 8).sum()
        ip_count_admin_reject = (events == 9).sum()

        ip_count_auth_accept = (events == 1).sum()
        ip_count_auth_attempt = (events == 3).sum()
        ip_count_auth_challenge = (events == 4).sum()
        ip_count_auth_logout = (events == 10).sum()
        ip_count_auth_reject = (events == 2).sum()

        ip_count_az_accept = (events == 5).sum()
        ip_count_az_reject = (events == 6).sum()
        ip_count_failed = (
            (events == 6).sum() + (events == 2).sum() + (events == 9).sum()
        )
        ip_count_get = (actions.str.contains("GET")).sum()
        ip_count_post = (actions.str.contains("POST")).sum()
        ip_count_http_methods = (actions.str.contains("GET|POST")).sum()

        ip_count_ou_ams = (cn.str.contains("ams|AMS")).sum()
        ip_count_ou_cms = (cn.str.contains("cms|CMS")).sum()
        ip_count_ou_identity = (cn.str.contains("ou=Identity")).sum()
        ip_count_ou_cred = (cn.str.contains("ou=Credential")).sum()
        ip_count_ou_securekey = (cn.str.contains("ou=SecureKey")).sum()

        ip_count_portal_mya = (resources.str.contains("mima")).sum()
        ip_count_portal_myba = (resources.str.contains("myba")).sum()

        ip_count_unique_action = actions.nunique()
        ip_count_unique_events = events.nunique()
        ip_count_unique_users = users.nunique()
        ip_count_unique_resource = resources.nunique()
        ip_count_unique_sessions = sessions.nunique()

        ip_count_portal_rac = (resources.str.contains("rep")).sum()
        ip_count_records = cra_seq_group.count()
        ip_count_visit = (events == 13).sum()

        ip_count_validate_accept = (events == 11).sum()
        ip_count_validate_reject = (events == 12).sum()

        ip_sm_action = ", ".join(np.sort(actions.unique()))
        ip_sm_usernames = ", ".join(np.sort(users.unique()))
        ip_sm_session_id = ", ".join(np.sort(sessions.unique()))
        ip_sm_portal = ", ".join(np.sort(resources.unique()))
        ip_sm_transaction_id = ", ".join(np.sort(transactions.unique()))

        ip_ou = ",".join(cn.str.extract(r"ou=(.*?),").iloc[:, 0].dropna().unique())
        ip_rep_app = ",".join(
            resources.str.extract(r"(rep.*?)/").iloc[:, 0].dropna().unique()
        )
        ip_timestamp = timestamps.loc[0]

        ip_count_unique_ou = (
            cn.str.extract(r"ou=(.*?),").iloc[:, 0].dropna().unique().size
        )
        ip_count_unique_username = (
            cn.str.extract(r"cn=(.*?),").iloc[:, 0].dropna().unique().size
        )
        ip_count_unique_rep = (
            resources.str.extract(r"rep(.*?)/").iloc[:, 0].dropna().unique().size
        )

        # --END OF FEATURES---------------------------------------------------------------------------------

        # Construct the DataFrame
        data = [
            (key,)
            + (ip_apps,)
            + (ip_avg_time_bt_records,)
            + (ip_max_time_bt_records,)
            + (ip_min_time_bt_records,)
            + (ip_count_admin_login,)
            + (ip_count_admin_logout,)
            + (ip_count_admin_reject,)
            + (ip_count_auth_accept,)
            + (ip_count_auth_attempt,)
            + (ip_count_auth_challenge,)
            + (ip_count_auth_logout,)
            + (ip_count_auth_reject,)
            + (ip_count_az_accept,)
            + (ip_count_az_reject,)
            + (ip_count_failed,)
            + (ip_count_get,)
            + (ip_count_post,)
            + (ip_count_http_methods,)
            + (ip_count_ou_ams,)
            + (ip_count_ou_cms,)
            + (ip_count_ou_identity,)
            + (ip_count_ou_cred,)
            + (ip_count_ou_securekey,)
            + (ip_count_portal_mya,)
            + (ip_count_portal_myba,)
            + (ip_count_unique_action,)
            + (ip_count_unique_events,)
            + (ip_count_unique_users,)
            + (ip_count_unique_resource,)
            + (ip_count_unique_sessions,)
            + (ip_count_portal_rac,)
            + (ip_count_records,)
            + (ip_count_visit,)
            + (ip_count_validate_accept,)
            + (ip_count_validate_reject,)
            + (ip_sm_action,)
            + (ip_sm_usernames,)
            + (ip_sm_session_id,)
            + (ip_sm_portal,)
            + (ip_sm_transaction_id,)
            + (ip_ou,)
            + (ip_rep_app,)
            + (ip_timestamp,)
            + (ip_count_unique_ou,)
            + (ip_count_unique_username,)
            + (ip_count_unique_rep,)
        ]

        ans = pd.DataFrame(
            data,
            columns=[
                "SM_CLIENTIP",
                "IP_APP",
                "IP_AVG_TIME_BT_RECORDS",
                "IP_MAX_TIME_BT_RECORDS",
                "IP_MIN_TIME_BT_RECORDS",
                "IP_COUNT_ADMIN_LOGIN",
                "IP_COUNT_ADMIN_LOGOUT",
                "IP_COUNT_ADMIN_REJECT",
                "IP_COUNT_AUTH_ACCEPT",
                "IP_COUNT_AUTH_ATTEMPT",
                "IP_COUNT_AUTH_CHALLENGE",
                "IP_COUNT_AUTH_LOGOUT",
                "IP_COUNT_AUTH_REJECT",
                "IP_COUNT_AZ_ACCEPT",
                "IP_COUNT_AZ_REJECT",
                "IP_COUNT_FAILED",
                "IP_COUNT_GET",
                "IP_COUNT_POST",
                "IP_COUNT_HTTP_METHODS",
                "IP_COUNT_OU_AMS",
                "IP_COUNT_OU_CMS",
                "IP_COUNT_OU_IDENTITY",
                "IP_COUNT_OU_CRED",
                "IP_COUNT_OU_SECUREKEY",
                "IP_COUNT_PORTAL_MYA",
                "IP_COUNT_PORTAL_MYBA",
                "IP_COUNT_UNIQUE_ACTION",
                "IP_COUNT_UNIQUE_EVENTS",
                "IP_COUNT_UNIQUE_USERS",
                "IP_COUNT_UNIQUE_RESOURCE",
                "IP_COUNT_UNIQUE_SESSIONS",
                "IP_COUNT_PORTAL_RAC",
                "IP_COUNT_RECORDS",
                "IP_COUNT_VISIT",
                "IP_COUNT_VALIDATE_ACCEPT",
                "IP_COUNT_VALIDATE_REJECT",
                "IP_SM_ACTION",
                "IP_SM_USERNAMES",
                "IP_SM_SESSIONID",
                "IP_SM_PORTAL",
                "IP_SM_TRANSACTIONID",
                "IP_OU",
                "IP_REP_APP",
                "IP_TIMESTAMP",
                "IP_COUNT_UNIQUE_OU",
                "IP_COUNT_UNIQUE_USERNAME",
                "IP_COUNT_UNIQUE_REP",
            ],
        )

        return ans

    def _transform(self, dataset):
        """
        Transforms the given dataset by deriving IP features and returning a new dataframe of those features
        """
        return dataset.groupby(
            str(self.getOrDefault("entityName")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("windowLength")) + " seconds",
                str(self.getOrDefault("windowStep")) + " seconds",
            ),
        ).applyInPandas(
            self.__generateIPFeatures,
            schema="SM_CLIENTIP String, IP_APP String, IP_AVG_TIME_BT_RECORDS float, IP_MAX_TIME_BT_RECORDS float, IP_MIN_TIME_BT_RECORDS float, IP_COUNT_ADMIN_LOGIN long, IP_COUNT_ADMIN_LOGOUT long, IP_COUNT_ADMIN_REJECT long, IP_COUNT_AUTH_ACCEPT long, IP_COUNT_AUTH_ATTEMPT long, IP_COUNT_AUTH_CHALLENGE long, IP_COUNT_AUTH_LOGOUT long, IP_COUNT_AUTH_REJECT long, IP_COUNT_AZ_ACCEPT long, IP_COUNT_AZ_REJECT long, IP_COUNT_FAILED long, IP_COUNT_GET long, IP_COUNT_POST long, IP_COUNT_HTTP_METHODS long, IP_COUNT_OU_AMS long, IP_COUNT_OU_CMS long, IP_COUNT_OU_IDENTITY long, IP_COUNT_OU_CRED long, IP_COUNT_OU_SECUREKEY long, IP_COUNT_PORTAL_MYA long, IP_COUNT_PORTAL_MYBA long, IP_COUNT_UNIQUE_ACTION long, IP_COUNT_UNIQUE_EVENTS long, IP_COUNT_UNIQUE_USERS long, IP_COUNT_UNIQUE_RESOURCE long, IP_COUNT_UNIQUE_SESSIONS long, IP_COUNT_PORTAL_RAC long, IP_COUNT_RECORDS long, IP_COUNT_VISIT long, IP_COUNT_VALIDATE_ACCEPT long, IP_COUNT_VALIDATE_REJECT long, IP_SM_ACTION String, IP_SM_USERNAMES String, IP_SM_SESSIONID String, IP_SM_PORTAL String, IP_SM_TRANSACTIONID String, IP_OU String, IP_REP_APP String, IP_TIMESTAMP timestamp, IP_COUNT_UNIQUE_OU long, IP_COUNT_UNIQUE_USERNAME long, IP_COUNT_UNIQUE_REP long",
        )
