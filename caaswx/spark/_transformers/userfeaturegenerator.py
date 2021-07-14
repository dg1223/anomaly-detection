from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import window, pandas_udf, PandasUDFType
from pyspark.sql.types import (
    LongType,
    StringType,
    TimestampType,
    FloatType,
)


class SWXUserFeatureGenerator(Transformer):
    """
    Feature transformer for the swx project.
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
    def __init__(self, entityName, windowLength=900, windowStep=900):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super(SWXUserFeatureGenerator, self).__init__()
        self._setDefault(windowLength=900, windowStep=900)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, entityName, windowLength=900, windowStep=900):
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

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_APP(resources) -> StringType():
        ans = ",".join(resources.str.extractall(r"/([a-z]{1,4})/").iloc[:, 0].unique())
        return ans

    @staticmethod
    @pandas_udf("string", PandasUDFType.GROUPED_AGG)
    def get_CN(users) -> StringType():
        return users.where(
            users.str.contains("cn") is False,
            users.str[users[0].find("=") + 1 : users[0].find(",")],
        )[0]

    @staticmethod
    @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
    def get_USER_AVG_TIME_BT_RECORDS(timestamps) -> FloatType():
        return abs(timestamps.diff().dt.total_seconds().fillna(0).mean())

    @staticmethod
    @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
    def get_USER_MAX_TIME_BT_RECORDS(timestamps) -> FloatType():
        return abs(timestamps.diff().dt.total_seconds().fillna(0).max())

    @staticmethod
    @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
    def get_USER_MIN_TIME_BT_RECORDS(timestamps) -> FloatType():
        return abs(timestamps.diff().dt.total_seconds().fillna(0).min())

    @staticmethod
    @pandas_udf(LongType(), PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_ADMIN_LOGIN(events) -> LongType():
        return (events == 7).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_ADMIN_LOGOUT(events) -> LongType():
        return (events == 8).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_ADMIN_REJECT(events) -> LongType():
        return (events == 9).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AUTH_ACCEPT(events) -> LongType():
        return (events == 1).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AUTH_ATTEMPT(events) -> LongType():
        return (events == 3).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AUTH_CHALLENGE(events) -> LongType():
        return (events == 4).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AUTH_LOGOUT(events) -> LongType():
        return (events == 10).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AUTH_REJECT(events) -> LongType():
        return (events == 2).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AZ_ACCEPT(events) -> LongType():
        return (events == 5).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_AZ_REJECT(events) -> LongType():
        return (events == 6).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_FAILED(events) -> LongType():
        return (events == 6).sum() + (events == 2).sum() + (events == 9).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_GET(actions) -> LongType():
        return (actions.str.contains("GET")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_POST(actions) -> LongType():
        return (actions.str.contains("POST")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_HTTP_METHODS(actions) -> LongType():
        return (actions.str.contains("GET|POST")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_OU_AMS(cn) -> LongType():
        return (cn.str.contains("ams|AMS")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_OU_CMS(cn) -> LongType():
        return (cn.str.contains("cms|CMS")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_OU_IDENTITY(cn) -> LongType():
        return (cn.str.contains("ou=Identity")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_OU_CRED(cn) -> LongType():
        return (cn.str.contains("ou=Credential")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_OU_SECUREKEY(cn) -> LongType():
        return (cn.str.contains("ou=SecureKey")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_PORTAL_MYA(resource) -> LongType():
        return (resource.str.contains("mima")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_PORTAL_MYBA(resource) -> LongType():
        return (resource.str.contains("myba")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_ACTION(actions) -> LongType():
        return actions.nunique()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_EVENTS(events) -> LongType():
        return events.nunique()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_IP(client_ip_group) -> LongType():
        return client_ip_group.nunique()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_RESOURCE(resources) -> LongType():
        return resources.nunique()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_SESSIONS(session_id_group) -> LongType():
        return session_id_group.nunique()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_PORTAL_RAC(resource) -> LongType():
        return (resource.str.contains("rep")).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_RECORDS(cra_seq_group) -> LongType():
        return cra_seq_group.count()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_VISIT(events) -> LongType():
        return (events == 13).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_VALIDATE_ACCEPT(events) -> LongType():
        return (events == 11).sum()

    @staticmethod
    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_VALIDATE_REJECT(events) -> LongType():
        return (events == 12).sum()

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_SM_ACTION(actions) -> StringType():
        return ", ".join(actions.unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_SM_CLIENTIP(client_ip_group) -> StringType():
        return ", ".join(client_ip_group.unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_SM_SESSIONID(session_id_group) -> StringType():
        return ", ".join(session_id_group.unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_SM_PORTAL(resource_portals) -> StringType():
        return ", ".join(resource_portals.unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_SM_TRANSACTIONID(transaction_id_group) -> StringType():
        return ", ".join(transaction_id_group.unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_OU(cn) -> StringType():
        return ",".join(cn.str.extract(r"ou=(.*?),").iloc[:, 0].dropna().unique())

    @staticmethod
    @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
    def get_USER_REP_APP(resources) -> StringType():
        return ",".join(
            resources.str.extract(r"(rep.*?)/").iloc[:, 0].dropna().unique()
        )

    @staticmethod
    @pandas_udf(TimestampType(), PandasUDFType.GROUPED_AGG)
    def get_USER_TIMESTAMP(timestamps) -> StringType():
        return timestamps.loc[0]

    @staticmethod
    @pandas_udf(LongType(), PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_OU(cn) -> StringType():
        return cn.str.extract(r"ou=(.*?),").iloc[:, 0].dropna().unique().size

    @staticmethod
    @pandas_udf(LongType(), PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_USERNAME(cn) -> StringType():
        return cn.str.extract(r"cn=(.*?),").iloc[:, 0].dropna().unique().size

    @staticmethod
    @pandas_udf(LongType(), PandasUDFType.GROUPED_AGG)
    def get_USER_COUNT_UNIQUE_REP(resources) -> StringType():
        return resources.str.extract(r"rep(.*?)/").iloc[:, 0].dropna().unique().size

    def _transform(self, dataset):
        return dataset.groupby(
            str(self.getOrDefault("entityName")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("windowLength")) + " seconds",
                str(self.getOrDefault("windowStep")) + " seconds",
            ),
        ).agg(
            self.get_USER_APP(dataset["SM_RESOURCE"]),
            self.get_CN(dataset["SM_USERNAME"]),
            self.get_USER_AVG_TIME_BT_RECORDS(dataset["SM_TIMESTAMP"]),
            self.get_USER_MAX_TIME_BT_RECORDS(dataset["SM_TIMESTAMP"]),
            self.get_USER_MIN_TIME_BT_RECORDS(dataset["SM_TIMESTAMP"]),
            self.get_USER_COUNT_ADMIN_LOGIN(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_ADMIN_LOGOUT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_ADMIN_REJECT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AUTH_ACCEPT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AUTH_ATTEMPT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AUTH_CHALLENGE(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AUTH_LOGOUT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AUTH_REJECT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AZ_ACCEPT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_AZ_REJECT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_FAILED(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_GET(dataset["SM_ACTION"]),
            self.get_USER_COUNT_POST(dataset["SM_ACTION"]),
            self.get_USER_COUNT_HTTP_METHODS(dataset["SM_ACTION"]),
            self.get_USER_COUNT_OU_AMS(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_OU_CMS(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_OU_IDENTITY(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_OU_CRED(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_OU_SECUREKEY(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_PORTAL_MYA(dataset["SM_RESOURCE"]),
            self.get_USER_COUNT_PORTAL_MYBA(dataset["SM_RESOURCE"]),
            self.get_USER_COUNT_UNIQUE_ACTION(dataset["SM_ACTION"]),
            self.get_USER_COUNT_UNIQUE_EVENTS(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_UNIQUE_IP(dataset["SM_CLIENTIP"]),
            self.get_USER_COUNT_UNIQUE_RESOURCE(dataset["SM_RESOURCE"]),
            self.get_USER_COUNT_UNIQUE_SESSIONS(dataset["SM_SESSIONID"]),
            self.get_USER_COUNT_PORTAL_RAC(dataset["SM_RESOURCE"]),
            self.get_USER_COUNT_RECORDS(dataset["CRA_SEQ"]),
            self.get_USER_COUNT_VISIT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_VALIDATE_ACCEPT(dataset["SM_EVENTID"]),
            self.get_USER_COUNT_VALIDATE_REJECT(dataset["SM_EVENTID"]),
            self.get_USER_SM_ACTION(dataset["SM_ACTION"]),
            self.get_USER_SM_CLIENTIP(dataset["SM_CLIENTIP"]),
            self.get_USER_SM_SESSIONID(dataset["SM_SESSIONID"]),
            self.get_USER_SM_PORTAL(dataset["SM_RESOURCE"]),
            self.get_USER_SM_TRANSACTIONID(dataset["SM_TRANSACTIONID"]),
            self.get_USER_OU(dataset["SM_USERNAME"]),
            self.get_USER_REP_APP(dataset["SM_RESOURCE"]),
            self.get_USER_TIMESTAMP(dataset["SM_TIMESTAMP"]),
            self.get_USER_COUNT_UNIQUE_OU(dataset["SM_USERNAME"]),
            self.get_USER_COUNT_UNIQUE_REP(dataset["SM_RESOURCE"]),
        )
