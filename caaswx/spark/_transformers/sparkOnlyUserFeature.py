# Import essential packages
from pyspark import keyword_only
from pyspark.ml import Transformer, UnaryTransformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import window
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, when, lag, isnull
from pyspark.sql.types import StringType

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local")
spark = SparkSession(sc)

# CN Extractor: When you execute it please set the outputcol to "SM_CN"
# Otherwise, usertransformer can not be executed


class CNextractor(UnaryTransformer):
    """
    CN extractor: It is based on UnaryTransformerExtract CN features from SM_USERNAME
    """

    def createTransformFunc(self):
        return lambda x: x if "cn=" not in x else x[x.index("cn=") + 3 : x.index(",")]

    def outputDataType(self):
        return StringType()

    def validateInputType(self, inputType):
        assert inputType == StringType(), f"Expected StringType() and found {inputType}"


def createTransformFunc(self):
    return lambda x: x if "cn=" not in x else x[x.index("cn=") + 3 : x.index(",")]


def outputDataType(self):
    return StringType()


def validateInputType(self, inputType):
    assert inputType == StringType(), f"Expected StringType() and found {inputType}"


# Please uncomment next 2 line to generate SM_CN feature siteminder_df is the dataframe name


# Feature generator based on Users (SM_CN)
# Execute CNextractor before this transformer
# Otherwise, we have no SM_CN feature


class UserFeatureGenerator(Transformer):
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
    def __init__(self, entityName="SM_CN", windowLength=900, windowStep=900):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super(UserFeatureGenerator, self).__init__()
        self._setDefault(entityName="SM_CN", windowLength=900, windowStep=900)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, entityName="SM_CN", windowLength=900, windowStep=900):
        """
        setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                  inputCols=None, outputCols=None)
        Sets params for this UserFeatureGenerator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setWindowLength(self, value):
        """
        Sets this UserFeatureGenerator's window length.
        """
        self._set(windowLength=value)

    def setEntityName(self, value):
        """
        Sets this UserFeatureGenerator's entity name.
        """
        self._set(entityName=value)

    def setWindowStep(self, value):
        """
        Sets this UserFeatureGenerator's window step size.
        """
        self._set(windowStep=value)

    def process_DataFrame_with_Window(self, dataset):
        ts_window = Window.partitionBy("SM_CN").orderBy("SM_TIMESTAMP")

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

    def _transform(self, dataset):
        dataset = self.process_DataFrame_with_Window(dataset)

        return dataset.groupby(
            str(self.getOrDefault("entityName")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("windowLength")) + " seconds",
                str(self.getOrDefault("windowStep")) + " seconds",
            ),
        ).agg(
            F.count(when(col("SM_EVENTID") == 7, True)).alias("COUNT_ADMIN_LOGIN"),
            F.count(when(col("SM_EVENTID") == 8, True)).alias("COUNT_ADMIN_LOGOUT"),
            F.count(when(col("SM_EVENTID") == 1, True)).alias("COUNT_AUTH_ACCEPT"),
            F.count(when(col("SM_EVENTID") == 3, True)).alias("COUNT_ADMIN_ATTEMPT"),
            F.count(when(col("SM_EVENTID") == 2, True)).alias("COUNT_ADMIN_REJECT"),
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
                    | (col("SM_EVENTID").contains("AMS")),
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
            F.countDistinct(col("SM_CN")).alias("COUNT_UNIQUE_USERNAME"),
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
            F.countDistinct(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0)).alias(
                "COUNT_UNIQUE_OU"
            ),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_USERNAME", r"ou=(.*?),", 0))
            ).alias("UNIQUE_USER_OU"),
            F.count(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0)).alias(
                "COUNT_PORTAL_RAC"
            ),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"(rep.*?)/", 0))
            ).alias("UNIQUE_PORTAL_RAC"),
            F.array_distinct(
                F.collect_list(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0))
            ).alias("UNIQUE_USER_APPS"),
            F.countDistinct(regexp_extract("SM_RESOURCE", r"/(.*?)/", 0)).alias(
                "COUNTUNIQUE_USER_APPS"
            ),
            F.min(col("SM_TIMESTAMP")).alias("USER_TIMESTAMP"),
            F.max("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("MAX_TIME_BT_RECORDS"),
            F.min("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("MIN_TIME_BT_RECORDS"),
            F.mean("SM_CONSECUTIVE_TIME_DIFFERENCE").alias("AVG_TIME_BT_RECORDS"),
        )


# spark.catalog.setCurrentDatabase("siteminder")
# siteminder_df = spark.table("raw_logs")
# cns = CNextractor().setInputCol("SM_USERNAME").setOutputCol("SM_CN")
# siteminder_df = cns.transform(siteminder_df)
#
# fg = UserFeatureGenerator()
# result = fg.transform(siteminder_df)
# result.show(5)