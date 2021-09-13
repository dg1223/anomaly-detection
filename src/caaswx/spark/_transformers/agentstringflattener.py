import httpagentparser
import pyspark.sql.functions as f
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.functions import (
    window,
    col,
    udf,
)
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    StructType,
    StructField,
    DateType,
    FloatType,
    IntegerType,
    ArrayType,
)
from src.caaswx.spark._transformers.sparknativetransformer import SparkNativeTransformer


class AgentStringFlattener(SparkNativeTransformer):
    """
    A module to flatten and clean the SM_AGENTNAME column of the Siteminder dataset.
    Input: A Spark dataframe
    Columns from raw_logs: SM_AGENTNAME, SM_TIMESTAMP, SM_CLIENTIP.
    Please refer to README.md for description.
    List of other required columns:
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | self.getOr  | string   | Pivot Column containing the      |
    | Default("en |          | CommonNames for each user. It is |
    | tityName")  |          | an alpha-numeric string and it   |
    |             |          | may contain  NULL values.        |
    +-------------+----------+----------------------------------+

    Output: Input dataframe with an additional column containing the flattended and cleaned agentnames
    +-------------+----------+----------------------------------+
    | Column_Name | Datatype | Description                      |
    +=============+==========+==================================+
    | SM_AGENTNAME|  array   | Contains a list of flattened     |
    |             | <string> | and/or cleaned agentnames        |
    +-------------+----------+----------------------------------+
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

    agentSizeLimit = Param(
        Params._dummy(),
        "agentSizeLimit",
        "Number of agent strings processed " + "Given as the number of strings.",
        typeConverter=TypeConverters.toInt,
    )

    runParser = Param(
        Params._dummy(),
        "runParser",
        "Choose to parse parquet_data." + "Given as the boolean.",
        typeConverter=TypeConverters.toBoolean,
    )

    @keyword_only
    def __init__(
        self,
        entityName="SM_USERNAME",
        agentSizeLimit=5,
        runParser=False,
        windowLength=900,
        windowStep=900,
    ):
        """
        :param entity_name: Column to be grouped by when cleaning the SM_AGENTNAME column along with the window column
        :param agentSizeLimit: Defines a limit on number of agent strings in the output column
        :param runParser: When False, it will only flatten the agent strings. When True, it will flatten the SM_AGENTNAME string along with cleaning the browser section of SM_AGENTNAME throufh the httpagentparser library.
        :param window_length: Sets this AgentStringFlattener.'s window length.
        :param window_step: Sets this AgentStringFlattener's window step.
        :type entity_name: string
        :type agentSizeLimit: long
        :type runParser: boolean
        :type window_length: long
        :type window_step: long
        :Example:
        >>> from agentstringflattener import AgentStringFlattener
        >>> flattener = AgentStringFlattener(window_length = 1800, window_step = 1800)
        >>> features = flattener.transform(input_dataset)
        """
        super(AgentStringFlattener, self).__init__()
        self._setDefault(
            windowLength=900,
            windowStep=900,
            entityName="SM_USERNAME",
            agentSizeLimit=5,
            runParser=False,
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        entityName="SM_USERNAME",
        agentSizeLimit=5,
        windowLength=900,
        windowStep=900,
        runParser=False,
    ):
        """
    set_params(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
              inputCols=None, outputCols=None)
    Sets params for this AgentStringFlattener.
    """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setEntityName(self, value):
        """
        Sets the Entity Name
        """
        self._set(entityName=value)

    def getEntityName(self):
        return self.entityName

    def setRunParser(self, value):
        """
        Sets the Entity Name
        """
        self._set(runParser=value)

    def getRunParser(self):
        return self.runParser

    def setAgentSizeLimit(self, value):
        """
        Sets the Agent Size
        """
        self._set(agentSizeLimit=value)

    def getAgentSizeLimit(self):
        return self.agentSizeLimit

    def setWindowLength(self, value):
        """
        Sets this AgentStringFlattener's window length.
        """
        self._set(windowLength=value)

    def setWindowStep(self, value):
        """
        Sets this AgentStringFlattener's window step size.
        """
        self._set(windowStep=value)

    @staticmethod
    def __flatten(self, df):
        """
        Flattens the URLs based on the order of their occurring timestamps
        Input: Siteminder dataframe
        Output: Dataframe containing entity name, window intervals and flattened URLs combined into lists
        """

        # Sorting the dataframe w.r.t timestamps in ascending order
        df = df.sort("SM_TIMESTAMP")

        # Applying flattening operation over SM_AGENTNAME by combining them into set for each SM_USERNAME.
        result = df.groupby(
            str(self.getOrDefault("entityName")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("windowLength")) + " seconds",
                str(self.getOrDefault("windowStep")) + " seconds",
            ),
        ).agg(f.collect_set("SM_AGENTNAME").alias("SM_AGENTNAME"))

        result = result.sort("window")
        # Slicing to only get N User Agent Strings.
        result = result.withColumn(
            "SM_AGENTNAME",
            f.slice(result["SM_AGENTNAME"], 1, self.getOrDefault("agentSizeLimit")),
        )

        return result

    def httpParser(self, value):

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

    sch_dict = {
        "SM_CLIENTIP": ["SM_CLIENTIP", StringType()],
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
        "SM_AGENTNAME": ["SM_AGENTNAME", StringType()],
    }

    def _transform(self, dataset):
        """
        Overridden function which flattens the input dataset w.r.t URLs
        Input : Siteminder dataframe
        Output : SM_USERNAMEs with flattened URLs merged into lists
        """

        result = self.__flatten(self, dataset)
        if self.getOrDefault("runParser"):
            httpParserUdf = udf(self.httpParser, StringType())
            df = result.withColumn(
                "Parsed_Agent_String", httpParserUdf(col("SM_AGENTNAME"))
            ).drop("SM_AGENTNAME")
            return df
        else:
            return result
