import httpagentparser
import pyspark.sql.functions as f
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params, HasOutputCol
from pyspark.sql.functions import (
    window,
    col,
    udf,
)
from pyspark.sql.types import (
    StringType,
    TimestampType,
)

from .sparknativetransformer import SparkNativeTransformer


class AgentStringFlattener(SparkNativeTransformer, HasOutputCol):
    """
     A transformer that flattens and cleans a target column (SM_AGENTNAME)
     of a spark dataframe.

     Input: A Spark dataframe containing SM_AGENTNAME,
     SM_TIMESTAMP, and SM_CLIENTIP (from raw_logs), and the following column.
     +-------------+----------+----------------------------------+
     | Column_Name | Datatype | Description                      |
     +=============+==========+==================================+
     | self.getOr  | string   | Pivot Column containing the      |
     | Default("   |          | CommonNames for each user. It is |
     | agg_col")   |          | an alpha-numeric string and it   |
     |             |          | may contain  NULL values.        |
     +-------------+----------+----------------------------------+
     Please refer to README.md for further description of raw_logs.

    Output: A Spark Dataframe with the following features calculated on rows
     aggregated by time window and agg_col, where the window is calculated
     using:
         - length: how many seconds the window is
         - step: the length of time between the start of successive time window
     +-------------+----------+----------------------------------+
     | Column_Name | Datatype | Description                      |
     +=============+==========+==================================+
     | SM_AGENTNAME|  array   | Contains a list of flattened     |
     |             | <string> | and/or cleaned agentnames        |
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
        "Length of time between start of successive time windows."
        + "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt,
    )

    agg_col = Param(
        Params._dummy(),
        "agg_col",
        "Name of the column to perform aggregation on.",
        typeConverter=TypeConverters.toString,
    )

    agent_string_col = Param(
        Params._dummy(),
        "agent_string_col",
        "Name of the column that contains the agent string.",
        typeConverter=TypeConverters.toString,
    )

    agent_size_limit = Param(
        Params._dummy(),
        "agent_size_limit",
        "Number of agent strings processed."
        + "Given as the number of strings.",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(
        self,
        agg_col="SM_USERNAME",
        agent_string_col="SM_AGENTNAME",
        agent_size_limit=5,
        window_length=900,
        window_step=900,
    ):
        """
        :param agg_col: Column to be grouped by when cleaning the
        SM_AGENTNAME column along with the window column
        :param agent_size_limit: Defines a limit on number of agent strings
        in the output column  :param window_length: Sets this
        AgentStringFlattener.'s window length. :param window_step: Sets this
        AgentStringFlattener's window step. :type agg_col: string :type
        agent_size_limit: long :type window_length:
        long :type window_step: long
        :Example:
        >>> from agentstringflattener import AgentStringFlattener
        >>> flattener = AgentStringFlattener(
                window_length = 1800, window_step = 1800)
        >>> features = flattener.transform(input_dataset)
        """
        super(AgentStringFlattener, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            agg_col="SM_USERNAME",
            agent_string_col="SM_AGENTNAME",
            outputCol="Parsed_Agent_String",
            agent_size_limit=5,
        )
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
        self,
        agg_col="SM_USERNAME",
        agent_string_col="SM_AGENTNAME",
        agent_size_limit=5,
        window_length=900,
        window_step=900,
    ):
        """
        set_params(self, \\*, threshold=0.0, inputCol=None,

        outputCol=None,

        thresholds=None, inputCols=None, outputCols=None)
        Sets params for this AgentStringFlattener.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_agg_col(self, value):
        """
        Sets the aggregation column
        """
        self._set(agg_col=value)

    def get_agg_col(self):
        return self.agg_col

    def set_agent_string_col(self, value):
        """
        Sets the agent string column
        """
        self._set(agent_string_col=value)

    def get_agent_string_col(self):
        return self.agent_string_col

    def set_agent_size_limit(self, value):
        """
        Sets the Agent Size
        """
        self._set(agent_size_limit=value)

    def get_agent_size_limit(self):
        return self.agent_size_limit

    def set_window_length(self, value):
        """
        Sets this AgentStringFlattener's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this AgentStringFlattener's window step size.
        """
        self._set(window_step=value)

    @staticmethod
    def __flatten(self, df):
        """
        Flattens the URLs based on the order of their occurring timestamps
        Input: Siteminder dataframe
        Output: Dataframe containing aggregation column, window intervals and
        flattened URLs combined into lists
        """

        # Sorting the dataframe w.r.t timestamps in ascending order
        df = df.sort("SM_TIMESTAMP")

        # Applying flattening operation over SM_AGENTNAME by combining them
        # into set for each SM_USERNAME.
        result = df.groupby(
            str(self.getOrDefault("agg_col")),
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            ),
        ).agg(
            f.collect_set(self.getOrDefault("agent_string_col")).alias(
                self.getOrDefault("agent_string_col")
            )
        )

        result = result.sort("window")
        # Slicing to only get N User Agent Strings.
        result = result.withColumn(
            self.getOrDefault("agent_string_col"),
            f.slice(
                result[self.getOrDefault("agent_string_col")],
                1,
                self.getOrDefault("agent_size_limit"),
            ),
        )

        return result

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

    sch_dict = {
        "SM_TIMESTAMP": ["SM_TIMESTAMP", TimestampType()],
    }

    def _transform(self, dataset):
        """
        Overridden function which flattens the input dataset w.r.t URLs
        Input : Siteminder dataframe
        Output : SM_USERNAMEs with flattened URLs merged into lists
        """

        result = self.__flatten(self, dataset)

        http_parser_udf = udf(self.http_parser, StringType())
        df = result.withColumn(
            self.getOrDefault("outputCol"),
            http_parser_udf(col(self.getOrDefault("agent_string_col"))),
        ).drop(self.getOrDefault("agent_string_col"))
        return df
