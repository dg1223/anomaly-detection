import httpagentparser

import pyspark.sql.functions as f

from pyspark import keyword_only
from pyspark.ml import Transformer, UnaryTransformer
from pyspark.ml.param.shared import TypeConverters, Param, Params

from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import window, col, pandas_udf, PandasUDFType, max, min, udf, lit
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType, StructType, StructField, DateType, FloatType, MapType


class UserAgentFlattenerParser(Transformer):
  """
  User Feature transformer for the Streamworx project.
  """

  windowLength = Param(
    Params._dummy(),
    "windowLength",
    "Length of the sliding window used for entity resolution. " +
    "Given as an integer in seconds.",
    typeConverter=TypeConverters.toInt
  )

  windowStep = Param(
    Params._dummy(),
    "windowStep",
    "Length of the sliding window step-size used for entity resolution. " +
    "Given as an integer in seconds.",
    typeConverter=TypeConverters.toInt
  )

  entityName = Param(
    Params._dummy(),
    "entityName",
    "Name of the column to perform aggregation on, together with the " +
    "sliding window.",
    typeConverter=TypeConverters.toString
  )

  agentSizeLimit = Param(
    Params._dummy(),
    "agentSizeLimit",
    "Number of agent strings processed " +
    "Given as the number of strings.",
    typeConverter=TypeConverters.toInt
  )

  runParser = Param(
    Params._dummy(),
    "runParser",
    "Choose to parse data." +
    "Given as the boolean.",
    typeConverter=TypeConverters.toBoolean
  )

  @keyword_only
  def __init__(self, entityName='SM_USERNAME', agentSizeLimit=5, runParser=False, windowLength=900, windowStep=900):
    """
    def __init__(self, *, window_length = 900, window_step = 900)
    """
    super(UserAgentFlattenerParser, self).__init__()
    self._setDefault(windowLength=900, windowStep=900, entityName='SM_USERNAME', agentSizeLimit=5, runParser=False)
    kwargs = self._input_kwargs
    self.setParams(**kwargs)

  @keyword_only
  def setParams(self, entityName='SM_USERNAME', agentSizeLimit=5, windowLength=900, windowStep=900, runParser=False):
    """
    setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
              inputCols=None, outputCols=None)
    Sets params for this UserAgentFlattenerParser.
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
    Sets this UserAgentFlattenerParser's window length.
    """
    self._set(windowLength=value)

  def setWindowStep(self, value):
    """
    Sets this UserAgentFlattenerParser's window step size.
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
    df = df.sort('SM_TIMESTAMP')

    # Applying flattening operation over SM_AGENTNAME by combining them into set for each SM_USERNAME.
    result = (
      df.groupby(
        str(self.getOrDefault('entityName')),
        window(
          'SM_TIMESTAMP',
          str(self.getOrDefault('windowLength')) + ' seconds', str(self.getOrDefault('windowStep')) + ' seconds'))
        .agg(f.collect_set('SM_AGENTNAME').alias('SM_AGENTNAME')))

    result = result.sort('window')
    # Slicing to only get N User Agent Strings.
    result = result.withColumn('SM_AGENTNAME', f.slice(result['SM_AGENTNAME'], 1, self.getOrDefault('agentSizeLimit')))

    return result

  def httpParser(self, value):

    base = []
    for string in value:
      if (len(string.split(' ')) == 1):
        if None not in base:
          base.append(None)
      else:
        parsed_string = httpagentparser.detect(string)
        if parsed_string not in base:
          base.append(parsed_string)

    return base

  def _transform(self, dataset):
    """
    Overridden function which flattens the input dataset w.r.t URLs
    Input : Siteminder dataframe
    Output : SM_USERNAMEs with flattened URLs merged into lists
    """

    result = (self.__flatten(self, dataset))
    if (self.getOrDefault('runParser') == True):
      httpParser_udf = udf(self.httpParser, StringType())
      df = result.withColumn("Parsed_Agent_String", httpParser_udf(col("SM_AGENTNAME"))).drop('SM_AGENTNAME')
      return df
    else:
      return result
