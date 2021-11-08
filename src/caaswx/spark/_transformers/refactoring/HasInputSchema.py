from src.caaswx.spark.utilities.schema_utils import *
import pyspark
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params


class HasInputSchema(Transformer):
    input_schema = Param(
        Params._dummy(),
        "input_schema",
        "Param specifying the required schema of the input dataframe.",
    )

    def __init__(self):
        super(HasInputSchema, self).__init__()

    @keyword_only
    def __init__(self, *, input_schema=None):
        super(HasInputSchema, self).__init__()
        self._setDefault(input_schema=None)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, *, input_schema=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def schema_is_admissable(
        self, schema: pyspark.sql.types.StructType, compare_nulls=False
    ):
        return schema_is_subset(
            self.input_schema, schema, compare_nulls=compare_nulls
        )

    def set_input_schema(self, schema: pyspark.sql.types.StructType):
        self.input_schema = schema

    def get_input_schema(self):
        return self.getOrDefault("input_schema")
