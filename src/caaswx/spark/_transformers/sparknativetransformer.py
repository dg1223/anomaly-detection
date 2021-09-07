from pyspark.ml import Transformer
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    StructType,
    StructField,
    DateType,
    FloatType,
    ArrayType
)

from pyspark.ml import Transformer

"""
implement _transform
and create member attribute sch_dict
store schema as dict with blah format
"""


class SparkNativeTransformer(Transformer):

    def test_Schema(self, incomingSchema, sch_dict):
        def nullSwap(st1, st2):
            """Function to swap datatype null parameter within a nested dataframe schema"""
            if not set([sf.name for sf in st1]).issubset(set([sf.name for sf in st2])):
                raise ValueError("Keys for first schema aren't a subset of the second.")
            for sf in st1:
                sf.nullable = st2[sf.name].nullable
                if isinstance(sf.dataType, StructType):
                    if not set([sf.name for sf in st1]).issubset(set([sf.name for sf in st2])):
                        raise ValueError("Keys for first schema aren't a subset of the second.")
                    nullSwap(sf.dataType, st2[sf.name].dataType)
                if isinstance(sf.dataType, ArrayType):
                    sf.dataType.containsNull = st2[sf.name].dataType.containsNull

        sch_list = []
        for x in sch_dict.keys():
            sch_list.append(StructField(sch_dict[x][0], sch_dict[x][1]))
        schema = StructType(sch_list)
        nullSwap(schema, incomingSchema)
        if not (sum([x not in schema for x in incomingSchema]) > 0):
            raise ValueError("Keys for first schema aren't a subset of the second.")

    def transform(self, dataset, params=None):
        """
        Transforms the input dataset with optional parameters.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            input dataset
        params : dict, optional
            an optional param map that overrides embedded params.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            transformed dataset
        """

        self.test_Schema(dataset.schema, self.sch_dict)

        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset)
            else:
                return self._transform(dataset)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))