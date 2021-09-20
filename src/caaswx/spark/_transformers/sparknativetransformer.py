from pyspark.ml import Transformer
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
)

"""This class inherits from the Transformer class and overrides Transform to
add input schema checking. For correct operation it is imperative that
_transform be implemented in the child class and a dictionary "sch_dict" be
implemented as a class attribute in the child class. The sch_dict is to be
formatted as follows: sch_dict = { "Column_1": ["Column_1", __Type()],
"Column_2": ["Column_2", __Type()], } where __Type() are pyspark.sql.types """


class SparkNativeTransformer(Transformer):
    def test_schema(self, incoming_schema, sch_dict):
        def null_swap(st1, st2):
            """Function to swap datatype null parameter within a nested
            dataframe schema """
            if not {sf.name for sf in st1}.issubset({sf.name for sf in st2}):
                raise ValueError(
                    "Keys for first schema aren't a subset of " "the second."
                )
            for sf in st1:
                sf.nullable = st2[sf.name].nullable
                if isinstance(sf.dataType, StructType):
                    if not {sf.name for sf in st1}.issubset(
                        {sf.name for sf in st2}
                    ):
                        raise ValueError(
                            "Keys for first schema aren't a subset of the "
                            "second. "
                        )
                    null_swap(sf.dataType, st2[sf.name].dataType)
                if isinstance(sf.dataType, ArrayType):
                    sf.dataType.containsNull = st2[
                        sf.name
                    ].dataType.containsNull

        sch_list = []
        for x in sch_dict.keys():
            sch_list.append(StructField(sch_dict[x][0], sch_dict[x][1]))
        schema = StructType(sch_list)
        null_swap(schema, incoming_schema)
        if any([x not in incoming_schema for x in schema]):
            raise ValueError(
                "Keys for first schema aren't a subset of the " "second."
            )

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

        self.test_schema(dataset.schema, self.sch_dict)

        if params is None:
            params = {}
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset)
            else:
                return self._transform(dataset)
        else:
            raise ValueError(
                "Params must be a param map but got %s." % type(params)
            )
