# Module containing the utility files and mixins

from pyspark.ml.param.shared import HasInputCol, HasInputCols, HasOutputCol, \
    HasOutputCols
from pyspark.ml.param import Param, Params
from src.caaswx.spark.utilities.schema_utils import schema_is_subset
import pyspark
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params


class HasInputSchema(Transformer):
    """
    A mixin for entities which maintain an input schema.
    """

    input_schema = Param(
        Params._dummy(),
        "input_schema",
        "Param specifying the required schema of the input dataframe.",
    )

    @keyword_only
    def __init__(self):
        super(HasInputSchema, self).__init__()

    def schema_is_admissable(
        self, schema: pyspark.sql.types.StructType, compare_nulls=False
    ):
        """
        Returns ``True`` if each :class:`StructField` of ``schema``
        is contained in this entity's schema, modulo nullability if
        ``compare_nulls`` is ``False``.

        :param schema: The input schema to be checked.
        :param compare_nulls: If this flag is ``False``, comparison
        of :class:`pyspark.sql.Types.StructField`'s is done
        ignoring nullability.
        :type schema: :class:`pyspark.sql.Types.StructType`
        :type compare_nulls: ``boolean``
        """
        return schema_is_subset(
            self.input_schema, schema, compare_nulls=compare_nulls
        )

    def set_input_schema(self, schema: pyspark.sql.types.StructType):
        """
        Sets this entity's input schema.
        :param schema: The input schema to be set.
        :type schema: :class:`pyspark.sql.types.StructType`
        """
        self.set("input_schema", schema)

    def get_input_schema(self):
        """
        Gets this entity's input schema.
        """
        return self.getOrDefault("input_schema")



class HasTypedInputCol(HasInputCol):
    """
    A mixin for entities that maintain the input column type.
    """

    inputColType = Param(Params._dummy(), "inputColType", "input column type")

    def __init__(self):
        super(HasTypedInputCol, self).__init__()

    def getInputColType(self):
        """
        Gets type of input column.

        :return: Param inputColType
        :rtype: StringType
        """
        return self.getOrDefault("inputColType")


class HasTypedInputCols(HasInputCols):
    """
    A mixin for entities that maintain multiple columns and their types
    """

    inputColsType = Param(Params._dummy(), "inputColsType",
                          "input column type")

    def __init__(self):
        super(HasTypedInputCols, self).__init__()

    def getInputColType(self):
        """
        Gets multiple input column types

        :return: Param inputColsType
        :rtype: ArrayType
        """
        return self.getOrDefault("inputColsType")


class HasTypedOutputCol(HasOutputCol):
    """
    A mixin for entities that maintain an output column type
    """

    outputColType = Param(Params._dummy(), "outputColType",
                          "output column type")  # <- this is a string

    def __init__(self):
        super(HasTypedOutputCol, self).__init__()

    def getOutputColType(self):
        """
        Gets the output column type

        :return: Param outputColType
        :rtype: StringType
        """
        return self.getOrDefault("outputColType")


class HasTypedOutputCols(HasOutputCols):
    """
    A mixin for entities that maintain multiple output columns and their types
    """

    outputColsType = Param(Params._dummy(), "outputColsType",
                           "output column type")

    def __init__(self):
        super(HasTypedOutputCols, self).__init__()

    def getOutputColType(self):
        """
        Gets multiple output column types

        :return: Param outputColsType
        :rtype: ArrayType
        """
        return self.getOrDefault("outputColsType")
