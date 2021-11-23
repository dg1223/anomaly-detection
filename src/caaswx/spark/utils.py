# Module containing the utility files and mixins

from pyspark.ml.param.shared import HasInputCol, HasInputCols, HasOutputCol, \
    HasOutputCols
from pyspark.ml.param import Param, Params


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
