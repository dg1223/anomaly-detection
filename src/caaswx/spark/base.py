from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params
from pyspark.sql.functions import (
    count,
    col,
    when,
    lag,
    isnull,
    regexp_extract,
    window,
    countDistinct,
    array_remove,
    array_distinct,
    sort_array,
    collect_set,
    collect_list,
    mean as sparkmean,
    stddev as sparkstddev,
    size as sparksize,
    min as sparkmin,
    max as sparkmax,
    round as sparkround,
    sum as sparksum,
)

from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ArrayType,
    TimestampType,
    StringType,
)
from pyspark.sql.window import Window
from utils import HasTypedOutputCol
from src.caaswx.spark._transformers.refactoring import HasInputSchema


class GroupbyFeature(HasInputSchema):
    """
    A feature that maintains an Input Schema (StructField) and
    pre/agg/post operations.
    """

    def pre_op(self, dataset):
        """
        The pre-operation performed by this feature before
        aggregation.
        :param dataset: The input dataframe.
        :type dataset: :class:`pyspark.sql.DataFrame`
        :return: The input DataFrame after applying this
        feature's pre-operation.
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        raise NotImplementedError()

    def agg_op(self):
        """
        This feature's aggregating operation performed
        during the groupby.
        :return: A SQL clause describing the aggregating
        function.
        :rtype: :class:`pyspark.sql.Column'
        """
        raise NotImplementedError()

    def post_op(self, dataset):
        """
        The post-operation performed by this feature after
        aggregation.
        :param dataset: The input dataframe.
        :type dataset: :class:`pyspark.sql.DataFrame`
        :return: The input DataFrame after applying this
        feature's post-operation.
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        raise NotImplementedError()

    def get_transformer(self, group_keys):
        """
        :return: A transformer that calculates this feature.
        :rtype: :class:`caaswx.spark.transformers.GroupbyTransformer`
        :param group_keys: strings describing the columns that the
        returned transformed aggregates on.
        :type group_keys: :class:list
        """
        return GroupbyTransformer(group_keys=group_keys, features=[self])
        

class GroupbyTransformer(Transformer):
    """
    A transformer that computes a list of features during a single
    groupby operation.
    """

    def __init__(self, group_keys, features):
        """
        :param group_keys: Columns to be aggregated on during
        groupby
        :param features: a list of features to be calculated by
        this transformer.
        :type group_keys: list of str
        :type features: list of :class:`Feature`
        :type features: list of :class:`GroupbyFeature`
        """
        super(GroupbyTransformer, self).__init__()
        self._features = features
        self._group_keys = group_keys

    def _transform(self, dataset):
        for feature in self._features:
            dataset = feature.pre_op(dataset)
        dataset = dataset.groupby(*self._group_keys).agg(
            *[
                feature.agg_op()
                for feature in self._features
                if feature.agg_op() is not None
            ]
        )
        for feature in self._features:
            dataset = feature.post_op(dataset)
        return dataset


class WindowedGroupbyTransformer(GroupbyTransformer):

    """
    A transformer that computes a list of features during a single
    groupby operation, where the input has Bucketized rows into one
    or more time windows given window length and step.
    """

    window_length = Param(
        Params._dummy(),
        "window_length",
        "Length of the timestamp window's slice in seconds",
    )
    window_step = Param(
        Params._dummy(),
        "window_step",
        "Width of the timestamp windows in seconds",
    )

    def __init__(self, group_keys, features, window_length, window_step):
        """
        :param group_keys: Columns to be aggregated on during groupby
        :param features: A list of features to be calculated by this
        transformer
        :param window_length: Length of the window slice to be
        considered for each row
        :param window_step: Size of hop between window's current
        position and next one
        :type group_keys: list of str
        :type features: list of :class:`GroupbyFeature`
        :type window_length: int
        :type window_step: int
        """
        super(WindowedGroupbyTransformer, self).__init__(
            group_keys=group_keys, features=features
        )

        self._setDefault(window_length=900, window_step=900)

        self._set(window_length=window_length, window_step=window_step)

    def get_window_length(self):
        """
        Gets this entity's window_length
        """
        return self.getOrDefault("window_length")

    def get_window_step(self):
        """
        Gets this entity's window_step
        """
        return self.getOrDefault("window_step")

    def _transform(self, dataset):

        for feature in self._features:
            dataset = feature.pre_op(dataset)
        dataset = dataset.groupby(
            *self._group_keys,
            window(
                "SM_TIMESTAMP",
                str(self.getOrDefault("window_length")) + " seconds",
                str(self.getOrDefault("window_step")) + " seconds",
            )
        ).agg(
            *[
                feature.agg_op()
                for feature in self._features
                if feature.agg_op() is not None
            ]
        )

        for feature in self._features:
            dataset = feature.post_op(dataset)
        return dataset


class CounterFeature(GroupbyFeature, HasTypedOutputCol):

    """
    Base class for counter feature, calculates total number of elements in the given
    group. 
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        
        :param outputColType: Type of column
        :type outputColType: IntegerType()
        """
        super(CounterFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=IntegerType())

    def count_clause(self):
        raise NotImplementedError()

    def agg_op(self):
        return count(self.count_clause()).alias(self.getOutputCol())


class DistinctCounterFeature(GroupbyFeature, HasTypedOutputCol):

    """
    Base class for distinct counter feature, calculates distinct number of elements 
    in the given group. 
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: IntegerType

        :param outputColType: Type of column
        :type outputColType: IntegerType()
        """
        super(DistinctCounterFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=IntegerType())

    def count_clause(self):
        raise NotImplementedError()

    def agg_op(self):
        return countDistinct(self.count_clause()).alias(self.getOutputCol())


class ArrayDistinctFeature(GroupbyFeature, HasTypedOutputCol):

    """
    Base class for array distinct feature, calculates a distinct list of objects from
    the grouped data.

    Removes Duplicates from grouped data.
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType

        :param outputColType: Type of column
        :type outputColType: ArrayType(StringType())
        """
        super(ArrayDistinctFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=ArrayType(StringType()))

    def array_clause(self):
        raise NotImplementedError()

    def agg_op(self):
        return array_distinct(
            collect_list(self.array_clause()).alias(self.getOutputCol())
        )


class ArrayRemoveFeature(GroupbyFeature, HasTypedOutputCol):

    """
    Base class for array remove feature, calculates a distinct list of objects from
    the grouped data with objects of 0 length removed.

    Designed to handle excess blank spaces("") created by regex operations.
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: ArrayType

        :param outputColType: Type of column
        :type outputColType: ArrayType(StringType())
        """
        super(ArrayRemoveFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=ArrayType(StringType()))

    def array_clause(self):
        raise NotImplementedError()

    def agg_op(self):
        return array_remove(
            array_distinct(self.array_clause()),
            "",
        ).alias(self.getOutputCol())


class SizeArrayRemoveFeature(GroupbyFeature, HasTypedOutputCol):

    """
    Base size of array remove feature, calculates the size of a distinct list 
    of objects from the grouped data with empty Strings removed.

    Designed to handle excess blank spaces("") created by regex operations.
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: IntegerType

        :param outputColType: Type of column
        :type outputColType: IntegerType()
        """
        super(SizeArrayRemoveFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=IntegerType())

    def array_clause(self):
        raise NotImplementedError()

    def agg_op(self):
        return sparksize(
            array_remove(
                self.array_clause(),
                "",
            )
        ).alias(self.getOutputCol())





