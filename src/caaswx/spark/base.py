from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params
from pyspark.sql.functions import window, count
from pyspark.sql.types import IntegerType
from utils import HasTypedOutputCol


class GroupbyFeature:
    def pre_op(self, dataset):
        raise NotImplementedError()

    def agg_op(self):
        raise NotImplementedError()

    def post_op(self, dataset):
        raise NotImplementedError()

    def get_transformer(self, group_keys):
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
    Inherited version of GroupbyTransformer for incorporating window
    slices between feature rows.
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
    Base counter feature, will be the parent class to all counting features.
    """

    def __init__(self, outputCol):
        """
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CounterFeature, self).__init__()
        self._set(outputCol=outputCol, outputColType=IntegerType())

    def count_clause(self):
        """
        Counting feature implementation.
        """
        raise NotImplementedError()

    def agg_op(self):
        """
        The aggregation operation that performs the count defined by subclasses

        :return: The Count
        :rtype: IntegerType
        """
        return count(self.count_clause()).alias(self.getOutputCol())