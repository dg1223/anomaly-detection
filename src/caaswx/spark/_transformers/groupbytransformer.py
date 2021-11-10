from sparknativetransformer import SparkNativeTransformer
from src.caaswx.spark._transformers.refactoring import HasInputSchema
from src.caaswx.spark.utilities.schema_utils import schema_concat


class GroupbyTransformer(SparkNativeTransformer, HasInputSchema):
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
        """
        super(GroupbyTransformer, self).__init__()
        self._features = features
        self._group_keys = group_keys
        feature_schemas = [
            feature.get_input_schema() for feature in self._features
        ]
        self.set_input_schema(schema_concat(list(feature_schemas)))

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
