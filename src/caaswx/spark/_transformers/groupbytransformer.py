from sparknativetransformer import SparkNativeTransformer
from src.caaswx.spark._transformers.refactoring import HasInputSchema
from src.caaswx.spark.utilities.schema_utils import schema_concat


class GroupbyTransformer(SparkNativeTransformer, HasInputSchema):
    """
    A transformer that GroupbyFeature operates using.
    """

    def __init__(self, group_keys, features):
        """
        Initializes transformer, schemas are concatenated and set automatically
        :param group_keys: IDs of column to be used
        :param features: Different features to be generated
        :type group_keys: list
        :type features: list
        """
        super(GroupbyTransformer, self).__init__()
        self._features = features
        self._group_keys = group_keys
        feature_schemas = [
            feature.get_input_schema() for feature in self._features
        ]
        self.set_input_schema(schema_concat(list(feature_schemas)))

    def _transform(self, dataset):
        """
        Runs pre_op, agg_op, and post_op in order corresponding to features
        """
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
