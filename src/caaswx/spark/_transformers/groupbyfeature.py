from groupbytransformer import GroupbyTransformer
from src.caaswx.spark._transformers.refactoring import HasInputSchema


class GroupbyFeature(HasInputSchema):
    """
    A feature that maintains an Input Schema (StructField) and
    pre/agg/post operations.
    """

    def pre_op(self, dataset):
        """
        Setting up dataset to be aggregated, not always necessary
        """
        raise NotImplementedError()

    def agg_op(self):
        """
        Aggregate operation
        """
        raise NotImplementedError()

    def post_op(self, dataset):
        """
        General cleanup, not always necessary
        """
        raise NotImplementedError()

    def get_transformer(self, group_keys):
        """
        Gets previously defined GroupbyTransformer
        """
        return GroupbyTransformer(group_keys=group_keys, features=[self])
