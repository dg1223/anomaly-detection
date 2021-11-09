from groupbytransformer import GroupbyTransformer
from src.caaswx.spark._transformers.refactoring import HasInputSchema


class GroupbyFeature(HasInputSchema):
    def pre_op(self, dataset):
        raise NotImplementedError()

    def agg_op(self):
        raise NotImplementedError()

    def post_op(self, dataset):
        raise NotImplementedError()

    def get_transformer(self, group_keys):
        return GroupbyTransformer(group_keys=group_keys, features=[self])
