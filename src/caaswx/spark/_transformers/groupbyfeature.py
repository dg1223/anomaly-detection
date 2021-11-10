from groupbytransformer import GroupbyTransformer
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
        :param dataset: The input data frame.
        :type dataset: :class:`pyspark.sql.DataFrame`
        :return: The input DataFrame after applying this
        feature's pre-operation.
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        raise NotImplementedError()

    def agg_op(self):
        """
        This feature's aggegaring operation peformed 
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
        :param dataset: The input data frame.
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
