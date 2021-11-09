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
