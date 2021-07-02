# Importing various datatype supported by Spark for specifying the schemas
# of result dataframes

# Importing OOP decorators
from pyspark import keyword_only
from pyspark.ml.param.shared import TypeConverters, Param, Params

# Importing window module for performing time slicing while grouping data
from pyspark.sql.functions import window
from pyspark.sql.window import Window

# Importing the Transformer class to be extended by Flattener classes
from pyspark.ml import Transformer

import pyspark.sql.functions as func


class ResourcesFlattener(Transformer):
    """
    User Feature transformer for the Streamworx project.
    """

    window_length = Param(
        Params._dummy(),
        "windowLength",
        "Length of the sliding window used for entity resolution. " +
        "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt
    )

    window_step = Param(
        Params._dummy(),
        "windowStep",
        "Length of the sliding window step-size used for entity resolution. " +
        "Given as an integer in seconds.",
        typeConverter=TypeConverters.toInt
    )

    entity_name = Param(
        Params._dummy(),
        "entityName",
        "Name of the column to perform aggregation on, together with the " +
        "sliding window.",
        typeConverter=TypeConverters.toString
    )

    max_resource_count = Param(
        Params._dummy(),
        "max_resource_count",
        "Maximum count of resources allowed in the resource list within the " +
        "sliding window.",
        typeConverter=TypeConverters.toInt
    )

    @keyword_only
    def __init__(
            self,
            entity_name='SM_USERNAME',
            window_length=900,
            window_step=900,
            max_resource_count=-1):
        """
        def __init__(self, *, window_length = 900, window_step = 900)
        """
        super(ResourcesFlattener, self).__init__()
        self._setDefault(
            window_length=900,
            window_step=900,
            entity_name='SM_USERNAME',
            max_resource_count=-1)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(
            self,
            entity_name='SM_USERNAME',
            window_length=900,
            window_step=900,
            max_resource_count=-1):
        """
        Sets params for this ResourcesFlattener.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def set_entity_name(self, value):
        """
        Sets the Entity Name
        """
        self._set(entity_name=value)

    def set_window_length(self, value):
        """
        Sets this ResourcesFlattener's window length.
        """
        self._set(window_length=value)

    def set_window_step(self, value):
        """
        Sets this ResourcesFlattener's window step size.
        """
        self._set(window_step=value)

    def set_max_resource_count(self, value):
        """
        Sets this ResourcesFlattener's maximum resource count.
        """
        self._set(max_resource_count=value)

    def _transform(self, dataset):
        """
        flatten input dataset w.r.t URLs
        Input : Siteminder dataframe
        Output : flattened URLs merged into lists
        """
        if int(self.getOrDefault('max_resource_count')) > 0:
            window_spec = (
                Window.
                partitionBy('window', str(self.getOrDefault('entity_name'))).
                orderBy('SM_TIMESTAMP')
            )
            dataset = (
                dataset
                .withColumn(
                    'window',
                    window(
                        'SM_TIMESTAMP',
                        str(self.getOrDefault('window_length')) + ' seconds',
                        str(self.getOrDefault('window_step')) + ' seconds')
                )
                .withColumn('dense_rank', func.dense_rank().over(window_spec))
            )
            dataset = dataset.filter(
                dataset['dense_rank'] <= int(
                    self.getOrDefault('max_resource_count')))
        return (
            dataset
            .groupby(
                str(self.getOrDefault('entity_name')),
                window(
                    'SM_TIMESTAMP', str(
                        self.getOrDefault('window_length')) + ' seconds',
                    str(self.getOrDefault('window_step')) + ' seconds'
                )
            ).agg(func.collect_list('SM_RESOURCE').alias('SM_RESOURCE'))
        )
