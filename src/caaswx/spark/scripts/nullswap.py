from pyspark.sql.types import StructType, ArrayType


def null_swap(st1, st2):
    """Function to swap datatype null parameter within a nested dataframe
    schema """
    for sf in st1:
        sf.nullable = st2[sf.name].nullable
        if isinstance(sf.dataType, StructType):
            null_swap(sf.dataType, st2[sf.name].dataType)
        if isinstance(sf.dataType, ArrayType):
            sf.dataType.containsNull = st2[sf.name].dataType.containsNull
