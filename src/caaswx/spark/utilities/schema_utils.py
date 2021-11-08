from pyspark.sql.types import StructType, ArrayType


def null_swap(st1, st2):
    """
    Function to swap datatype null parameter within a nested dataframe
    schema.

    :param st1: The StructType that will have its nullability changed.
    :type st1: StructType

    :param st2: The StructType that will be used as the standard for changing
    nullability.
    :type st2: StructType
    """
    for sf in st1:
        sf.nullable = st2[sf.name].nullable
        if isinstance(sf.dataType, StructType):
            null_swap(sf.dataType, st2[sf.name].dataType)
        if isinstance(sf.dataType, ArrayType):
            sf.dataType.containsNull = st2[sf.name].dataType.containsNull


def struct_field_compare(sf1, sf2):
    """
    Compares two StructField against one another.

    :param sf1: The first StructField to be tested for comparison.
    :type sf1: StructField

    :param sf2:  The second StructField to be tested for comparison.
    :type sf2: StructField

    :return: Returns True if two structfields are the same, else False.
    :rtype: Boolean
    """
    if isinstance(sf1.dataType, StructType):
        return struct_compare(sf1.dataType, sf2.dataType)
    else:
        return sf1.dataType == sf2.dataType


def struct_compare(st1, st2):
    """
    Compares two StructType against one another.

    :param st1: The StructType that will be tested for being a subset.
    :type st1: StructType

    :param st2: The StructType that will be tested against(potential superset).
    :type st2: StructType

    :return: Returns True if one schema is a subset of another, else False.
    :rtype: Boolean
    """
    if not {sf.name for sf in st1}.issubset({sf.name for sf in st2}):
        return False
    for sf in st1:
        if not struct_field_compare(sf, st2[sf.name]):
            return False
    return True


def schema_is_subset(schema1, schema2, compare_nulls=False):
    """
    Tests whether each StructField of schema2 is contained in schema1.
    Comparison ignores nullable field if compare_nulls is False.

    :param schema1: The StructType that is the incoming schema(schema to be
    checked).
    :type schema1: StructType

    :param schema2: The StructType that will be the required schema(schema to
    be checked against).
    :type schema2: StructType

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean

    :return: Returns true if each StructField of schema2 is contained in
    schema1.
    :rtype: Boolean
    """

    if compare_nulls:
        return all([x in schema2 for x in schema1])
    else:
        return struct_compare(schema1, schema2)


def schema_is_equal(schema1, schema2, compare_nulls=False):
    """
    Test Whether each StructField of schema2 is contained in
    schema1, and vice versa. Comparison ignores nullable field if compare_nulls
    is False.

    :param schema1: The first StructType to be tested for equality.
    :type schema1: StructType

    :param schema2: The second StructType to be tested for equality.
    :type schema2: StructType

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean

    :return: Returns true if Returns true if each StructField of schema2 is
    contained in schema1, and vice versa.
    :rtype: Boolean
    """
    if compare_nulls:
        return schema1 == schema2
    else:
        return struct_compare(schema1, schema2) and struct_compare(
            schema2, schema1
        )


def schema_contains(schema, structfield, compare_nulls=False):
    """
    Returns true if structfield is a field in schema, ignoring nullability if
    compare_nulls is false.

    :param schema: The StructType that the StructField will be tested against.
    :type schema: StructType

    :param structfield: The StructField that will be tested for existence in
    the schema.
    :type structfield: StructField

    :param compare_nulls: Flag controls if nullability will be considered.
    :type compare_nulls: Boolean
    """
    if compare_nulls:
        return structfield in schema
    return struct_compare(StructType([structfield]), schema)


def schema_concat(schema_list):
    """
    Returns a Schema, without any duplicates. If their is a duplicate name
    amongst the structfields with differing datatype an Exception will be
    thrown.

    :param schema_list: The StructType that will be processed.
    :type schema_list: StructType

    ...
    :raises Exception: Duplicate Name, Type Mismatch ERROR.
    ...
    :return: A Schema with no Duplicates.
    :rtype: StructType
    """
    duplicate_rem = set()
    for sf in schema_list:
        duplicate_rem.add(sf)

    schema_name_list = [sf.name for sf in duplicate_rem]
    if len(schema_name_list) != len(set(schema_name_list)):
        raise Exception("Duplicate Name, Type Mismatch ERROR.")

    return StructType(list(duplicate_rem))
