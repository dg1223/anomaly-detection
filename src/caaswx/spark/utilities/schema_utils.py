from pyspark.sql.types import StructType, ArrayType


def null_swap(st1, st2):
    """
    Function to swap datatype null parameter within a nested dataframe
    schema
    """
    for sf in st1:
        sf.nullable = st2[sf.name].nullable
        if isinstance(sf.dataType, StructType):
            null_swap(sf.dataType, st2[sf.name].dataType)
        if isinstance(sf.dataType, ArrayType):
            sf.dataType.containsNull = st2[sf.name].dataType.containsNull


def struct_field_compare(sf1, sf2):
    """
    Returns True if two structfeilds are the same, else False.
    """
    if isinstance(sf1.dataType, StructType):
        return struct_compare(sf1.dataType, sf2.dataType)
    else:
        return sf1.dataType == sf2.dataType


def struct_compare(st1, st2):
    """
    Returns True if one schema is a subset of another, else False.
    """
    if not {sf.name for sf in st1}.issubset({sf.name for sf in st2}):
        return False
    for sf in st1:
        if not struct_field_compare(sf, st2[sf.name]):
            return False
    return True


def schema_is_subset(schema1, schema2, compare_nulls=False):
    """
    Returns true if each StructField of schema2 is contained in schema1.
    Comparison ignores nullable field if compare_nulls is False.
    schema1: incoming schema(schema to be checked)
    schema2: required schema(schema to be checked against)
    """

    if compare_nulls:
        return all([x in schema2 for x in schema1])
    else:
        return struct_compare(schema1, schema2)


def schema_is_equal(schema1, schema2, compare_nulls=False):
    """
    Returns true if Returns true if each StructField of schema2 is contained in
    schema1, and vice versa. Comparison ignores nullable field if compare_nulls
    is False.
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
    """
    if compare_nulls:
        return structfield in schema
    return struct_compare(StructType([structfield]), schema)


def schema_concat(schema_list):
    """
    Returns a Schema, without any duplicates. If their is a duplicate name
    amongst the structfields with differing datatype an Exception will be
    thrown.
    """

    duplicate_rem = set()
    for sf in schema_list:
        duplicate_rem.add(sf)

    schema_name_list = [sf.name for sf in duplicate_rem]
    if len(schema_name_list) != len(set(schema_name_list)):
        raise Exception("Duplicate Name, Type Mismatch ERROR.")

    return StructType(list(duplicate_rem))
