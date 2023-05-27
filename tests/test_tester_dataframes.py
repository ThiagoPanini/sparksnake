"""Test cases for features defined on tester/dataframes.py module.

This file handles test cases for features on datframes.py module located on
tester parent module, ensuring that everything is working properly.

___
"""

# Importing libraries
import pytest

from sparksnake.tester.dataframes import parse_string_to_spark_dtype

from tests.helpers.user_inputs import FAKE_SCHEMA_INFO

from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType,\
    FloatType, DoubleType, BooleanType, DateType, TimestampType, StructType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_string_reference_correctly_parsed_as_spark_string_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "string"
    T: Then the return must be a StringType object
    """

    assert parse_string_to_spark_dtype("string") == StringType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_int_reference_correctly_parsed_as_spark_integer_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "int"
    T: Then the return must be a IntegerType object
    """

    assert parse_string_to_spark_dtype("int") == IntegerType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_integer_reference_correctly_parsed_as_spark_integer_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "integer"
    T: Then the return must be a IntegerType object
    """

    assert parse_string_to_spark_dtype("integer") == IntegerType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_long_reference_correctly_parsed_as_spark_long_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "long"
    T: Then the return must be a LongType object
    """

    assert parse_string_to_spark_dtype("long") == LongType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_bigint_reference_correctly_parsed_as_spark_long_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "bigint"
    T: Then the return must be a LongType object
    """

    assert parse_string_to_spark_dtype("bigint") == LongType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_decimal_reference_correctly_parsed_as_spark_decimal_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "decimal"
    T: Then the return must be a DecimalType object
    """

    assert parse_string_to_spark_dtype("decimal") == DecimalType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_float_reference_correctly_parsed_as_spark_float_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "float"
    T: Then the return must be a FloatType object
    """

    assert parse_string_to_spark_dtype("float") == FloatType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_double_reference_correctly_parsed_as_spark_double_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "double"
    T: Then the return must be a DoubleType object
    """

    assert parse_string_to_spark_dtype("double") == DoubleType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_boolean_reference_correctly_parsed_as_spark_boolean_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "boolean"
    T: Then the return must be a BooleanType object
    """

    assert parse_string_to_spark_dtype("boolean") == BooleanType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_date_reference_correctly_parsed_as_spark_date_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "date"
    T: Then the return must be a DateType object
    """

    assert parse_string_to_spark_dtype("date") == DateType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_timestamp_reference_correctly_parsed_as_spark_timestamp_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "timestamp"
    T: Then the return must be a TimestampType object
    """

    assert parse_string_to_spark_dtype("timestamp") == TimestampType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_error_when_trying_to_parse_an_invalid_string_to_spark_dtype():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument assuming a string reference that is not parseable as a valid
       Spark dtype (i.e. "foo")
    T: Then a TypeError exception must be raised
    """

    with pytest.raises(TypeError):
        _ = parse_string_to_spark_dtype(dtype="foo")


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframe_schema
def test_function_generate_dataframe_schema_returns_a_structtype_object(
    fake_schema
):
    """
    G: Given that users want to create a Spark DataFrame schema object using
       a predefined schema list with attributes info
    W: When the funtion generate_dataframe_schema() is called
    T: Then the return object must be a StructType object
    """

    assert type(fake_schema) is StructType


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframe_schema
def test_schema_object_generated_contains_all_predefined_field_names(
    fake_schema
):
    """
    G: Given that users want to create a Spark DataFrame schema object using
       a predefined schema list with attributes info
    W: When the funtion generate_dataframe_schema() is called
    T: Then the schema object returned must have all field names previously
       defined by user on the list used to generate the schema
    """

    # Extracting the field names of the user predefined attributes list
    expected_field_names = [field["Name"] for field in FAKE_SCHEMA_INFO]

    assert fake_schema.fieldNames() == expected_field_names


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframe_schema
def test_schema_object_generated_contains_all_predefined_data_types(
    fake_schema
):
    """
    G: Given that users want to create a Spark DataFrame schema object using
       a predefined schema list with attributes info
    W: When the funtion generate_dataframe_schema() is called
    T: Then the schema object returned must have all data types previously
       defined by user on the list used to generate the schema
    """

    # Extracting the field names of the user predefined attributes list
    expected_dtypes = [
        parse_string_to_spark_dtype(f["Type"]) for f in FAKE_SCHEMA_INFO
    ]

    # Extracting the data types on the returned schema
    schema_dtypes = [type(field.dataType) for field in fake_schema]

    assert schema_dtypes == expected_dtypes
