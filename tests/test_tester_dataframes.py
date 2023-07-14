"""Test cases for features defined on tester/dataframes.py module.

This file handles test cases for features on datframes.py module located on
tester parent module, ensuring that everything is working properly.

___
"""

# Importing libraries
import pytest
from decimal import Decimal
from datetime import date, datetime

from sparksnake.tester.dataframes import (
    parse_string_to_spark_dtype,
    compare_schemas,
    generate_fake_dataframe
)

from tests.helpers.user_inputs import (
    FAKE_SCHEMA_INFO,
    FAKE_DATAFRAMES_DEFINITION
)

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DecimalType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    StructType,
    ArrayType
)


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
def test_array_str_reference_correctly_parsed_as_spark_timestamp_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "array<string>"
    T: Then the return must be a ArrayType(StringType()) object
    """

    dtype = parse_string_to_spark_dtype(dtype="array<string>")
    assert dtype == ArrayType(StringType())


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_array_int_reference_correctly_parsed_as_spark_timestamp_dype_object():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "array<int>"
    T: Then the return must be a ArrayType(IntegerType()) object
    """

    dtype = parse_string_to_spark_dtype(dtype="array<int>")
    assert dtype == ArrayType(IntegerType())


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_error_on_try_to_parse_an_array_type_with_a_not_supported_inner_type():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "array<foo>"
    T: Then a TypeError must be raised
    """

    with pytest.raises(TypeError):
        _ = parse_string_to_spark_dtype(dtype="array<foo>")


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.parse_string_to_spark_dtype
def test_error_on_try_to_parse_an_array_type_with_an_invalid_string_type_ref():
    """
    G: Given that users want to get a valid Spark dtype object based on a
       python string reference
    W: When the function parse_string_to_spark_dtype() is called with dtype
       argument equals to "array(string)"
    T: Then a TypeError must be raised
    """

    with pytest.raises(TypeError):
        _ = parse_string_to_spark_dtype(dtype="array(string)")


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
@pytest.mark.skip(reason="Work in progress")
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
    schema_dtypes = [
        type(field.dataType) for field in fake_schema
    ]

    print(expected_dtypes)
    print(schema_dtypes)

    assert schema_dtypes == expected_dtypes


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_fake_data_from_schema
def test_function_generate_fake_data_from_schema_returns_a_list(fake_data):
    """
    G: Given that users want to generate fake data based on a DataFrame schema
    W: When the function generate_fake_data_from_schema() is called
    T: Then the returned object must be a list
    """

    assert type(fake_data) is list


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_fake_data_from_schema
def test_function_generate_fake_data_from_schema_returns_a_list_of_tuples(
    fake_data
):
    """
    G: Given that users want to generate fake data based on a DataFrame schema
    W: When the function generate_fake_data_from_schema() is called
    T: Then all elements for the returned list must be tuples
    """

    # Extracting the type of the inner elements of the fake_data list
    fake_data_inner_elements = [type(row) for row in fake_data]
    fake_data_inner_elements_type = list(set(fake_data_inner_elements))[0]

    assert fake_data_inner_elements_type is tuple


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_fake_data_from_schema
def test_function_generate_fake_data_from_schema_returns_correct_num_of_rows(
    fake_data
):
    """
    G: Given that users want to generate fake data based on a DataFrame schema
    W: When the function generate_fake_data_from_schema() is called with
       n_rows=5 to generate exactly 5 rows
    T: Then the returned object must be a list with 5 elements
    """

    assert len(fake_data) == 5


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_fake_data_from_schema
@pytest.mark.skip(reason="Work in progress")
def test_function_generate_fake_data_from_schema_returns_expected_data_types(
    fake_data
):
    """
    G: Given that users want to generate fake data based on a DataFrame schema
    W: When the function generate_fake_data_from_schema() with a predefined
       schema
    T: Then the elements returned in the list must have a set of expected
       data types based on the input schema provided
    """

    # Extracting data types of the generated fake data
    fake_data_types = [[type(element) for element in row] for row in fake_data]

    # Getting a sample row to compare
    row_data_types = fake_data_types[0]

    # Creating an expected list of data types
    expec_types = [str, int, int, Decimal, float, float, bool, date, datetime]

    assert row_data_types == expec_types


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_fake_dataframe
def test_function_generate_fake_dataframe_returns_a_spark_dataframe_object(
    df_fake
):
    """
    G: Given users want to generate a fake Spark DataFrame object
    W: When the function generate_fake_dataframe() is called
    T: Then the return must be a Spark DataFrame object
    """

    assert type(df_fake) is DataFrame


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.compare_schemas
def test_compare_schemas_returns_true_when_comparing_equal_schemas(df_fake):
    """
    G: Given that users want to compare two Spark DataFrame's schemas
    W: When the function compare_schemas() is called with two Spark
       DataFrames with the same schema
    T: Then the return must be True
    """

    assert compare_schemas(df1=df_fake, df2=df_fake)


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.compare_schemas
def test_compare_schemas_returns_false_when_comparing_different_schemas(
    spark_session
):
    """
    G: Given that users want to compare two Spark DataFrame's schemas
    W: When the function compare_schemas() is called with two Spark
       DataFrames with different schema
    T: Then the return must be False
    """

    # Defining the attributes for the first fake DataFrame
    fake_schema_1 = [
        {
            "Name": "id",
            "Type": "string",
            "nullable": True
        },
        {
            "Name": "value",
            "Type": "integer",  # This is the difference!
            "nullable": True
        }
    ]

    # Defining the attributes for the second fake DataFrame
    fake_schema_2 = [
        {
            "Name": "id",
            "Type": "string",
            "nullable": True
        },
        {
            "Name": "value",
            "Type": "string",
            "nullable": True
        }
    ]

    # Creating fake DataFrames
    df_fake_1 = generate_fake_dataframe(spark_session, fake_schema_1)
    df_fake_2 = generate_fake_dataframe(spark_session, fake_schema_2)

    # Comparing both DataFrames
    assert not compare_schemas(df1=df_fake_1, df2=df_fake_2)


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframes_dict
def test_compare_schemas_nullable_info_return_false_when_nullable_is_diff(
    spark_session
):
    """
    G: Given that users want to compare two Spark DataFrame's schemas
    W: When the function compare_schemas() is called with two Spark
       DataFrames with schemas having the same attributes and the same data
       types but different nullable flag (considering the function is called
       with flag compare_nullable_info=True)
    T: Then the return must be False
    """

    # Defining the attributes for the first fake DataFrame
    fake_schema_1 = [
        {
            "Name": "id",
            "Type": "string",
            "nullable": True
        },
        {
            "Name": "value",
            "Type": "integer",
            "nullable": True
        }
    ]

    # Defining the attributes for the second fake DataFrame
    fake_schema_2 = [
        {
            "Name": "id",
            "Type": "string",
            "nullable": True
        },
        {
            "Name": "value",
            "Type": "integer",
            "nullable": False  # This is the difference!
        }
    ]

    # Creating fake DataFrames
    df_fake_1 = generate_fake_dataframe(spark_session, fake_schema_1)
    df_fake_2 = generate_fake_dataframe(spark_session, fake_schema_2)

    # Comparing both DataFrames
    assert not compare_schemas(
        df1=df_fake_1,
        df2=df_fake_2,
        compare_nullable_info=True
    )


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframes_dict
def test_function_generate_dataframes_dict_returns_a_python_dictionary(
    dataframes_dict: dict
):
    """
    G: Given that users want to generate multiple DataFrame objects based in
       one dictionary definition with user configuration provided
    W: When the function generate_dataframes_dict() is called
    T: Then the return must be a Python dictionary
    """

    assert type(dataframes_dict) is dict


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframes_dict
def test_dataframes_dict_has_the_expected_number_of_dataframe_objects(
    dataframes_dict: dict
):
    """
    G: Given that users want to generate multiple DataFrame objects based in
       one dictionary definition with user configuration provided
    W: When the function generate_dataframes_dict() is called
    T: Then the length of the returned dictionary (i.e the number of DataFrame)
       objects matchs the same as defined on the inputs dictionary used to
       generate the DataFrames
    """

    assert len(dataframes_dict) == len(FAKE_DATAFRAMES_DEFINITION)


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframes_dict
def test_dataframes_dict_has_spark_dataframes_objects_in_its_content(
    dataframes_dict: dict
):
    """
    G: Given that users want to generate multiple DataFrame objects based in
       one dictionary definition with user configuration provided
    W: When the function generate_dataframes_dict() is called
    T: Then the returned dictionary must have Spark DataFrame objects
    """

    # Extracting the type of the dictionary values
    dict_objects = [type(value) for value in list(dataframes_dict.values())]
    object_type = list(set(dict_objects))[0]

    assert object_type is DataFrame


@pytest.mark.tester
@pytest.mark.dataframes
@pytest.mark.generate_dataframes_dict
def test_function_generate_dataframes_dict_generates_empty_df_when_told_to(
    dataframes_dict: dict,
    empty_data_dataframe_reference: str = "df_with_empty_data"
):
    """
    G: Given that users want to generate multiple DataFrame objects based in
       one dictionary definition with user configuration provided
    W: When the function generate_dataframes_dict() is called with a
       predefined user input that has at least one dataframe definition
       with the empty data flag equals to True
    T: Then this dataframe must not have any data
    """

    assert dataframes_dict[empty_data_dataframe_reference].count() == 0
