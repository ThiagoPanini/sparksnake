"""Handling operations that help users to improve their test cases.

This module puts together some useful functions created in order to provid
an easy way to fake Spark DataFrames objects. Its features can be imported
and applied on every scenario that demands the creation of fake data rows,
fake schema or even fake Spark DataFrame objects (for example, a conftest
file that defined fixtures for unit test cases).

___
"""

# Importing libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType,\
    IntegerType, LongType, DecimalType, FloatType, DoubleType, BooleanType,\
    DateType, TimestampType

from faker import Faker
from decimal import Decimal
from random import randrange


# Creating a faker object
faker = Faker()
Faker.seed(42)


# Parsing a string for a dtype into a valid Spark dtype
def parse_string_to_spark_dtype(dtype: str):
    """Transform a string dtype reference into a valid Spark dtype.

    This function checks for the data type reference for a field given by users
    while filling the JSON schema file in order to return a valid Spark dtype
    based on the string reference.

    Examples:
        ```python
        # Returning the Spark reference for a "string" data type
        spark_dtype = parse_string_to_spark_dtype(dtype="string")
        # spark_dtype now holds the StringType Spark dtype object
        ```

    Args:
        dtype (str): A string reference for any parseable Spark dtype

    Returns:
        A callable Spark dtype object based on the string reference provided
    """

    # Removing noise on string before validating
    dtype_prep = dtype.lower().strip()

    # Parsing string reference for dtype to spark data type
    if dtype_prep == "string":
        return StringType
    elif dtype_prep in ("int", "integer"):
        return IntegerType
    elif dtype_prep in ("bigint", "long"):
        return LongType
    elif dtype_prep == "decimal":
        return DecimalType
    elif dtype_prep == "float":
        return FloatType
    elif dtype_prep == "double":
        return DoubleType
    elif dtype_prep == "boolean":
        return BooleanType
    elif dtype_prep == "date":
        return DateType
    elif dtype == "timestamp":
        return TimestampType
    else:
        raise TypeError(f"Data type {dtype} is not valid or currently "
                        "parseable into a native Spark dtype")


# Creating a valid Spark DataFrame schema from a list with fields information
def generate_dataframe_schema(
    schema_info: list,
    attribute_name_key: str = "Name",
    dtype_key: str = "Type",
    nullable_key: str = "nullable"
) -> StructType:
    """Generates a StructType Spark schema based on a list of fields info.

    This function receives a preconfigured Python list extracted from a JSON
    schema definition file provided by user in order to return a valid Spark
    schema composed by a StructType structure with multiple StructField objects
    containing informations about name, data type and nullable info about
    attributes.

    Examples:
        ```python
        # Showing an example of a input schema list
        schema_info = [
            {
                "Name": "idx",
                "Type": "int",
                "nullable": true
            },
            {
                "Name": "order_id",
                "Type": "string",
                "nullable": true
            }
        ]

        # Returning a valid Spark schema object based on a dictionary
        schema = generate_dataframe_schema(schema_info)
        ```

    Args:
        schema_info (list):
            A list with information about fields of a DataFrame

        attribute_name_key (str):
            A string identification of the attribute name defined on every
            attribute dictionary

        dtype_key (str):
            A string identification of the attribute type defined on every
            attribute dictionary

        nullable_key (bool):
            A boolean flag that tells if the given attribute defined in
            the dictionary can hold null values

    Returns:
        A StructType object structured in such a way that makes it possible to\
        create a Spark DataFrame with a predefined schema.
    """

    # Extracing the schema based on the preconfigured dict info
    schema = StructType([
        StructField(
            field_info[attribute_name_key],
            parse_string_to_spark_dtype(field_info[dtype_key])(),
            nullable=field_info[nullable_key]
            if nullable_key in field_info.keys() else True
        ) for field_info in schema_info
    ])

    return schema


# Generating fake data based on native Spark data types and the Faker library
def generate_fake_data_from_schema(
    schema: StructType,
    n_rows: int = 5
) -> tuple:
    """Generates fake data based on a Struct Type Spark schema object.

    This function receives a predefined DataFrame schema in order to return
    a list of tuples with fake data generated based on attribute types and
    the Faker library. The way the fake data is structured makes it easy to
    create Spark DataFrames to be used for test purposes.

    Examples:
        ```python
        # Defining a list with attributes info to be used on schema creation
        schema_info = [
            {
                "Name": "idx",
                "Type": "int",
                "nullable": true
            },
            {
                "Name": "order_id",
                "Type": "string",
                "nullable": true
            }
        ]

        # Returning a valid Spark schema object based on a dictionary
        schema = generate_dataframe_schema(schema_info)

        # Generating fake data based on a Spark DataFrame schema
        fake_data = generate_fake_data_from_schema(schema=schema, n_rows=10)
        ```

    Args:
        schema (StructType): a Spark DataFrame schema
        n_rows (int): the number of fake rows to be generated

    Returns:
        A list of tuples where each tuple representes a row with fake data\
        generated using the Faker library according to each data type of\
        the given Spark DataFrame schema. For example, for a string attribute\
        the fake data will be generated using the `faker.word()` method. For a\
        date attribute, the fake data will be generated using the\
        `faker.date_this_year()`. And so it goes on for all other dtypes.
    """

    # Creating fake data based on each schema attribute
    fake_data_list = []
    for _ in range(n_rows):
        # Iterting over columns and faking data
        fake_row = []
        for field in schema:
            dtype = field.dataType.typeName()
            if dtype == "string":
                fake_row.append(faker.word())
            elif dtype == "integer":
                fake_row.append(randrange(-10000, 10000))
            elif dtype == "long":
                fake_row.append(randrange(-10000, 10000))
            elif dtype == "decimal":
                fake_row.append(Decimal(randrange(1, 100000)))
            elif dtype == "boolean":
                fake_row.append(faker.boolean())
            elif dtype == "date":
                fake_row.append(faker.date_this_year())
            elif dtype == "timestamp":
                fake_row.append(faker.date_time_this_year())

        # Appending the row to the data list
        fake_data_list.append(fake_row)

    # Generating a list of tuples
    return [tuple(row) for row in fake_data_list]


# Generating Spark DataFrame objects with fake data
def generate_fake_dataframe(
    spark_session: SparkSession,
    schema_info: list,
    attribute_name_key: str = "Name",
    dtype_key: str = "Type",
    nullable_key: str = "nullable",
    n_rows: int = 5
) -> DataFrame:
    """Creates a Spark DataFrame with fake data using Faker.

    This function receives a list of dictionaries, each one populated with
    information about the desired attributes defined in order to create a
    Spark DataFrame with fake data. So, this list of dictionaries (schema_info
    function argument) is used to create a StructType Spark DataFrame schema
    object and this objects is used to generate fake data using Faker and based
    on the type of the attributes defined on the schema. Finally, with the
    schema object and the fake data, this function returns a Spark DataFrame
    that can be used for any purposes.

    This function calls the generate_dataframe_schema() and
    generate_fake_data_from_schema() in order to execute all the the steps
    explained above.

    Examples:
        ```python
        # Defining a list with attributes info to be used on schema creation
        schema_info = [
            {
                "Name": "idx",
                "Type": "int",
                "nullable": true
            },
            {
                "Name": "order_id",
                "Type": "string",
                "nullable": true
            }
        ]

        # Generating a Spark DataFrame object with fake data
        fake_df = generate_fake_dataframe(schema_info)
        ```

    Args:
        spark_session (SparkSession):
            A SparkSession object that is used to call createDataFrame method

        schema_info (list):
            A list with information about fields of a DataFrame. Check the
            generate_dataframe_schema() for more details.

        attribute_name_key (str):
            A string identification of the attribute name defined on every
            attribute dictionary. Check the generate_dataframe_schema() for
            more details.

        dtype_key (str):
            A string identification of the attribute type defined on every
            attribute dictionary. Check the generate_dataframe_schema() for
            more details.

        nullable_key (bool):
            A boolean flag that tells if the given attribute defined in
            the dictionary can hold null values. Check the
            generate_dataframe_schema() for more details.

        n_rows (int):
            The number of fake rows to be generated. Check the
            generate_fake_data_from_schema() for more details.

    Returns:
        A new Spark DataFrame with fake data generated by Faker providers and
        Python built-in libraries.
    """

    # Returning a valid Spark schema object based on a dictionary
    schema = generate_dataframe_schema(
        schema_info=schema_info,
        attribute_name_key=attribute_name_key,
        dtype_key=dtype_key,
        nullable_key=nullable_key
    )

    # Generating fake data based on a Spark DataFrame schema
    fake_data = generate_fake_data_from_schema(schema=schema, n_rows=n_rows)

    # Returning a fake Spark DataFrame
    return spark_session.createDataFrame(data=fake_data, schema=schema)


# Comparing Spark schemas based on custom conditions
def compare_dataframe_schemas(
    df1: DataFrame,
    df2: DataFrame,
    compare_nullable_info: bool = False
) -> bool:
    """Compares the schema from two Spark DataFrames with custom options.

    This function helps users to compare two Spark DataFrames schemas based on
    custom conditions provided in order to help the comparison.

    The schema of a Spark DataFrame is made of three main elements:
    column name, column type and a boolean information telling if the field
    accepts null values. In some cases, this third element can cause errors
    when comparing two DataFrame schemas. Imagine that a Spark DataFrame is
    created from a transformation function and there is no way to configure
    if a field accepts a null value without (think of an aggregation step that
    can create null values for some rows... or not). So, when comparing schemas
    from two DataFrames, maybe we are interested only on column names and data
    types, and not if an attribute is nullable or not.

    This function uses a flag in order to provide two ways to compare Spark
    DataFrame schemas:
        1. Removing the "nullable" info from schema
        2. Comparing native DataFrame.schema attributes without any changes

    Examples:
        ```python
        compare_dataframe_schemas(df1, df2, compare_nullable_info=False)
        # Result is True or False
        ```

    Args:
        df1 (pyspark.sql.DataFrame): the first Spark DataFrame to be compared
        df2 (pyspark.sql.DataFrame): the second Spark DataFrame to be compared
        compare_nullable_info (bool):
            a boolean flag that leads to compare the schemas including the
            nullable information or not.

    Return:
        The function returns True if both DataFrame schemas are equal or\
        False if it isn't.
    """

    # Extracting infos to be compared based on user conditions
    if not compare_nullable_info:
        df1_schema = [[col.name, col.dataType] for col in df1.schema]
        df2_schema = [[col.name, col.dataType] for col in df2.schema]
    else:
        df1_schema = df1.schema
        df2_schema = df2.schema

    # Checking if schemas are equal
    return df1_schema == df2_schema
