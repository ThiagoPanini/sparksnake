"""Handling operations to fake data in order to help users in their test cases.

This module puts together some useful functions created to provide an easy way
to fake Spark DataFrames. Its functions can be imported and applied in order
to create fixtures in a conftest.py file or in any test script that needs a
fake DataFrame.

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
        schema = create_spark_schema_from_dict(schema_info)
        ```

    Args:
        schema_info (list):
            A list with information about fields of a DataFrame
        
        attribute_name_key (str):
            A string identification of the attribute name defined on every
            attribute dictionary

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
    num_rows: int = 5
) -> tuple:
    """
    """

    # Creating fake data based on each schema attribute
    fake_data_list = []
    for _ in range(num_rows):
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


def fake_dataframe(
    spark_session: SparkSession,
    schema_info: list,
    attribute_name_key: str = "Name",
    dtype_key: str = "Type",
    nullable_key: str = "nullable",
    num_rows: int = 5
) -> DataFrame:
    """Creates a Spark DataFrame with fake data using Faker.

    This function receives a list of Spark data types imported from
    pyspark.sql.types to create a schema with column names based on the
    data type name (typeName()) and fake data using faker providers.

    Examples:
        ```python
        # Importing classes
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType,\
            IntegerType, DecimalType, DateType, TimestampType, BooleanType

        from tests.helper.faker import fake_dataframe

        # Creating a list with all types to be used on faking a Spark DataFrame
        dtypes_list = [
            StringType, IntegerType, DecimalType, DateType, TimestampType,
            BooleanType
        ]

        # Calling function to create a fake Spark DataFrame
        df_fake = fake_dataframe(spark, dtypes_list)
        ```

    Args:
        spark_session (SparkSession):
            A SparkSession object for calling the createDataFrame method.

        dtypes_list (list):
            A list with Spark data types to be used as schema for the
            fake DataFrame to be created.

        num_rows (int):
            Total number of rows of the fake DataFrame.

    Returns:
        A new Spark DataFrame with fake data generated by Faker providers
    """

    # Extracting kwargs for customizing function calls
    

    # Creating a schema with fields based on data types provided on the list
    schema = generate_dataframe_schema(
        schema_info=schema_info
    )


    # Returning a fake Spark DataFrame
    # return spark_session.createDataFrame(data=fake_data, schema=fake_schema)
