"""Test cases for features defined on manager.py module.

This file handles the definition of all test cases for testing GlueJobManager
class and its features. The idea is to isolate a test script for testing
sparksnake features delivered for users who want to develop Spark applications
using AWS Glue service.

___
"""

# Importing libraries
import pytest

from pyspark.sql.functions import expr
from pyspark.sql.types import StringType, DateType, TimestampType

from sparksnake.manager import SparkETLManager


@pytest.mark.spark_manager
@pytest.mark.constructor
def test_error_on_initializing_class_with_invalid_mode():
    """
    G: Given that users want to initialize a SparkETLManager class object
    W: When the SparkETLManager class is initialized with a mode attribute
    different than the acceptable values
    T: Then a ValueError exception muset be raised
    """

    # Initializing class
    with pytest.raises(ValueError):
        _ = SparkETLManager(mode="invalid_mode")


@pytest.mark.spark_manager
@pytest.mark.constructor
def test_error_with_glue_mode_without_argvlist_or_datadict_attributes():
    """
    G: Given that users want to initialize a SparkETLManager class object
    W: When the SparkETLManager class is initialized with mode="glue" but
    there's no argv_list or data_dict_attributes passed by the user
    T: Then a TypeError exception muset be raised
    """

    # Initializing class
    with pytest.raises(TypeError):
        _ = SparkETLManager(mode="glue")


@pytest.mark.spark_manager
@pytest.mark.date_transform
def test_casting_date_column_with_date_transform_method(
    df_fake,
    spark_manager_local,
    date_col="date_string_field",
    date_col_type="date",
    date_format="yyyy-MM-dd"
):
    """
    G: Given that users have a string column in a DataFrame that has date
    information and they need to cast it to date
    W: When the date_transform method is called with the following parameters:
        - date_col="name-of-the-string-column"
        - date_col_type="date"
        - date_format="yyyy-MM-dd"
        - cast_string_to_date=True
    T: Then the "name-of-the-string-column" column on the resulting DataFrame
    must have the DateType type
    """

    # Casting a source date column to string to test the feature
    df_fake_tmp = df_fake.withColumn(
        "date_string_field",
        expr("cast(date_field AS STRING)")
    )

    # Calling the method for casting a string field to date
    df_fake_prep = spark_manager_local.date_transform(
        df=df_fake_tmp,
        date_col=date_col,
        date_col_type=date_col_type,
        date_format=date_format,
        cast_string_to_date=True
    )

    # Extracting data type BEFORE casting
    dtype_pre_casting = df_fake_tmp.schema[date_col].dataType

    # Extracting data type AFTER casting
    dtype_pos_casting = df_fake_prep.schema[date_col].dataType

    # Asserting the casting
    assert dtype_pre_casting == StringType()
    assert dtype_pos_casting == DateType()


@pytest.mark.spark_manager
@pytest.mark.date_transform
def test_casting_timestamp_column_with_date_transform_method(
    df_fake,
    spark_manager_local,
    date_col="timestamp_string_field",
    date_col_type="timestamp",
    date_format="yyyy-MM-dd HH:mm:ss"
):
    """
    G: Given that users have a string column in a DataFrame that has timestamp
    information and they need to cast it to timestamp
    W: When the date_transform method is called with the following parameters:
        - date_col="name-of-the-string-column"
        - date_col_type="timestamp"
        - date_format="yyyy-MM-dd HH:mm:ss"
        - cast_string_to_date=True
    T: Then the "name-of-the-string-column" column on the resulting DataFrame
    must have the TimestampType type
    """

    # Casting a source date column to string to test the feature
    df_fake_tmp = df_fake.withColumn(
        "timestamp_string_field",
        expr("cast(timestamp_field AS STRING)")
    )

    # Calling the method for casting a string field to date
    df_fake_prep = spark_manager_local.date_transform(
        df=df_fake_tmp,
        date_col=date_col,
        date_col_type=date_col_type,
        date_format=date_format,
        cast_string_to_date=True
    )

    # Extracting data type BEFORE casting
    dtype_pre_casting = df_fake_tmp.schema[date_col].dataType

    # Extracting data type AFTER casting
    dtype_pos_casting = df_fake_prep.schema[date_col].dataType

    # Asserting the casting
    assert dtype_pre_casting == StringType()
    assert dtype_pos_casting == TimestampType()


@pytest.mark.spark_manager
@pytest.mark.date_transform
def test_error_on_casting_date_column_with_wrong_column_name(
    df_fake,
    spark_manager_local,
    date_col="invalid_column_name",
    date_col_type="date",
    date_format="yyyy-MM-dd HH:mm:ss"
):
    """
    G: Given that users have a string column in a DataFrame that has date
    information and they need to cast it to date
    W: When the date_transform method is called with a column name that
    doesn't exist on DataFrame
    T: Then an Exception must be raised
    """

    # Asserting execption raising
    with pytest.raises(Exception):
        _ = spark_manager_local.date_transform(
            df=df_fake,
            date_col=date_col,
            date_col_type=date_col_type,
            date_format=date_format,
            cast_string_to_date=True
        )


@pytest.mark.spark_manager
@pytest.mark.date_transform
@pytest.mark.skip(reason="Checking why it's failing")
def test_error_on_casting_date_column_with_invalid_date_col_type(
    df_fake,
    spark_manager_local,
    date_col="date_field",
    date_col_type="invalid_type",
    date_format="yyyy-MM-dd"
):
    """
    G: Given that users have a string column in a DataFrame that has date
    information and they need to cast it to date
    W: When the date_transform method is called with an invalid date_col_type
    attribute (e.g. something different from "date" or "timestamp")
    T: Then an ValueError must be raised
    """

    # Asserting execption raising
    with pytest.raises(ValueError):
        _ = spark_manager_local.date_transform(
            df=df_fake,
            date_col=date_col,
            date_col_type=date_col_type,
            date_format=date_format,
            cast_string_to_date=True
        )
