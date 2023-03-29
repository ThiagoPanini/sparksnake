"""Test cases for features defined on manager.py module.

This file handles the definition of all test cases for testing SparkETLManager
class and its features. The idea is to isolate a test script for testing
sparksnake features delivered for users who want to develop Spark applications
in their local environment through a SparkETLManager class object with
mode="local".

___
"""

# Importing libraries
import pytest

from pyspark.sql.functions import expr
from pyspark.sql.types import StringType, DateType, TimestampType
from pyspark.sql.utils import AnalysisException

from sparksnake.manager import SparkETLManager


@pytest.mark.spark_manager_local
@pytest.mark.constructor
def test_error_on_initializing_class_with_invalid_mode():
    """
    G: Given that an user wants to initialize a SparkETLManager class object
    W: When the SparkETLManager class is initialized with a mode attribute
    different than the acceptable values
    T: Then a ValueError exception muset be raised
    """

    # Initializing class
    with pytest.raises(ValueError):
        _ = SparkETLManager(mode="invalid_mode")


@pytest.mark.spark_manager_local
@pytest.mark.constructor
def test_error_with_glue_mode_without_argvlist_or_datadict_attributes():
    """
    G: Given that an user wants to initialize a SparkETLManager class object
    W: When the SparkETLManager class is initialized with mode="glue" but
    there's no argv_list or data_dict_attributes passed by the user
    T: Then a TypeError exception muset be raised
    """

    # Initializing class
    with pytest.raises(TypeError):
        _ = SparkETLManager(mode="glue")


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_casting_date_column_with_date_transform_method(
    df_fake,
    spark_manager_local,
    date_col="date_string_field",
    date_col_type="date",
    date_format="yyyy-MM-dd"
):
    """
    G: Given that an user has a string column in a DataFrame that has date
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


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_casting_timestamp_column_with_date_transform_method(
    df_fake,
    spark_manager_local,
    date_col="timestamp_string_field",
    date_col_type="timestamp",
    date_format="yyyy-MM-dd HH:mm:ss"
):
    """
    G: Given that an user has a string column in a DataFrame that has timestamp
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


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_error_on_casting_date_column_with_invalid_date_col_type(
    df_fake,
    spark_manager_local,
    date_col="date_field",
    date_col_type="invalid_type",
    date_format="yyyy-MM-dd"
):
    """
    G: Given that an user has a string column in a DataFrame that has date
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


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_error_on_casting_date_column_with_invalid_column_name(
    df_fake,
    spark_manager_local,
    date_col="invalid_column_name",
    date_col_type="date",
    date_format="yyyy-MM-dd HH:mm:ss"
):
    """
    G: Given that an user has a string column in a DataFrame that has date
    information and they need to cast it to date
    W: When the date_transform method is called with a column name that
    doesn't exist on DataFrame
    T: Then an AnalysisException must be raised
    """

    # Asserting execption raising
    with pytest.raises(AnalysisException):
        _ = spark_manager_local.date_transform(
            df=df_fake,
            date_col=date_col,
            date_col_type=date_col_type,
            date_format=date_format,
            cast_string_to_date=True
        )


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_correct_col_name_after_extracting_year_info_from_date(
    spark_manager_local,
    df_fake,
    date_col="date_field"
):
    """
    G: Given that an user wants to add a new column to an existing DataFrame
       with year information based on a date column
    W: When the date_transform() method is executed with kwarg year=True
    T: Then there might be a new DataFrame column named "year_{date_col}"
       where {date_col} is the name of the target date column used to get the
       year
    """

    # Calling the date_transform() method to extract year info from a date
    df_fake_year = spark_manager_local.date_transform(
        df=df_fake,
        date_col=date_col,
        year=True
    )

    assert f"year_{date_col}" in df_fake_year.schema.fieldNames()


@pytest.mark.date_transform
def test_correct_col_value_after_extracting_year_info_from_date(
    spark_manager_local,
    df_fake,
    date_col="date_field"
):
    """
    G: Given that an user wants to add a new column to an existing DataFrame
       with year information based on a date column
    W: When the date_transform() method is executed with kwarg year=True
    T: Then the new year_{date_col} field must have the expected year
       information extracted from {date_col}
    """

    # Calling the date_transform() method to extract year info from a date
    df_fake_year = spark_manager_local.date_transform(
        df=df_fake,
        date_col=date_col,
        year=True
    )

    # Getting the expected field value
    expected_value = df_fake.select(date_col).take(1)[0][0].year

    # Getting the generated value
    current_value = df_fake_year.select(f"year_{date_col}").take(1)[0][0]

    assert current_value == expected_value


@pytest.mark.spark_manager_local
@pytest.mark.date_transform
def test_correct_col_names_after_extracting_all_date_information(
    spark_manager_local,
    df_fake,
    date_col="date_field"
):
    """
    G: Given that an user wants to add a new column to an existing DataFrame
       with all possible date information based on a date column
    W: When the date_transform() method is executed with kwargs year=True,
       quarter=True, month=True, dayofmonth=True, dayofweek=True,
       dayofyear=True, weekofyear=True
    T: Then, for each aforementioned kwarg, there might be a new column
       named "{kwarg}_{date_col}" where {kwarg} is the date information
       to be extracted and {date_col} is the name of the target date column
    """

    # Calling the date_transform() method to extract year info from a date
    df_fake_date = spark_manager_local.date_transform(
        df=df_fake,
        date_col=date_col,
        year=True,
        quarter=True,
        month=True,
        dayofmonth=True,
        dayofweek=True,
        dayofyear=True,
        weekofyear=True
    )

    # Extracting field names from the new DataFrame
    new_field_names = df_fake_date.schema.fieldNames()

    # Creating a list of expected field names to be included
    date_kwargs = ["year", "quarter", "month", "dayofmonth", "dayofweek",
                   "dayofyear", "weekofyear"]
    expected_field_names = [f"{d}_{date_col}" for d in date_kwargs]

    assert all(d in new_field_names for d in expected_field_names)


@pytest.mark.spark_manager_local
@pytest.mark.agg_data
def test_correct_col_names_after_aggregating_data_with_all_possible_functions(
    spark_manager_local,
    df_fake,
    spark_session,
    numeric_col="integer_field",
    group_by="boolean_field"
):
    """
    G: Given that an user wants to aggregate data from a DataFrame
    W: When the agg_data() method is called to aggregate data using all
       possible functions available on method
    T: Then there might new columns with named matching the pattern
       {function}_{numeric_col} where {function} is the aggreaget function flag
       passed on kwargs and {numeric_col} is the numeric col parameter passed
       on method call
    """

    # Creating a kwargs dict with all possible functions to aggregate
    agg_functions = ["sum", "mean", "max", "min", "count", "variance",
                     "stddev", "kurtosis", "skewness"]
    kwargs_dict = {f: True for f in agg_functions}

    # Aggregating data
    df_agg = spark_manager_local.agg_data(
        spark_session=spark_session,
        df=df_fake,
        numeric_col=numeric_col,
        group_by=group_by,
        round_result=True,
        n_round=2,
        **kwargs_dict
    )

    # Creating a list of expected column names to be in df_agg DataFrame
    expected_columns = [f"{f}_{numeric_col}" for f in agg_functions]

    assert all(c in df_agg.schema.fieldNames() for c in expected_columns)


@pytest.mark.spark_manager_local
@pytest.mark.agg_data
def test_columns_on_groupby_list_are_part_of_schema_after_aggregating_data(
    spark_manager_local,
    df_fake,
    spark_session,
    numeric_col="integer_field",
    group_by=["string_field", "boolean_field"]
):
    """
    G: Given that an user wants to aggregate data from a DataFrame
    W: When the agg_data() method is called to aggregate data using a list of
       columns on group_by argument and any agg function
    T: Then the group_by list must be on the returned DataFrame schema
    """

    # Aggregating data
    df_agg = spark_manager_local.agg_data(
        spark_session=spark_session,
        df=df_fake,
        numeric_col=numeric_col,
        group_by=group_by,
        sum=True
    )

    # Checking if group_by list are in schema
    assert all(c in df_agg.schema.fieldNames() for c in group_by)


@pytest.mark.spark_manager_local
@pytest.mark.agg_data
def test_aggregating_data_with_sum_function_returns_expected_value(
    spark_manager_local,
    df_fake,
    spark_session,
    numeric_col="integer_field",
    group_by="boolean_field"
):
    """
    G: Given that an user wants to aggregate data from a DataFrame
    W: When the agg_data() method is called to aggregate data with sum
    T: Then sum_{numeric_col} column value must match the expected
    """

    # Aggregating data
    df_agg = spark_manager_local.agg_data(
        spark_session=spark_session,
        df=df_fake,
        numeric_col=numeric_col,
        group_by=group_by,
        sum=True
    )

    # Collecting rows for the raw DataFrame to be tested
    value_rows = df_fake.where(expr("boolean_field == True"))\
        .select(numeric_col).collect()

    # Applying the sum on the result rows using Python
    expected_result = sum([r[0] for r in value_rows])

    # Collecting the result from the DataFrame after agg_data() method
    agg_result = df_agg.where(expr("boolean_field == True"))\
        .select(f"sum_{numeric_col}").collect()[0][0]

    assert agg_result == expected_result


@pytest.mark.spark_manager_local
@pytest.mark.agg_data
def test_error_on_calling_agg_data_method_with_invalid_numeric_col_reference(
    spark_manager_local,
    df_fake,
    spark_session,
    numeric_col="invalid_col",
    group_by="boolean_field"
):
    """
    G: Given that an user wants to aggregate data from a DataFrame
    W: When the agg_data() method is called with a column reference for
       numeric_col argument that doesn't exist on the target DataFrame
    T: Then a AnalysisException error must be raised
    """

    # Trying to aggregate with invalid numeric_col argument
    with pytest.raises(AnalysisException):
        _ = spark_manager_local.agg_data(
            spark_session=spark_session,
            df=df_fake,
            numeric_col=numeric_col,
            group_by=group_by
        )


@pytest.mark.spark_manager_local
@pytest.mark.agg_data
def test_error_on_calling_agg_data_method_with_invalid_group_by_reference(
    spark_manager_local,
    df_fake,
    spark_session,
    numeric_col="integer_field",
    group_by="invalid_col"
):
    """
    G: Given that an user wants to aggregate data from a DataFrame
    W: When the agg_data() method is called with a column reference for
       group_by argument that doesn't exist on the target DataFrame
    T: Then a AnalysisException error must be raised
    """

    # Trying to aggregate with invalid numeric_col argument
    with pytest.raises(AnalysisException):
        _ = spark_manager_local.agg_data(
            spark_session=spark_session,
            df=df_fake,
            numeric_col=numeric_col,
            group_by=group_by
        )


@pytest.mark.spark_manager_local
@pytest.mark.add_partition_column
def test_correct_col_name_after_adding_partition_with_add_partition_method(
    spark_manager_local,
    df_fake,
    partition_name="execution_date",
    partition_value=0
):
    """
    G: Given that an user wants to add a new partition column to a DataFrame
    W: When the add_partition_column() method is called
    T: Then the partition column name must exists on the returned DataFrame
    """

    # Adding a partition column
    df_fake_partitioned = spark_manager_local.add_partition_column(
        df=df_fake,
        partition_name=partition_name,
        partition_value=partition_value
    )

    assert partition_name in df_fake_partitioned.schema.fieldNames()


@pytest.mark.spark_manager_local
@pytest.mark.add_partition_column
def test_correct_col_value_after_adding_partition_with_add_partition_method(
    spark_manager_local,
    df_fake,
    partition_name="execution_date",
    partition_value=0
):
    """
    G: Given that an user wants to add a new partition column to a DataFrame
    W: When the add_partition_column() method is called
    T: Then the partition column value must match the partition_value argument
    """

    # Adding a partition column
    df_fake_partitioned = spark_manager_local.add_partition_column(
        df=df_fake,
        partition_name=partition_name,
        partition_value=partition_value
    )

    assert df_fake_partitioned.select(partition_name).take(1)[0][0] == 0


@pytest.mark.spark_manager_local
@pytest.mark.add_partition_column
def test_error_on_calling_add_partition_method_with_invalid_partition_name(
    spark_manager_local,
    df_fake,
    partition_name=0,
    partition_value=0
):
    """
    G: Given that an user wants to add a new partition column to a DataFrame
    W: When the add_partition_column() method is called with an invalid value
       for partition_name argument (e.g. an integer number instead of a string
       column name reference)
    T: Then an Exception must be raised
    """

    # Trying to execute the method and caughting exception
    with pytest.raises(Exception):
        _ = spark_manager_local.add_partition_column(
            df=df_fake,
            partition_name=partition_name,
            partition_value=partition_value
        )


@pytest.mark.spark_manager_local
@pytest.mark.repartition_dataframe
def test_decreasing_dataframe_partitions_with_repartition_method(
    spark_manager_local,
    df_fake
):
    """
    G: Given that an user wants to decrease the number of partitions in a
       Spark DataFrame
    W: When the repartition_dataframe() method is called with a number of
       desired partitions on num_partition arg that is LESS THAN the current
       number of DataFrame partitions
    T: Then the returned DataFrame must have the desired number of partitions
    """

    # Getting the current partitions number and setting a desired number
    current_partitions = df_fake.rdd.getNumPartitions()
    partitions_to_set = current_partitions // 2

    # Calling the repartition method
    df_repartitioned = spark_manager_local.repartition_dataframe(
        df=df_fake,
        num_partitions=partitions_to_set
    )

    assert df_repartitioned.rdd.getNumPartitions() == partitions_to_set


@pytest.mark.spark_manager_local
@pytest.mark.repartition_dataframe
def test_increasing_dataframe_partitions_with_repartition_method(
    spark_manager_local,
    df_fake
):
    """
    G: Given that an user wants to increase the number of partitions in a
       Spark DataFrame
    W: When the repartition_dataframe() method is called with a number of
       desired partitions on num_partition arg that is MORE THAN the current
       number of DataFrame partitions
    T: Then the returned DataFrame must have the desired number of partitions
    """

    # Getting the current partitions number and setting a desired number
    current_partitions = df_fake.rdd.getNumPartitions()
    partitions_to_set = current_partitions * 2

    # Calling the repartition method
    df_repartitioned = spark_manager_local.repartition_dataframe(
        df=df_fake,
        num_partitions=partitions_to_set
    )

    assert df_repartitioned.rdd.getNumPartitions() == partitions_to_set


@pytest.mark.spark_manager_local
@pytest.mark.repartition_dataframe
def test_trying_to_repartition_dataframe_with_current_number_of_partitions(
    spark_manager_local,
    df_fake
):
    """
    G: Given that an user wants to change the number of partitions in a
       Spark DataFrame
    W: When the repartition_dataframe() method is called with a number of
       desired partitions on num_partition arg that is EQUAL to the current
       number of DataFrame partitions
    T: Then no repartition operation should be executed and the returned
       DataFrame must have the same number of partitions as before
    """

    # Getting the current partitions number and setting a desired number
    current_partitions = df_fake.rdd.getNumPartitions()
    partitions_to_set = current_partitions

    # Calling the repartition method
    df_repartitioned = spark_manager_local.repartition_dataframe(
        df=df_fake,
        num_partitions=partitions_to_set
    )

    assert df_repartitioned.rdd.getNumPartitions() == partitions_to_set


@pytest.mark.spark_manager_local
@pytest.mark.repartition_dataframe
def test_trying_to_repartition_dataframe_with_negative_number_of_partitions(
    spark_manager_local,
    df_fake
):
    """
    G: Given that an user wants to change the number of partitions in a
       Spark DataFrame
    W: When the repartition_dataframe() method is called with a negative
       number of desired partitions on num_partition argument
    T: Then no operation should be done and the same DataFrame must be
       returned with no changes in its number of partitions
    """

    # Getting the current partitions number and setting a desired number
    current_partitions = df_fake.rdd.getNumPartitions()
    partitions_to_set = -1

    # Calling the repartition method
    df_repartitioned = spark_manager_local.repartition_dataframe(
        df=df_fake,
        num_partitions=partitions_to_set
    )

    assert df_repartitioned.rdd.getNumPartitions() == current_partitions
