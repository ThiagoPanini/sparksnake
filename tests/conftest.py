"""Confest file for managing pytest fixtures and other components.

This file will handle essential components and elements to be used on test
scripts along the project, like features and other things.

___
"""

# Importing libraries
import sys
import pytest

from sparksnake.manager import SparkETLManager
from sparksnake.tester.dataframes import generate_fake_dataframe

from tests.helpers.user_inputs import FAKE_ARGV_LIST, FAKE_DATA_DICT,\
    FAKE_SCHEMA_INFO

from pyspark.sql import SparkSession, DataFrame


# Getting the active SparkSession object (or creating one)
spark = SparkSession.builder.getOrCreate()


# Returning the SparkSession object as a fixture
@pytest.fixture()
def spark_session(spark=spark) -> SparkSession:
    return spark


# A SparkETLManager class object with mode="local"
@pytest.fixture()
def spark_manager_local() -> SparkETLManager:
    return SparkETLManager(mode="local")


# A GlueJobManager class object
@pytest.fixture()
def spark_manager_glue() -> SparkETLManager:
    # Adding system args
    for fake_arg in FAKE_ARGV_LIST:
        sys.argv.append(f"--{fake_arg}=a-fake-arg-value")

    # Initializing a class object
    return SparkETLManager(
        mode="glue",
        argv_list=FAKE_ARGV_LIST,
        data_dict=FAKE_DATA_DICT
    )


# A fake Spark DataFrame object
@pytest.fixture()
def df_fake(spark_session) -> DataFrame:
    return generate_fake_dataframe(
        spark_session=spark_session,
        schema_info=FAKE_SCHEMA_INFO
    )
