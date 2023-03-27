"""Confest file for managing pytest fixtures and other components.

This file will handle essential components and elements to be used on test
scripts along the project, like features and other things.

___
"""

# Importing libraries
import sys
import pytest

from sparksnake.glue import GlueJobManager
from sparksnake.manager import SparkETLManager

from tests.helpers.faker import fake_dataframe
from tests.helpers.user_inputs import FAKE_ARGV_LIST, FAKE_DATA_DICT,\
    FAKE_SCHEMA_DTYPES

from pyspark.sql import SparkSession, DataFrame


# Defining a SparkSession object
spark = SparkSession.builder\
    .appName("sparksnake-conftest-file")\
    .getOrCreate()


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


# A GlueJobManager class object
@pytest.fixture()
def job_manager() -> GlueJobManager:
    # Adding system args
    for fake_arg in FAKE_ARGV_LIST:
        sys.argv.append(f"--{fake_arg}=a-fake-arg-value")

    # Initializing a class object
    job_manager = GlueJobManager(
        argv_list=FAKE_ARGV_LIST,
        data_dict=FAKE_DATA_DICT
    )

    return job_manager


# A fake Spark DataFrame object
@pytest.fixture()
def df_fake() -> DataFrame:
    return fake_dataframe(spark, FAKE_SCHEMA_DTYPES)
