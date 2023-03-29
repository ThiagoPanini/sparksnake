"""Test cases for features defined on glue.py module.

This file handles the definition of all test cases for testing GlueJobManager
class and its features. The idea is to isolate a test script for testing
sparksnake features delivered for users who want to develop Spark applications
using AWS Glue environment through a SparkETLManager class object with
mode="glue".

___
"""

# Importing libraries
import pytest
import logging

from sparksnake.manager import SparkETLManager

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job


@pytest.mark.spark_manager_glue
@pytest.mark.constructor
def test_error_on_using_glue_mode_and_not_providing_essential_attributes():
    """
    G: Given that an user wants to initialize a Glue job on AWS
    W: When the class SparkETLManager is initialized with mode="glue" but
       without argv_list or data_dict attributes on object construction
    T: Then a TypeError exception must be raised
    """

    # Trying to initialize SparkETLManager with glue mode and without argv_list
    with pytest.raises(TypeError):
        _ = SparkETLManager(mode="glue")


@pytest.mark.spark_manager_glue
@pytest.mark.constructor
def test_args_attribute_for_class_on_glue_mode_must_be_dict(
    spark_manager_glue
):
    """
    G: Given that an user wants to initialize a Glue job on AWS
    W: When an object of class SparkETLManager is initialized with
       mode="glue"
    T: Then the self.args class attribute must be a Python dictionary
    """

    assert type(spark_manager_glue.args) is dict


@pytest.mark.spark_manager_glue
@pytest.mark.constructor
def test_args_attribute_contains_user_defined_arguments(
    spark_manager_glue
):
    """
    G: Given that a user wants to initialize a Glue job on AWS
    W: When the class SparkETLManager is initialized with mode="glue"
    T: Then the arguments passed on argv_list parameter must be contained on
       self.args class attribute
    """

    # Getting argument job names
    job_arg_names = list(spark_manager_glue.args.keys())

    assert all(a in job_arg_names for a in spark_manager_glue.argv_list)


@pytest.mark.spark_manager_glue
@pytest.mark.job_initial_log_message
def test_content_of_initial_log_message_match_the_expected(
    spark_manager_glue,
    caplog
):
    """
    G: Given that a user wants to initialize a Glue job on AWS
    W: When the job_initial_message() is called from SparkETLManager with
       glue mode
    T: Then the log message captured must match a expected value
    """

    # Defining the expected log message based on data_dict_dictionary used
    expected_log_msg = "Initializing the execution of a-fake-arg-value job. "\
        "Data sources used in this ETL process:\n\n"\
        "Table some-fake-database.orders-fake-table without push down "\
        "predicate\n"\
        "Table some-fake-database.customers-fake-table with the following "\
        "push down predicate info: anomesdia=20221201\n"

    # Calling the method for logging the initial message
    with caplog.at_level(logging.INFO):
        spark_manager_glue.job_initial_log_message()

    assert caplog.records[-1].message == expected_log_msg


@pytest.mark.spark_manager_glue
@pytest.mark.print_args
def test_print_args_method_really_prints_job_arguments_defined_by_user(
    spark_manager_glue,
    caplog
):
    """
    G: Given that a user wants to initialize a Glue job on AWS
    W: When the print_args() is called from SparkETLManager with glue mode
    T: Then the log message captured must have the arguments defined by user
       on self.argv_list class attribute
    """

    # Creating a list for the expected message based on user defined job args
    expected_argv_msg = [f'--{arg}="a-fake-arg-value"'
                         for arg in spark_manager_glue.argv_list]

    # Calling the method
    with caplog.at_level(logging.INFO):
        spark_manager_glue.print_args()

    assert all(a in caplog.text.split("\n") for a in expected_argv_msg)


@pytest.mark.spark_manager_glue
@pytest.mark.get_context_and_session
def test_get_context_and_session_method_generates_context_and_session(
    spark_manager_glue
):
    """
    G: Given that a user wants to get the context and session elements
    W: When method get_context_and_session() is called from SparkETLManager
       with glue mode
    T: Then the object class must have new attributes identified as
       self.sc, self.glueContext and self.spark
    """

    # Calling the method for getting context and session elements
    spark_manager_glue.get_context_and_session()

    # Getting class attributes and defining an assertion list
    class_attribs = spark_manager_glue.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Asserting class has now new attributes
    assert all(a in list(class_attribs.keys()) for a in attribs_names)


@pytest.mark.spark_manager_glue
@pytest.mark.get_context_and_session
def test_correct_type_of_context_and_session_elements_gotten_in_class(
    spark_manager_glue
):
    """
    G: Given that users want to get the context and session elements
    W: When method get_context_and_session() is called from SparkETLManager
       with glue mode
    T: Then the self.sc must be of type SparkContext, the self.glueContext
       attrib must be of type GlueContext and the self.spark attribute must be
       of type SparkSession
    """

    # Calling the method for getting context and session elements
    spark_manager_glue.get_context_and_session()

    # Getting class attributes dictionary
    class_attribs = spark_manager_glue.__dict__

    # Asserting type of each element
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.spark_manager_glue
@pytest.mark.init_job
def test_init_job_method_generates_context_and_session_elements(
    spark_manager_glue
):
    """
    G: Given that users want to initialize a Glue job on AWS
    W: When method init_job() is called from SparkETLManager with glue mode
    T: Then context and session elements from Glue and Spark must exist and
       its types must match
    """

    # Calling method for job initialization
    spark_manager_glue.init_job()

    # Getting class attribs and setting up a assertion list
    class_attribs = spark_manager_glue.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Asserting if the new attributes really exist
    assert all(a in list(class_attribs.keys()) for a in attribs_names)

    # Asserting if types match
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.spark_manager_glue
@pytest.mark.init_job
def test_init_job_methods_generates_a_job_type_attribute(spark_manager_glue):
    """
    G: Given that users want to initialize a Glue job on AWS
    W: When method init_job() is called from SparkETLManager with glue mode
    T: Then a new class attribute self.job of type awsglue.job.Job must exists
    """

    # Calling method for job initialization
    spark_manager_glue.init_job()

    # Getting class attributes
    class_attribs = spark_manager_glue.__dict__

    assert "job" in class_attribs.keys()
    assert type(class_attribs["job"]) is Job
