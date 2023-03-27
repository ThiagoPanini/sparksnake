"""Test cases for features defined on glue.py module.

This file handles the definition of all test cases for testing GlueJobManager
class and its features. The idea is to isolate a test script for testing
sparksnake features delivered for users who want to develop Spark applications
using AWS Glue service.

___
"""

# Importing libraries
import pytest
import logging

from tests.helpers.user_inputs import FAKE_ARGV_LIST

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job


@pytest.mark.glue_job_manager
def test_type_of_args_class_attribute_is_dictionary(
    job_manager
):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when the class GlueJobManager is called to create an object
    T: then args attribute created must be of dict type
    """

    assert type(job_manager.args) is dict


@pytest.mark.glue_job_manager
def test_args_class_attribute_has_user_defined_arguments(
    job_manager
):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when the class GlueJobManager is called to create an object
    T: then the arguments previous defined by user on argv_list must be into
       self.args dictionary attribute
    """

    # Getting argument job names
    job_arg_names = list(job_manager.args.keys())

    assert all(a in job_arg_names for a in FAKE_ARGV_LIST)


@pytest.mark.glue_job_manager
@pytest.mark.job_inital_log_message
def test_content_of_initial_log_message_match_expected(
    job_manager, caplog
):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when the job_initial_message() is called
    T: then the log message captured must match the expected value based on
       data_dict attribute used on object creation from GlueJobManager class
    """

    # Degining the expected log message based on data_dict_dictionary used
    expected_log_msg = "Initializing the execution of a-fake-arg-value job. "\
        "Data sources used in this ETL process:\n\n"\
        "Table some-fake-database.orders-fake-table without push down "\
        "predicate\n"\
        "Table some-fake-database.customers-fake-table with the following "\
        "push down predicate info: anomesdia=20221201\n"

    # Calling the method for logging the initial message
    with caplog.at_level(logging.INFO):
        job_manager.job_initial_log_message()

    assert caplog.records[-1].message == expected_log_msg


@pytest.mark.glue_job_manager
@pytest.mark.print_args
def test_print_args_method_really_prints_job_arguments_defined_by_user(
    job_manager, caplog
):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when the print_args() is called
    T: then the log message captured must have the arguments defined by user
       on self.argv_list class attribute
    """

    # Creating a list for the expected message based on user defined job args
    expected_argv_msg = [f'--{arg}="a-fake-arg-value"'
                         for arg in FAKE_ARGV_LIST]

    # Calling the method
    with caplog.at_level(logging.INFO):
        job_manager.print_args()

    assert all(a in caplog.text.split("\n") for a in expected_argv_msg)


@pytest.mark.glue_job_manager
@pytest.mark.get_context_and_session
def test_getting_context_and_session_elements(job_manager):
    """
    G: given that users want to get the context and session elements
    W: when method get_context_and_session() is called
    T: then the object class must have new attributes identified as
       self.sc, self.glueContext and self.spark
    """

    # Calling the method for getting context and session elements
    job_manager.get_context_and_session()

    # Getting class attributes and defining an assertion list
    class_attribs = job_manager.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Asserting class has now new attributes
    assert all(a in list(class_attribs.keys()) for a in attribs_names)


@pytest.mark.glue_job_manager
@pytest.mark.get_context_and_session
def test_type_of_context_and_session_elements_match_expected(job_manager):
    """
    G: given that users want to get the context and session elements
    W: when method get_context_and_session() is called
    T: then the self.sc must be of type SparkContext, the self.glueContext
       attrib must be of type GlueContext and the self.spark attribute must be
       of type SparkSession
    """

    # Calling the method for getting context and session elements
    job_manager.get_context_and_session()

    # Getting class attributes dictionary
    class_attribs = job_manager.__dict__

    # Asserting type of each element
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.glue_job_manager
@pytest.mark.init_job
def test_init_job_method_generates_context_and_session_elements(job_manager):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when method init_job() is called
    T: then context and session elements from Glue and Spark must exist and
       its types must match
    """

    # Calling method for job initialization
    job_manager.init_job()

    # Getting class attribs and setting up a assertion list
    class_attribs = job_manager.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Asserting if the new attributes really exist
    assert all(a in list(class_attribs.keys()) for a in attribs_names)

    # Asserting if types match
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.glue_job_manager
@pytest.mark.init_job
def test_init_job_methods_generates_a_job_type_attribute(job_manager):
    """
    G: given that users want to initialize a Glue job on AWS
    W: when method init_job() is called
    T: then a new class attribute self.job of type awsglue.jobJob must exists
    """

    # Calling method for job initialization
    job_manager.init_job()

    # Getting class attributes
    class_attribs = job_manager.__dict__

    assert "job" in class_attribs.keys()
    assert type(class_attribs["job"]) is Job
