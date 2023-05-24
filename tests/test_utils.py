"""Test cases for features defined on utils.py module.

This file handles the definition of all test cases for testing everything on
sparksnake's utils module.

___
"""

# Importing libraries
import pytest
import logging

from sparksnake.utils.log import log_config


@pytest.mark.utils
def test_log_config_function_returns_a_logger_object():
    """
    G: Given that users want to obtain a preconfigured logger object
    W: When the log_config functions is called from sparksnake.utils module
    T: Then the returned object must be a logging.Logger object
    """

    logger = log_config()
    assert type(logger) == logging.Logger
