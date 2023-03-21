"""Confest file for managing pytest fixtures and other components.

This file will handle essential components and elements to be used on test
scripts along the project, like features and other things.

___
"""

# Importing libraries
import sys
import pytest

from sparksnake.manager import GlueJobManager

from tests.helpers.user_inputs import FAKE_ARGV_LIST, FAKE_DATA_DICT


# A GlueJobManager objects
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
