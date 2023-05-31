"""Centralizing all user inputs for helping on fixtures and test cases.

This file aims to put together all variables used on fixture definitions and
test cases that requires user inputs as a way to configure or validate
something.

The idea behind this file is to have everything related to user inputs on
test cases in a single place. This makes easier to handle, give maintenance,
support and improvements for building new test cases.

___
"""

# A fake argument list for creating Glue jobs
FAKE_ARGV_LIST = ["JOB_NAME", "S3_SOURCE_PATH", "S3_OUTPUT_PATH"]

# A fake data dictionary for setting up data sources
FAKE_DATA_DICT = {
    "orders": {
        "database": "some-fake-database",
        "table_name": "orders-fake-table",
        "transformation_ctx": "dyf_orders"
    },
    "customers": {
        "database": "some-fake-database",
        "table_name": "customers-fake-table",
        "transformation_ctx": "dyf_customers",
        "push_down_predicate": "anomesdia=20221201",
        "create_temp_view": True,
        "additional_options": {
            "compressionType": "lzo"
        }
    }
}

# A fake DataFrame schema object to create Spark DataFrames for test purposes
FAKE_SCHEMA_INFO = [
    {
        "Name": "string_field",
        "Type": "string",
        "nullable": True
    },
    {
        "Name": "integer_field",
        "Type": "int",
        "nullable": True
    },
    {
        "Name": "long_field",
        "Type": "long",
        "nullable": True
    },
    {
        "Name": "decimal_field",
        "Type": "decimal",
        "nullable": True
    },
    {
        "Name": "float_field",
        "Type": "float",
        "nullable": True
    },
    {
        "Name": "double_field",
        "Type": "double",
        "nullable": True
    },
    {
        "Name": "boolean_field",
        "Type": "boolean",
        "nullable": True
    },
    {
        "Name": "date_field",
        "Type": "date",
        "nullable": True
    },
    {
        "Name": "timestamp_field",
        "Type": "timestamp",
        "nullable": False
    }
]
