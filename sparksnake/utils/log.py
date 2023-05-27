"""Helps user to create and configure their logger object

This module can be used to improve observability in log messages by providing
and easy way to create and retrieve logger objects with a given configuration.

___
"""

# Importing libraries
import logging


# Creating and configuring logs
def log_config(
    logger_name: str = __file__,
    logger_level: int = logging.INFO,
    logger_date_format: str = "%Y-%m-%d %H:%M:%S"
) -> logging.Logger:
    """Logger object configuration.

    This function can be used in any Python application in order to improve log
    proccesses and enhance software observability. It uses the logging built-in
    Python package to retrieve users a basic logger object with a given
    configuration.

    Examples:
        ```python
        # Getting and configuring the logger object
        logger = log_config(logger)
        ```

    Args:
        logger_name (str): A name for the logger object
        logger_level (int): The type of the logger object
        logger_date_format (str): Message format of the logger object

    Returns:
        A preconfigured logger object
    """

    # Creating a logger object and setting its level
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)

    # Setting the format message
    log_format = "%(levelname)s;%(asctime)s;%(filename)s;"
    log_format += "%(lineno)d;%(message)s"
    formatter = logging.Formatter(log_format,
                                  datefmt=logger_date_format)

    # Setting up the stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
