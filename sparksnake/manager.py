"""Managing Spark operations in AWS services that uses it in ETL jobs.

This module provides Spark features do be applied in Spark applications to
be deployed locally or using AWS services like Glue and EMR.

___
"""

# Importing libraries
from time import sleep

from sparksnake.utils.log import log_config

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, lit
from pyspark.sql.utils import AnalysisException

try:
    from sparksnake.glue import GlueJobManager as ManagerClass
except ImportError:
    class ManagerClass:
        pass

# Setting up a logger object
logger = log_config(logger_name=__file__)


class SparkETLManager(ManagerClass):
    """Puts together all Spark features used in ETL jobs in AWS.

    This class provides an easy and fast way for users to improve and
    enhance the development of their Spark applications. The class makes it
    possible through a series of features (attributes and methods) specially
    coded for making the development journey something really simple and fun.

    According to the AWS service to be used by the user to run their Spark
    application, this class can be configured through its "mode" attribute.
    Technically, it means that the "mode" attribute literally handles all
    class inheritance processes based on target AWS service to be used.

    So, users can have available special attributes and methods for improving
    their development process wherever they are creating a Spark application
    for running on AWS Glue, Amazon EMR or even locally.

    Example: A basic usage example of class `SparkETLManager` with mode="glue"
        ```python
        # Importing class
        from sparksnake.manager import SparkETLManager

        # Defining job arguments
        ARGV_LIST = ["JOB_NAME", "S3_OUTPUT_PATH"]

        # Defining dictionary of data sources to be used on job
        DATA_DICT = {
            "orders": {
                "database": "ra8",
                "table_name": "orders",
                "transformation_ctx": "dyf_orders"
            },
            "customers": {
                "database": "ra8",
                "table_name": "customers",
                "transformation_ctx": "dyf_customers",
                "push_down_predicate": "anomesdia=20221201",
                "create_temp_view": True,
                "additional_options": {
                    "compressionType": "lzo"
                }
            }
        }

        # Creating a class object on initializing a glue job
        spark_manager = SparkETLManager(
            mode="glue",
            argv_list=ARGV_LIST,
            data_dict=DATA_DICT
        )

        spark_manager.init_job()

        # Getting all DataFrames Spark based on data_dict provided
        dfs_dict = spark_manager.generate_dataframes_dict()

        # Indexing a DataFrame from the dictionary
        df_orders = dfs_dict["orders"]

        # Dropping a partition on S3 (if exists)
        spark_manager.drop_partition(
            s3_partition_uri="s3://some-bucket-name/some-table-name/partition/"
        )

        # Adding a partition column into the DataFrame
        df_orders_partitioned = spark_manager.add_partition_column(
            partition_name="anomesdia",
            partition_value="20230101"
        )

        # Applying a repartition method for storage optimization
        df_orders_repartitioned = spark_manager.repartition_dataframe(
            df=df_orders_partitioned,
            num_partitions=10
        )

        # Writing data on S3 and cataloging it on Data Catalog
        spark_manager.write_data_to_catalog(df=df_orders_repartitioned)

        # Job commit
        spark_manager.job.commit()
        ```

    Args:
        mode (string):
            Operation mode for the class. It handles inheritance from other
            classes based on this library so the `SparkETLManager` class can
            expand its features for a Spark application development in
            specific scenarios.
            Acceptable values are: "glue", "emr", "local".

    Tip: The "mode" attribute may not be the only one
        By its own construction, the class `SparkETLManager`inherits attributes
        and methods from other classes in the library. It means that these
        other classes may have its own attributes to be passed on class
        construction.

        For example, when users choose to set the Glue operation mode
        (`mode="glue"`) on starting up the class `SparkETLManager`, a class
        inheritance from `GlueJobManager` is set under the hood. Because of
        that, attributes like `argv_list` and `data_dict` must be passed
        together with `mode` as they are mandatory on `GlueJobManager` class
        construction.

        For a special tip, check the construction of the class to be inherited
        based on mode operation to see which attributes are necessary to pass
        at `SparkETLManager` class start up.
    """

    def __init__(self, mode: str, **kwargs) -> None:
        self.mode = mode.strip().lower()

        # Checking if the operation mode value was passed as expected
        if self.mode not in ("local", "glue", "emr"):
            raise ValueError(f"The attribute mode was set as {self.mode} "
                             "but acceptable values are 'local', 'glue' and "
                             'emr')

        # Looking out for glue operation mode
        if self.mode == "glue":
            # Checking if required args for GlueJobManager were passed
            if "argv_list" not in kwargs or "data_dict" not in kwargs:
                raise TypeError("The operation mode was set as 'glue' but "
                                "'argv_list' and/or 'data_dict' required "
                                "arguments weren't set properly.")

            # Collecting required args for mode="glue"
            argv_list = kwargs["argv_list"]
            data_dict = kwargs["data_dict"]

            # Applying class inheritance for this mode
            try:
                ManagerClass.__init__(self, argv_list=argv_list,
                                      data_dict=data_dict)
            except TypeError as te:
                logger.error("Error on inherting class GlueJobManager. Check "
                             "if your environment has the awsglue libraries "
                             "and try again. If you don't have awsglue libs "
                             "available, you probably want to run sparksnake "
                             "in a local operation mode. If this is the case, "
                             "change the mode attribute to 'local'")
                raise te

            # Logging initialization message
            logger.info("The class was succesfully initialized with Glue "
                        "operation mode and all Glue sparksnake features were "
                        "inherited and are now available")

        # Looking out for local mode
        elif self.mode == "local":
            # Checking if user wants to pass its own SparkSession object
            if "spark" in kwargs and type(kwargs["spark"]) is SparkSession:
                self.spark = kwargs["spark"]
            else:
                logger.info("Creating a SparkSession object to be used in a "
                            "local environment")
                self.spark = SparkSession.builder\
                    .appName("sparksnake-app")\
                    .getOrCreate()

            # Logging initialization message
            logger.info("The class was succesfully initialized with 'local' "
                        "operation mode")

    @staticmethod
    def date_transform(df: DataFrame,
                       date_col: str,
                       date_col_type: str = "date",
                       date_format: str = "yyyy-MM-dd",
                       cast_string_to_date: bool = True,
                       **kwargs) -> DataFrame:
        """Extracting date attributes from a Spark DataFrame date column.

        This method makes it possible to extract multiple date attributes from
        a column in a Spark DataFrame that represents a date or timestamp
        information. To do that, the given date column in the DataFrame should
        be of types DATE, TIMESTAMP. There is a special case where the target
        date column is of STRING data type. In that situtation, the column
        must be parseable as a date so the date attributes can be extracted
        from it.

        The method logic is made for a initial field casting validation
        followed by the extraction of all date attributes according to the user
        input on the method call. The big idea behing this features is the
        ability to provide users a huge DataFrame enrichment with a single
        method call to improve their analytics processes.

        Examples:
            ```python
            # Extracting date attributes from a date column in a Spark df
            df_date_prep = spark_manager.date_transform(
                df=df_raw,
                date_col="order_date",
                date_col_type="timestamp",
                year=True,
                month=True,
                dayofmonth=True
            )

            # In the above example, the method will return a new DataFrame
            # with additional columns based on the order_date_content, such as:
            # year_order_date, month_order_date and dayofmonth_order_date
            ```

        Args:
            df (pyspark.sql.DataFrame):
                A target Spark DataFrame for applying the transformation.

            date_col (str):
                A date column name (or parseable string as date) to be used in
                the date extraction process.

            date_col_type (str):
                Reference for data type of `date_col` argument. Acceptable
                values are "date" or "timestamp".

            date_format (str):
                Date format applied in a optional string to date casting.
                It's applicable only if `cast_string_to_date=True`

            cast_string_to_date (bool):
                Enables an automatic casting of the `date_col` column reference
                into a given `date_format`.

        Keyword Args:
            year (bool): Extracts the year of target date column
            quarter (bool): Extracts the quarter of target date column
            month (bool): Extracts the month of target date column
            dayofmonth (bool): Extracts the dayofmonth of target date column
            dayofweek (bool): Extracts the dayofweek of target date column
            weekofyear (bool): Extracts the weekofyear of target date column

        Raises:
            ValueError: Exception raised if the `date_col_type` argument is\
            passed in non acceptable value (e.g. something different of "date"\
            or "timestamp").

        Returns:
            Spark DataFrame with new date columns extracted.
        """

        try:
            # Creating casting expressions based on data type of date_col arg
            date_col_type = date_col_type.strip().lower()
            if cast_string_to_date:
                if date_col_type == "date":
                    casting_expr = f"to_date({date_col},\
                        '{date_format}') AS {date_col}_{date_col_type}"
                elif date_col_type == "timestamp":
                    casting_expr = f"to_timestamp({date_col},\
                        '{date_format}') AS {date_col}_{date_col_type}"
                else:
                    raise ValueError("The data type of date_col_type parameter"
                                     " is invalid. Acceptable values are "
                                     "'date' or 'timestamp'")

                # Applying a select expression for casting data if applicable
                df = df.selectExpr(
                    "*",
                    casting_expr
                ).drop(date_col)\
                    .withColumnRenamed(f"{date_col}_{date_col_type}", date_col)

        except ValueError as ve:
            logger.error(ve)
            raise ve

        except AnalysisException as ae:
            logger.error("Analysis error on trying to cast the column "
                         f"{date_col} using the expression {casting_expr}. "
                         "Maybe this column doesn't exist on the DataFrame. "
                         f"Check the error treceback for more details: {ae}")
            raise ae

        # Creating a list of all possible date attributes to be extracted
        possible_date_attribs = ["year", "quarter", "month", "dayofmonth",
                                 "dayofweek", "dayofyear", "weekofyear"]

        # Iterating over all possible attributes and extracting date attribs
        for attrib in possible_date_attribs:
            # Add a new column only if attrib is in kwargs
            if attrib in kwargs and bool(kwargs[attrib]):
                df = df.withColumn(
                    f"{attrib}_{date_col}", expr(f"{attrib}({date_col})")
                )

        return df

    @staticmethod
    def agg_data(spark_session: SparkSession,
                 df: DataFrame,
                 numeric_col: str,
                 group_by: str or list,
                 round_result: bool = False,
                 n_round: int = 2,
                 **kwargs) -> DataFrame:
        """Extracts statistical attributes based on a group by operation.

        This method makes it possible to extract multiple statistical
        aggregations based in a numerical column and a set of columns to be
        grouped by. With this feature, users can configure complex aggregations
        in a single method call in order to enhance their analysis.

        Examples:
            ```python
            # Creating a new special and aggregated DataFrame
            df_stats = spark_manager.agg_data(
                spark_session=spark,
                df=df_orders,
                numeric_col="order_value",
                group_by=["order_id", "order_year"],
                sum=True,
                mean=True,
                max=True,
                min=True
            )

            # In the above example, the method will return a new DataFrame with
            # the following columns:
            # order_id e order_year (group by)
            # sum_order_value (sum of order_value column)
            # mean_order_value (average of order_value column)
            # max_order_value (max value of order_value column)
            # min_order_value (min value of order_value column)
            ```

        Args:
            spark_session (pyspark.sql.SparkSession):
                A SparkSession object to be used to run SparkSQL query for
                grouping data

            df (pyspark.sql.DataFrame):
                A target Spark DataFrame for applying the transformation

            numeric_col (str):
                A numeric column name on the target DataFrame to be used as
                target of aggregation process

            group_by (str or list):
                A column name or a list of columns used as group categories
                on the aggregation process

            round_result (bool):
                Enables rounding aggregation results on each new column

            n_round (int):
                Defines the round number on rounding. Applied only if
                `round_result=True`

        Tip: About keyword arguments
            In order to provide a new feature that is capable to put together
            the extraction of multiple statistical attributes with a single
            line of code, a special list of pyspark functions were selected
            as acceptable functions to be called on the aggregation process.

            It means that if users wants to apply an aggregation on the
            target DataFrame and extract the sum, the mean, the minimum and
            the maximum value of a given numeric column, they must pass
            keyword arguments as following: `sum=True`, `mean=True`,
            `min=True` and `max=True`.

            All acceptable keyword arguments (pyspark functions) can be
            found right below:

        Keyword Args:
            sum (bool): Extracts the sum of a given numeric column
            mean (bool): Extracts the mean of a given numeric column
            max (bool): Extracts the max of a given numeric column
            min (bool): Extracts the min of a given numeric column
            countDistinct (bool): Extracts the count distinct value\
                of a given numeric column
            variance (bool): Extracts the variance of a given numeric column
            stddev (bool): Extracts the standard deviation of a given numeric\
                column
            kurtosis (bool): Extracts the kurtosis of a given numeric column
            skewness (bool): Extracts the skewness of a given numeric column

        Returns:
            A new Spark DataFrame with new statistical columns based on the\
            aggregation configured by user on method call.

        Raises:
            Exception: Generic exception raised when failed to execute the\
            SparkSQL query for extracting the stats from the DataFrame.
        """

        # Joining all group by columns in a single string to make agg easier
        if type(group_by) == list and len(group_by) > 1:
            group_by = ",".join(group_by)

        # Creating a Spark temporary table for grouping data using SparkSQL
        df.createOrReplaceTempView("tmp_extract_aggregate_statistics")

        possible_functions = ["sum", "mean", "max", "min", "count",
                              "variance", "stddev", "kurtosis", "skewness"]
        try:
            # Iterating over the attributes to build a single aggregation expr
            agg_query = ""
            for f in possible_functions:
                if f in kwargs and bool(kwargs[f]):
                    if round_result:
                        agg_function = f"round({f}({numeric_col}), {n_round})"
                    else:
                        agg_function = f"{f}({numeric_col})"

                    agg_query += f"{agg_function} AS {f}_{numeric_col},"

            # Dropping the last comma on the expression
            agg_query = agg_query[:-1]

            # Building the final query to be executed
            final_query = f"""
                SELECT
                    {group_by},
                    {agg_query}
                FROM tmp_extract_aggregate_statistics
                GROUP BY
                    {group_by}
            """

            return spark_session.sql(final_query)

        except AnalysisException as ae:
            logger.error("Error on trying to aggregate data from DataFrame "
                         f"using the following query:\n {final_query}. "
                         "Possible reasons are: missing to pass group_by "
                         "parameter or numeric_col argument doesn't exists "
                         f"on the DataFrame. Exception: {ae}")
            raise ae

    @staticmethod
    def add_partition_column(df: DataFrame,
                             partition_name: str,
                             partition_value) -> DataFrame:
        """Adding a "partition" column on a Spark DataFrame.

        This method is responsible for adding a new column on a target Spark
        DataFrame to be considered as a table partition. In essence, this
        method uses the native pyspark `.withColumn()` method for adding a
        new column to the DataFrame using a name for the partition column
        (partition_name) and its value (partition_value).

        Examples
            ```python
            # Defining partition information
            partition_name = "anomesdia"
            partition_value = int(datetime.strftime('%Y%m%d'))

            # Adding a partition column to the DataFrame
            df_partitioned = spark_manager.add_partition_column(
                df=df_orders,
                partition_name=partition_name,
                partition_value=partition_value
            )

            # The method returns a new DataFrame with a new column
            # referenced by "anomesdia" and its value referenced by
            # the datetime library
            ```

        Args:
            df (pyspark.sql.DataFrame): A target Spark DataFrame.
            partition_name (str): Column name to be added on the DataFrame.
            partition_value (Any): Value for the new column to be added.

        Returns:
            A Spark DataFrame with the new column added.

        Raises:
            Exception: A generic exception raised on failed to execute the\
            method `.withColumn()` for adding the new column.
        """

        logger.info("Adding a new partition column to the DataFrame "
                    f"({partition_name}={str(partition_value)})")
        try:
            df_partitioned = df.withColumn(partition_name,
                                           lit(partition_value))
            return df_partitioned

        except Exception as e:
            logger.error("Error on adding a partition colum to the DataFrame "
                         f"using the .withColumn() method. Exception: {e}")
            raise e

    @staticmethod
    def repartition_dataframe(df: DataFrame, num_partitions: int) -> DataFrame:
        """Repartitioning a Spark DataFrame for optimizing storage.

        This method applies the repartition process in a Spark DataFrame in
        order to optimize its storage on S3. The method has some important
        checks based on each pyspark method to use for repartitioning the
        DataFrame. Take a look at the below tip to learn more.

        Tip: Additional details on method behavior
            The method `repartition_dataframe()` works as follows:

            1. The current number of partitions in the target DataFrame
            is checked
            2. The desired number of partitions passed as a parameter
            is checked

            * If the desired number is LESS than the current number, then the
            method `coalesce()` is executed
            * If the desired number is GREATER than the current one, then the
            method `repartition()` is executed

        Examples:
            ```python
            # Repartitioning a Spark DataFrame
            df_repartitioned = spark_manager.repartition_dataframe(
                df=df_orders,
                num_partitions=10
            )
            ```

        Args:
            df (pyspark.sql.DataFrame): A target Spark Dataframe.
            num_partitions (int): Desider number of partitions.

        Returns:
            A new Spark DataFrame gotten after the repartition process.

        Raises:
            Exception: A generic exception is raised on a failed attempt to\
            run the repartition method (`coalesce()` or `repartition()`) in\
            the given Spark DataFrame.
        """

        # Casting arg to integer to avoid exceptions
        num_partitions = int(num_partitions)

        # Getting the current number of partitions of the given DataFrame
        logger.info("Getting the current number of partition of the DataFrame")
        try:
            current_partitions = df.rdd.getNumPartitions()

        except Exception as e:
            logger.error("Error on collecting the current number of "
                         "using df.rdd.getNumPartitions method. "
                         f"Exception: {e}")
            raise e

        # If the desired partition number if equal to the current number, skip
        if num_partitions == current_partitions:
            logger.warning(f"The current number of partitions "
                           f"({current_partitions}) is equal to the desired "
                           f"number ({num_partitions}). There is no need to "
                           "run any Spark repartition method")
            sleep(0.01)
            return df

        # If the desired number if LESS THAN the current, use coalesce()
        elif num_partitions < current_partitions:
            logger.info("Initializing the repartition process using "
                        f"coalesce(), changing the number of DataFrame "
                        f"partitions from {current_partitions} to "
                        f"{num_partitions}")
            try:
                df_repartitioned = df.coalesce(num_partitions)

            except Exception as e:
                logger.warning("Error on repartitioning using coalesce(). "
                               "The repartition process won't be executed and "
                               "the target DataFrame will be returned without "
                               f"any changes. Exception: {e}")
                return df

        # If the desired number is GREATER THAN the current, use repartition()
        elif num_partitions > current_partitions:
            logger.warning(f"The desired partition number ({num_partitions}) "
                           f"is greater than the current one "
                           f"({current_partitions}) and, because of that, "
                           "the repartitioning operation will be executed "
                           "using the repartition() method which can be "
                           "expensive under the application perspective. As a "
                           "suggestion, check if this is really the expected "
                           "behavior for this operation.")
            try:
                df_repartitioned = df.repartition(num_partitions)

            except Exception as e:
                logger.warning("Error on repartitioning using repartition(). "
                               "The repartition process won't be executed and "
                               "the target DataFrame will be returned without "
                               f"any changes. Exception: {e}")
                return df

        return df_repartitioned
