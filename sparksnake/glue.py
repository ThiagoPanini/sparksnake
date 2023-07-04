"""Managing common operations found in ETL jobs developed using AWS Glue.

This module aims to put together everything it's needed to improve and enhance
the development journey of Glue jobs on AWS.

___
"""

# Importing libraries
import sys
from time import sleep
from sparksnake.utils.log import log_config

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import DataFrame


# Setting up a logger object
logger = log_config(logger_name=__file__)


class GlueJobManager():
    """Enables users to use AWS Glue features in their Spark applications.

    This class provides an enhanced experience on developing Glue jobs using
    Apache Spark. The core idea behind it is related complex code encapsulation
    that is mandatory to build and run Glue jobs. By that, it's possible to
    say that this `GlueJobManager` class has attributes and methods that uses
    the `awsglue` library to handle almost everything that would be handled
    individually by users if they are not using sparksnake.

    By the end, it's important to mention that all attributes and methods of
    this class are inherited by `SparkETLManager` class in the `manager` module
    when users initialize it with `mode="glue"`. So, when doing that, users
    need to pass some additional arguments in order to make things work
    properly in the Glue specific world.

    Args:
        argv_list (list):
            List with all user defined arguments for the job

        data_dict (dict):
            Dictionary with all source data references to be read from the
            catalog and used in the ETL job.

    This class can get some other attributes along the execution of its
    methods. Those are:

    Attributes:
        args (dict):
            Dictionary with all arguments of the job. This is a joint
            between user defined arguments and system arguments (`sys.argv`)

        sc (SparkContext):
            A Spark context object used for creating a Glue context object

        glueContext (GlueContext):
            A Glue context object used for creating a Spark session object

        spark (SparkSession):
            A Spark session object used as a central point for job operations

    Tip: About setting up the data_dict class attribute
        The data_dict dictionary passed as a required attribute for the
        `GlueJobManager` class must be defined following some rules. Then main
        purpose of such attribute is to provide a single variable to handle
        all data sources to be read by the Glue job application.

        With that in mind, it's important to say that the data_dict attribute
        can be defined using everything that is available and acceptable in
        the Glue DynamicFrameReader class. Users can take a look at the AWS
        official docs about the DynamicFrameReader class to see more details
        about reading DynamicFrame objects in Glue jobs.

        On this class scope, the data_dict dictionary can also have additional
        keys that can used to guide reading proccesses and apply some special
        conditions. The additional keys that can be put on data_dict class
        attribute are:

        - "source_method": str -> Defines if users want to read data
        from catalog ('from_catalog') or from other options ('from_options').
        Under the hood, the 'source_method' dictionary key defines which method
        will be used along the `glueContext.create_dynamic_frame` method.
        The default value is 'from_catalog'.

        - "create_temp_view": bool -> Sets the creation of a Spark temporary
        table (view) after reading the data source as a DynamicFrame. If this
        additional key is set as True, then the `DataFrame.createTempView()`
        method is executed in order to create temporary tables using the
        table name as the main reference for the temp view.

        By the end, an example on how to define the data_dict class attribute
        dictionary can be find right below:

        ```python
        # Defining elements of all data sources used on the job
        ```python
        {
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
            },
            "payments": {
                "source_method": "from_options",
                "connection_type": "s3",
                "connection_options": {
                    "paths": [
                        "s3://some-bucket-name/some-prefix/file.csv"
                    ],
                    "recurse": True
                },
                "format": "csv",
                "format_options": {
                    "withHeader": True,
                    "separator": ",",
                    "quoteChar": '"'
                },
                "transformation_ctx": "dyf_payments"
            }
        }
        ```

        As you can see, the DATA_DICT variable defined in the example above
        uses mixed data sources, each one with special configurations
        accepted by the DynamicFrameReader Glue methods. But don't worry,
        you will find more examples along this documentation page.
    """

    def __init__(self, argv_list: list, data_dict: dict) -> None:
        self.argv_list = argv_list
        self.data_dict = data_dict

        # Getting job arguments
        self.args = getResolvedOptions(sys.argv, self.argv_list)

    def job_initial_log_message(self) -> None:
        """Preparing a detailed log message for job start up.

        This method is responsible for composing an initial log message to be
        logged in CloudWatch after the user starts a Glue Job. The message aims
        to clarify some job details, such as the data sources mapped and its
        push down predicate values (if used). This can be a good practice in
        order to develop more organized Glue jobs.
        """

        # Defining a initial string for composing the message
        welcome_msg = f"Successfully started Glue job {self.args['JOB_NAME']}"\
                      ". Data sources in this ETL process:\n\n"
        initial_msg = ""

        # Iterating over the data_dict dicionary for extracting some info
        for _, params in self.data_dict.items():
            # Getting table name and start building the message
            tbl_ref = f"{params['database']}.{params['table_name']}"
            table_msg = f"Table {tbl_ref} "

            # Looking for a push_down_predicate information in the dictionary
            if "push_down_predicate" in params:
                table_msg += "with the following push down predicate info: "\
                             f"{params['push_down_predicate']}\n"
            else:
                table_msg += "without push down predicate\n"

            # Concat final message
            initial_msg += table_msg

        # Logging the message to CloudWatch
        logger.info(welcome_msg + initial_msg)

    def print_args(self) -> None:
        """Getting and logging job arguments in CloudWatch.

        This method is responsible to show users all the arguments used in the
        job. It achieves its goal by iterating over all args in self.args class
        attribut for composing a detailed log message.
        """

        # Formatting and composing a log message with job arguments
        args_formatted = "".join([f'--{k}="{v}"\n'
                                  for k, v in self.args.items()])
        logger.info(f"Job arguments:\n\n{args_formatted}")
        sleep(0.01)

    def get_context_and_session(self) -> None:
        """Getting context and session elements for Glue job application.

        This method is a central point for creating the following elements:
        SparkContext, GlueContext and SparkSession.
        """
        logger.info("Creating SparkContext, GlueContext and SparkSession")
        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session

    def init_job(self):
        """Starting a Glue job.

        This method consolidates all the necessary steps required for a Glue
        job initialization process. In its definition, it calls the following
        methods:

        * `self.job_initial_log_message()`
        * `self.print_args()`
        * `self.get_context_and_session()`

        After that, the class has all the attributes required for calling the
        Job class used by Glue to initialize a job object.
        """

        # Getting job arguments and logging useful infos for users
        self.job_initial_log_message()
        self.print_args()

        # Getting context and session elements
        self.get_context_and_session()

        # Initializing a Glue job based on a GlueContext element
        self.job = Job(self.glueContext)
        self.job.init(self.args['JOB_NAME'], self.args)

    def generate_dynamicframes_dict(self) -> dict:
        """Getting a dictionary of DynamicFrames objects read from the catalog.

        This method uses the data_dict class attribute for reading and getting
        DynamicFrame objects mapped as data sources on the mentioned
        dictionary. The main advantage of using this method is explained by
        having the possibility to read multiple data sources with a single
        method call. The result of this method is a dictionary containing all
        DynamicFrames objects as values of keys mapped on data_dict class
        attribute. The user will be able to access those DynamicFrames in
        a easy way through indexing.

        Examples:
            ```python
            # Getting a dictionary of Glue DynamicFrames
            dyfs_dict = spark_manager.generate_dynamicframes_dict()

            # Inexing and getting individual DynamicFrames
            dyf_orders = dyfs_dict["orders"]
            dyf_customers = dyfs_dict["customers"]
            ```

        Returns:
            Python dictionary with keys representing the identification of the\
            data source put into the data_dict class attributes and values\
            representing DynamicFrame objects read from catalog.

        Tip: Details about the returned dictionary from the method
            In order to provide a clear view of the return of this method,
            consider the following definition example of `self.data_dict` as a
            class attribute:

            ```python
            {
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
            ```

            As stated before, all elements on Glue DynamicFrameReader class
            methods can be used as dictionary keys on `self.data_dict`
            definition. Moreover, some additional keys can be defined by the
            user for some special purposes, such as:

            - "source_method": str -> Defines if users want to read data
            from catalog ('from_catalog') or from other options
            ('from_options'). Under the hood, the 'source_method' dictionary
            key defines which method will be used along the
            `glueContext.create_dynamic_frame` method. The default value is
            'from_catalog'.

            - "create_temp_view": bool -> Sets the creation of a Spark
                temporary table (view) after reading the data source as a
                DynamicFrame

            The return of the method `generate_dynamicframes_dict()` will be
            presented as the following format:

            ```python
            {
                "orders": <DynamicFrame>
                "customers": <DynamicFrame>
            }
            ```

            where the tags <DynamicFrame> means objects of DynamicFrame type
            read for each data source.
        """

        logger.info("Reading all data sources as Glue DynamicFrame objects")
        try:
            dynamic_frames = []
            for t in self.data_dict.keys():
                # Defining the source method to be called to read the dyf
                source_method = self.data_dict[t]["source"].lower().strip() \
                    if "source" in self.data_dict[t].keys()\
                    else "from_catalog"

                # Entering a condition where the data is read from catalog
                if source_method == "from_catalog":
                    # Getting some required args: database and table_name
                    database = self.data_dict[t]["database"]
                    table_name = self.data_dict[t]["table_name"]

                    # Getting non required args: redshift_tmp_dir
                    redshift_tmp_dir = self.data_dict[t]["redshift_tmp_dir"]\
                        if "redshift_tmp_dir" in self.data_dict[t].keys()\
                        else ""

                    # Getting non required args: transformation_ctx
                    transformation_ctx = \
                        self.data_dict[t]["transformation_ctx"]\
                        if "transformation_ctx" in self.data_dict[t].keys()\
                        else ""

                    # Getting non required args: push_down_predicate
                    push_down_predicate = \
                        self.data_dict[t]["push_down_predicate"]\
                        if "push_down_predicate" in self.data_dict[t].keys()\
                        else ""

                    # Getting non required args: additional_options
                    additional_options = \
                        self.data_dict[t]["additional_options"] \
                        if "additional_options" in self.data_dict[t].keys()\
                        else {}

                    # Getting non required args: catalog_id
                    catalog_id = self.data_dict[t]["catalog_id"] \
                        if "catalog_id" in self.data_dict[t].keys()\
                        else None

                    # Reads a DynamicFrame from catalog
                    dyf = self.glueContext.create_dynamic_frame.from_catalog(
                        database=database,
                        table_name=table_name,
                        redshift_tmp_dir=redshift_tmp_dir,
                        transformation_ctx=transformation_ctx,
                        push_down_predicate=push_down_predicate,
                        additional_options=additional_options,
                        catalog_id=catalog_id
                    )

                # Entering a condition where the data is read from other option
                elif source_method == "from_options":
                    # Getting some required args: connection_type
                    connection_type = self.data_dict[t]["connection_type"]

                    # Getting non required args: connection_options
                    conn_options = self.data_dict[t]["connection_options"] \
                        if "connection_options" in self.data_dict[t].keys()\
                        else {}

                    # Getting non required args: catalog_id
                    format = self.data_dict[t]["format"] \
                        if "format" in self.data_dict[t].keys()\
                        else None

                    # Getting non required args: format_options
                    format_options = self.data_dict[t]["format_options"] \
                        if "format_options" in self.data_dict[t].keys()\
                        else {}

                    # Reads a DynamicFrame from options
                    dyf = self.glueContext.create_dynamic_frame.from_options(
                        connection_type=connection_type,
                        connection_options=conn_options,
                        format=format,
                        format_options=format_options,
                        transformation_ctx=transformation_ctx
                    )

                else:
                    # Raising an error if source_method is not expected
                    raise TypeError("Invalid value for 'source_method' in "
                                    f"DATA_DICT dictionary ({source_method}). "
                                    "Acceptable values are 'from_catalog' or "
                                    "'from_options'.")

                # Appending the DynamicFrame obnject to DynamicFrames list
                dynamic_frames.append(dyf)

        except Exception as e:
            logger.error("Failed to read data sources mapped on DATA_DICT "
                         f"dictionary as Glue DynamicFrames. Exception: {e}")
            raise e

        # Creating a DynamicFrames dictionary
        dynamic_dict = {
            k: dyf for k, dyf in zip(self.data_dict.keys(), dynamic_frames)
        }

        # Preparing a final message with all data sources read
        print_dict = {k: type(v) for k, v in dynamic_dict.items()}
        logger.info("Sucessfully read all data sources. There are "
                    f"{len(dynamic_dict.values())} DynamicFrames available "
                    "in a Python dictionary presented as following: \n"
                    f"{print_dict}")

        # Returning the dictionary
        sleep(0.01)
        return dynamic_dict

    def generate_dataframes_dict(self) -> dict:
        """Getting a dictionary of DataFrames objects read from the catalog.

        This method uses the data_dict class attribute for reading and getting
        DataFrame objects mapped as data sources on the mentioned
        dictionary. The main advantage of using this method is explained by
        having the possibility to read multiple data sources with a single
        method call. The result of this method is a dictionary containing all
        DataFrames objects as values of keys mapped on data_dict class
        attribute. The user will be able to access those DataFrames in
        a easy way through indexing.

        In its behalf, this method calls `generate_dynamicframes_dict()` method
        for getting a dictionary of DynamicFrames after applying the method
        `toDF()` for transforming all objects into Spark DataFrames.

        Examples:
            ```python
            # Getting a dictionary of Spark DataFrames
            dyfs_dict = spark_manager.generate_dataframes_dict()

            # Inexing and getting individual DataFrames
            dyf_orders = dyfs_dict["orders"]
            dyf_customers = dyfs_dict["customers"]
            ```

        Returns:
            Python dictionary with keys representing the identification of the\
            data source put into the data_dict class attributes and values\
            representing DataFrame objects read from catalog.

        Tip: Details about the returned dictionary from the method
            In order to provide a clear view of the return of this method,
            consider the following definition example of `self.data_dict` as a
            class attribute:

            ```python
            {
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
            ```

            As stated before, all
            `glueContext.create_dynamic_frame.from_catalog()` can be used as
            dictionary keys on `self.data_dict` definition. Moreover, some
            additional keys can be defined by the user for some special
            purposes, such as:

            - "create_temp_view": bool -> Sets the creation of a Spark
                temporary table (view) after reading the data source as a
                DataFrame

            The return of the method `generate_dataframes_dict()` will be
            presented as the following format:

            ```python
            {
                "orders": <DataFrame>
                "customers": <DataFrame>
            }
            ```

            where the tags <DataFrame> means objects of DataFrame type
            read for each data source.
        """

        # Getting a DynamicFrame objects dictionary
        dyf_dict = self.generate_dynamicframes_dict()

        # Transforming DynamicFrames into DataFrames
        logger.info(f"Transforming {len(dyf_dict.keys())} "
                    "Glue DynamicFrames into Spark DataFrames")
        try:
            # Transforming objects and mapping the result into a python dict
            df_dict = {k: dyf.toDF() for k, dyf in dyf_dict.items()}

            # Preparing a log message
            print_dict = {k: type(v) for k, v in df_dict.items()}
            logger.info("Sucessfully generated Spark DataFrames from Glue "
                        "DynamicFrames previously read. There are "
                        f"{len(df_dict.values())} Spark DataFrames available "
                        "in a Python dictionary presented as following: "
                        f"{print_dict}")
            sleep(0.01)

        except Exception as e:
            logger.error("Failed to transform Glue DynamicFrames into Spark "
                         f"DataFrames. Exception: {e}")
            raise e

        # Creating spark temporary tables if applicable
        for table_key, params in self.data_dict.items():
            try:
                # Extracting useful variables from the dict
                df = df_dict[table_key]
                table_name = params["table_name"]

                # Creating temp tables (if the flag for it was set as True)
                if "create_temp_view" in params\
                        and bool(params["create_temp_view"]):
                    df.createOrReplaceTempView(table_name)

                    logger.info("Successfully created a Spark temporary table "
                                f"(view) named {table_name}")

            except Exception as e:
                logger.error("Failed to create a Spark temporary table for "
                             f"data source {table_name}. Exception: {e}")
                raise e

        # Returning the DataFrames dict
        sleep(0.01)
        return df_dict

    def drop_partition(self, s3_partition_uri: str,
                       retention_period: int = 0) -> None:
        """Deleting (purging) a physical table partition directly on S3.

        This methods is responsable for excluding physical partitions on S3
        through purge method from a GlueContext. The users can use this feature
        for ensuring that new writing processes in a specific partitions will
        only be done after deleting existing references for the given
        partition.

        Examples:
            ```python
            # Dropping a physycal partition on S3
            partition_uri = "s3://some-bucket/some-table/partition=value/"
            spark_manager.drop_partition(partition_uri)

            # The result is the elimination of everything under the prefix
            # partition=name (including the prefix itself and all table files)
            ```

        Args:
            s3_partition_uri (str): Partition URI on S3
            retention_period (int): Hours to data retention

        Raises:
            Exception: Generic exception raises when a failed attempt of
            executing the `glueContext.purge_s3_path()` method.
        """

        logger.info(f"Searching for partition {s3_partition_uri} and "
                    "dropping it (if exists)")
        try:
            self.glueContext.purge_s3_path(
                s3_path=s3_partition_uri,
                options={"retentionPeriod": retention_period}
            )
        except Exception as e:
            logger.error(f"Failed to purge partition {s3_partition_uri}. "
                         f"Exception: {e}")
            raise e

    def write_and_catalog_data(
            self,
            df: DataFrame or DynamicFrame,
            s3_table_uri: str,
            output_database_name: str,
            output_table_name: str,
            partition_name: str or list = None,
            connection_type: str = "s3",
            update_behavior: str = "UPDATE_IN_DATABASE",
            compression: str = "snappy",
            enable_update_catalog: bool = True,
            output_data_format: str = "parquet"
    ) -> None:
        """Writing data on S3 anda cataloging on Data Catalog.

        This methods is responsible to put together all the steps needed to
        write a Spark DataFrame or a Glue DynamicFrame into S3 and catalog
        its metadata on Glue Data Catalog. In essence, this methods include
        the following steps:

        1. Check if the data object type (df argument) is a Glue DynamicFrame
        (if it's not, it converts it)
        2. Make a sink with data catalog
        3. Write data in S3 and update the data catalog with the behavior
        chosen by the user

        Examples:
            ```python
            # Writing and cataloging data
            spark_manager.write_and_catalog_data(
                df=df_orders,
                s3_table_uri="s3://some-bucket/some-table",
                output_database_name="db_corp_business_inteligence",
                output_table_name="tbl_orders_prep",
                partition_name="anomesdia"
            )
            ```

        Args:
            df (DataFrame or DynamicFrame):
                A data object that can be a Spark DataFrame
                (`pyspark.sql.DataFrame`) or a Glue DynamicFrame
                (`awsglue.dynamicframe.DynamicFrame`) considered as a target
                element for writing and cataloging processes. This information
                is used on `glueContext.getSink().writeFrame()` method.

            s3_table_uri (str):
                Table URI in the format `s3://bucket-name/table-prefix/`. This
                information is used on parameter "path" from
                `glueContext.getSink()` method.

            output_database_name (str):
                Reference for the database used on the catalog process for the
                table. This information is used on parameter "catalogDatabase"
                from `glueContext.getSink().setCatalogInfo()` method.

            output_table_name (str):
                Reference for the table used on the catalog process. This
                information is used on parameter "catalogTableName"
                from `glueContext.getSink().setCatalogInfo()` method.

            partition_name (str or list or None):
                Partition coumn name chosen for the table. This information is
                used on parameter "partitionKeys" from `glueContext.getSink()`
                method. In case of partitioning by multiple columns, users can
                pass a list with partition names.

            connection_type (str):
                Connection type used in storage. This information is used on
                parameter "connection_type" from `glueContext.getSink()` method

            update_behavior (str):
                Defines the update behavior for the target table. This
                information is used on parameter "updateBehavior" from
                `glueContext.getSink()` method.

            compression (str):
                Defines the compression method used on data storage process.
                This information is used on parameter "compression" from
                `glueContext.getSink()` method.

            enable_update_catalog (bool):
                Enables the update of data catalog with data storage by this
                method. This information is used on parameter
                "enableUpdateCatalog" from  `glueContext.getSink()` method.

            output_data_format (str):
                Defines the data format for the data to be stored on S3. This
                information is used on parameter "output_data_format" from
                `glueContext.getSink().setFormat()` method.
        """

        # Converting DataFrame into DynamicFrame
        if type(df) == DataFrame:
            logger.info("Converting the Spark DataFrame as Glue DynamicFrame")
            try:
                dyf = DynamicFrame.fromDF(df, self.glueContext, "dyf")
            except Exception as e:
                logger.error("Failed to transform the Spark DataFrame object "
                             f"in a Glue DynamicFrame object. Exception: {e}")
                raise e
        else:
            dyf = df

        # Handling partition information
        if isinstance(partition_name, str):
            partition_keys = [partition_name]
        elif isinstance(partition_name, list):
            partition_keys = partition_name
        else:
            raise ValueError("Invalid type for argument partition_name. "
                             "Acceptable values are str or list")

        # Creating a sink with S3
        logger.info("Creating a sink with all configurations provided")
        try:
            # Criando relação de escrita de dados
            data_sink = self.glueContext.getSink(
                path=s3_table_uri,
                connection_type=connection_type,
                updateBehavior=update_behavior,
                partitionKeys=partition_keys,
                compression=compression,
                enableUpdateCatalog=enable_update_catalog,
                transformation_ctx="data_sink",
            )
        except Exception as e:
            logger.error("Failed to create a sink for the output table. "
                         f"Exception: {e}")
            raise e

        # Setting up data catalog info
        logger.info("Setting up information on the Data Catalog for the table")
        try:
            data_sink.setCatalogInfo(
                catalogDatabase=output_database_name,
                catalogTableName=output_table_name
            )
            data_sink.setFormat(output_data_format,
                                useGlueParquetWriter=True)
            data_sink.writeFrame(dyf)

            logger.info(f"Successfully updated table {output_database_name}"
                        f".{output_table_name} in Glue Data Catalog. The "
                        "table data are stored in the following S3 path: "
                        f"{s3_table_uri}")
        except Exception as e:
            logger.error("Failed to write and catalog data for table  "
                         f"{output_database_name}.{output_table_name}. "
                         f"Exception: {e}")
            raise e
