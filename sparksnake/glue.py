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


# Configuring a logger object
logger = log_config(logger_name=__file__)


class GlueJobManager():
    """Management of elements and operations commonly used in Glue jobs.

    This class is responsible for managing and providing all necessary inputs
    for launching and developing Glue jobs in AWS. All attributes and methods
    declared in this class are *inherited* by `SparkETLManager`class in the
    `manager` module.

    To see a end to end usage example, check the docs for the `SparkETLManager`
    class.

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
        `GlueJobManager` class must be defined following some rules.

        Your main purpose is to provide a single point for controlling all
        data sources used in the job. So, your composition is something really
        important for ensuring that reading process can be executed in an
        expected way.

        With that in mind, it's crucial to say that the data_dict dictionary
        can be defined with all parameters found in 
        `glueContext.create_dynamic_frame.from_catalog` Glue method. In other
        words, all keys of data_dict dictionary can be assume any valid
        parameter of the Glue method mentioned above. Of course the values
        of those keys defined in data_dict depends on the job rules.

        :star: It means that if the user wants to read a data source from the
        catalog using the push down predicate Glue feature, it's only necessary
        to define a key called "push_down_predicate" (the same way as found in
        `glueContext.create_dynamic_frame.from_catalog` method) with the value
        to be filtered. This is the same rule for all other parameters, such as
        additional_options, catalog_id, transformation_ctx, and others. If the
        user wants to read some data source with a special parameter, just take
        a look at the mentioned Glue method above and input a key on the
        data_dict to set up the data source reading process.

        By the other hand, it's also important to mention that the user is not
        forced to define the data_dict dictionary with all parameters found in
        `glueContext.create_dynamic_frame.from_catalog` method. If the
        data_dict attribute doesn't have a key for a specific parameter on the
        Glue method mentioned abouve, so its default value will be considered.
        For example, that if data_dict doesn't have a push_down_predicate key
        for a data source, the value "None" will be considered as it's the
        default value for this key on the
        `glueContext.create_dynamic_frame.from_catalog` method.
    """

    def __init__(self, argv_list: list, data_dict: dict) -> None:
        self.argv_list = argv_list
        self.data_dict = data_dict

        # Getting job arguments
        self.args = getResolvedOptions(sys.argv, self.argv_list)

    def job_initial_log_message(self) -> None:
        """Preparation of a detailed log message for job initialization.

        This method is responsible for composing an initial log message to be
        shown in CloudWatch after the user initializes a Glue Job. The message
        aim to clarify some job details, such as all the data sources mapped
        and its push down predicate values (if used). This can be a good
        practice for making Glue jobs cleaner and more organized.
        """

        # Defining a initial string for composing the message
        welcome_msg = f"Initialing the execution of {self.args['JOB_NAME']} "\
                      "job. Data sources used in this ETL process:\n\n"
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
        """Initializing a Glue job.

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

            As stated before, all
            `glueContext.create_dynamic_frame.from_catalog()` can be used as
            dictionary keys on `self.data_dict` definition. Moreover, some
            additional keys can be defined by the user for some special
            purposes, such as:

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

        logger.info("Looping for the data_dict dictionary for reading data "
                    "sources as Glue DynamicFrame objects")
        try:
            dynamic_frames = []
            for t in self.data_dict.keys():
                # Getting some required args: database, table_name, ctx
                database = self.data_dict[t]["database"]
                table_name = self.data_dict[t]["table_name"]
                transformation_ctx = self.data_dict[t]["transformation_ctx"]

                # Getting non required args: push_down_predicate
                push_down_predicate = self.data_dict[t]["push_down_predicate"]\
                    if "push_down_predicate" in self.data_dict[t].keys()\
                    else ""

                # Getting non required args: additional_options
                additional_options = self.data_dict[t]["additional_options"] \
                    if "additional_options" in self.data_dict[t].keys()\
                    else {}

                # Getting non required args: catalog_id
                catalog_id = self.data_dict[t]["catalog_id"] \
                    if "catalog_id" in self.data_dict[t].keys()\
                    else None

                # Reading the DynamicFrame
                dyf = self.glueContext.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table_name,
                    transformation_ctx=transformation_ctx,
                    push_down_predicate=push_down_predicate,
                    additional_options=additional_options,
                    catalog_id=catalog_id
                )

                # Appending the DynamicFrame obnject to DynamicFrames list
                dynamic_frames.append(dyf)

        except Exception as e:
            logger.error("Error on generating a list of DynamicFrames list. "
                         f"Exception: {e}")
            raise e

        logger.info("Mapping DynamicFrames objects to a dictionary key")
        sleep(0.01)

        # Creating a DynamicFrames dictionary
        dynamic_dict = {k: dyf for k, dyf
                        in zip(self.data_dict.keys(), dynamic_frames)}
        logger.info("Success on creating data. There are a number of "
                    f"{len(dynamic_dict.values())} DynamicFrames read.")

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
            df_dict = {k: dyf.toDF() for k, dyf in dyf_dict.items()}
            logger.info("Success on generating all Spark DataFrames")
            sleep(0.01)

        except Exception as e:
            logger.error("Error on transforming Glue DataFrames into Spark "
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

                    logger.info(f"Spark temporary table (view) {table_name} "
                                "was successfully created.")

            except Exception as e:
                logger.error("Error on creating Spark temporary table "
                             f"{table_name}. Exception: {e}")
                raise e

        # Returning the DataFrames dict
        sleep(0.01)
        return df_dict

    def drop_partition(self, s3_partition_uri: str,
                       retention_period: int = 0) -> None:
        """
        Exclusão (purge) de partição física de tabela no S3.

        Método responsável por eliminar partições do s3 através do purge físico
        dos arquivos armazenados em um determinado prefixo. Em essência, este
        método pode ser utilizado em conjunto com o método de adição de
        partições, garantindo que dados não serão duplicados em uma mesma
        partição em casos de dupla execução do job em uma mesma janela.
        Para o processo de eliminação física dos arquivos, o método
        `purge_s3_path` do glueContext é utilizado.

        Examples:
            ```python
            # Eliminando partição física do S3
            partition_uri = "s3://some-bucket/some-table/anomesdia=20230101/"
            glue_manager.drop_partition(partition_uri)

            # O resultado é a exclusão física do prefixo anomesdia=20230101/
            # e todos os seus arquivos.
            ```

        Args:
            s3_partition_uri (str): URI da partição física localizada no s3.
            retention_period (int):
                Especifica o número de horas de retenção de dados.

        Raises:
            Exception: exceção genérica lançada caso o método\
            `glueContext.purge_s3_path()` não possa ser executado com sucesso.
        """

        logger.info(f"Verificando e eliminando (se existir) partição "
                    f"{s3_partition_uri}")
        try:
            self.glueContext.purge_s3_path(
                s3_path=s3_partition_uri,
                options={"retentionPeriod": retention_period}
            )
        except Exception as e:
            logger.error(f"Erro ao eliminar partição {s3_partition_uri}. "
                         f"Exception: {e}")
            raise e

    def write_and_catalog_data(
            self,
            df: DataFrame or DynamicFrame,
            s3_table_uri: str,
            output_database_name: str,
            output_table_name: str,
            partition_name: str = None,
            connection_type: str = "s3",
            update_behavior: str = "UPDATE_IN_DATABASE",
            compression: str = "snappy",
            enable_update_catalog: bool = True,
            output_data_format: str = "parquet"
    ) -> None:
        """
        Escrita de dados no S3 e catalogação no Data Catalog.

        Método responsável por consolidar todas as etapas necessárias para
        escrita de dados no s3 e a subsequente catalogação no Data Catalog.
        Em essência, este método realiza as seguintes operações:

        1. Verificação se o conjunto de dados fornecido como argumento
        é do tipo DynamicFrame (caso contrário, converte)
        2. Faz um sink com o catálogo de dado
        3. Escreve dados no s3 e atualiza catálogo de dados

        Examples:
            ```python
            # Escrevendo e catalogando dados
            glue_manager.write_data(
                df=df_orders,
                s3_table_uri="s3://some-bucket/some-table",
                output_database_name="db_corp_business_inteligence",
                output_table_name="tbl_orders_prep",
                partition_name="anomesdia"
            )
            ```

        Args:
            df (DataFrame or DynamicFrame):
                Objeto do tipo DataFrame Spark (`pyspark.sql.DataFrame`) ou
                DynamicFrame do Glue (`awsglue.dynamicframe.DynamicFrame`)
                alvo do processo de escrita e catalogação.

            s3_table_uri (str):
                URI da tabela a ser escrita no s3 no formato
                `s3://bucket-name/table-prefix/`. Esta informação é utilizada
                no parâmetro "path" do método `glueContext.getSink()`

            output_database_name (str):
                Referência nominal do database alvo do armazenamento da tabela.
                Esta informação é utilizada no parâmetro catalogDatabase do
                método `glueContext.getSink().setCatalogInfo()`.

            output_table_name (str):
                Referência nominal da tabela para armazenametno dos dados.
                Esta informação é utilizada no parâmetro catalogTableName do
                método `glueContext.getSink().setCatalogInfo()`.

            partition_name (str or None):
                Referência da coluna de partição utilizada para particionamento
                dos dados a serem armazenados. Esta informação é utilizada no
                parâmetro partitionKeys do método `glueContext.getSink()`.

            connection_type (str):
                Tipo de conexão utilizada para o armazenamento. Esta informação
                é utilizada no parâmetro connection_type do método
                `glueContext.getSink()`.

            update_behavior (str):
                Determina o comportamento de atualização dos dados da tabela
                alvo. Esta informação é utilizada no parâmetro updateBehavior
                do método `glueContext.getSink()`.

            compression (str):
                Tipo de compressão a ser utilizada no armazenamento dos dados.
                Esta informação é utilizada no parâmetro compression do
                método `glueContext.getSink()`.

            enable_update_catalog (bool):
                Flag para habilitar a atualização do catálogo de dados com os
                dados armazenados pelo método. Esta informação é utilizada no
                atributo enableUpdateCatalog do método `glueContext.getSink()`.

            output_data_format (str):
                Formato dos dados a serem armazenados. Esta informação é
                utilizada no parâmetro output_data_format do método
                `glueContext.getSink().setFormat()`.
        """

        # Convertendo DataFrame em DynamicFrame
        if type(df) == DataFrame:
            logger.info("Transformando DataFrame preparado em DynamicFrame")
            try:
                dyf = DynamicFrame.fromDF(df, self.glueContext, "dyf")
            except Exception as e:
                logger.error("Erro ao transformar DataFrame em DynamicFrame. "
                             f"Exception: {e}")
                raise e
        else:
            dyf = df

        # Criando sincronização com bucket s3
        logger.info("Preparando e sincronizando elementos de saída da tabela")
        try:
            # Criando relação de escrita de dados
            data_sink = self.glueContext.getSink(
                path=s3_table_uri,
                connection_type=connection_type,
                updateBehavior=update_behavior,
                partitionKeys=[partition_name],
                compression=compression,
                enableUpdateCatalog=enable_update_catalog,
                transformation_ctx="data_sink",
            )
        except Exception as e:
            logger.error("Erro ao configurar elementos de saída via getSink. "
                         f"Exception: {e}")
            raise e

        # Configurando informações do catálogo de dados
        logger.info("Adicionando entrada para tabela no catálogo de dados")
        try:
            data_sink.setCatalogInfo(
                catalogDatabase=output_database_name,
                catalogTableName=output_table_name
            )
            data_sink.setFormat(output_data_format,
                                useGlueParquetWriter=True)
            data_sink.writeFrame(dyf)

            logger.info(f"Tabela {output_database_name}."
                        f"{output_table_name} "
                        "atualizada com sucesso no catálogo. Seus dados estão "
                        f"armazenados em {s3_table_uri}")
        except Exception as e:
            logger.error("Erro ao adicionar entrada para tabela no catálogo "
                         f"de dados. Exception: {e}")
            raise e
