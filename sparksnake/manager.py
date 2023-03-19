"""Managing Spark operations in AWS services that uses it in ETL jobs.

This module aims to provide a central point for users to have a series of
useful features for developing their own Spark applications in AWS in services
like Glue and EMR.

___
"""

# Importing libraries
from time import sleep

from sparksnake.utils.log import log_config
from sparksnake.glue import GlueJobManager

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit


# Setting up a logger object
logger = log_config(logger_name=__file__)


class SparkETLManager(GlueJobManager):
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

    Example: Class `SparkETLManager` basic usage example with mode="glue"
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

        # Validando modo/serviço de execução da classe
        if self.mode == "glue":
            # Validar
            # Coletando argumentos necessários para mode="glue"
            argv_list = kwargs["argv_list"]
            data_dict = kwargs["data_dict"]

            # Herdando atributos e métodos da classe GlueJobManager
            GlueJobManager.__init__(self, argv_list=argv_list,
                                    data_dict=data_dict)

        # Se for local, instanciar uma sessão Spark como atributo da classe

    @staticmethod
    def extract_date_attributes(df: DataFrame,
                                date_col: str,
                                date_col_type: str = "date",
                                date_format: str = "yyyy-MM-dd",
                                convert_string_to_date: bool = True,
                                **kwargs) -> DataFrame:
        """
        Extração de atributos de datas de coluna de um DataFrame.

        Método responsável por consolidar a extração de diferentes atributos de
        data de um dado campo de um DataFrame informado pelo usuário. Tal campo
        pode ser do tipo primitivo DATE ou TIMESTAMP. Caso o campo alvo da
        extração de atributos de data passado pelo usuário seja do tipo STRING,
        porém com possibilidades de conversão para um tipo de data, então o
        parâmetro `convert_string_to_date` deve ser igual a `True`.


        A lógica estabelecida é composta por uma validação inicial de conversão
        do campo, seguida da extração de todos os atributos possíveis de data
        conforme argumentos fornecidos pelo usuário. Com essa funcionalidade,
        os usuários podem enriquecer seus DataFrames Spark com novos atributos
        relevantes para seus respectivos processos analíticos.

        Examples:
            ```python
            # Extraindo atributos temporais de uma coluna de data em um df
            df_date_prep = spark_manager.extract_date_attributes(
                df=df_raw,
                date_col="order_date",
                date_col_type="timestamp",
                year=True,
                month=True,
                dayofmonth=True
            )

            # O resultado será um novo DataFrame contendo três colunas
            # adicionais criadas com base no conteúdo de order_date:
            # year_order_date, month_order_date e dayofmonth_order_date
            ```

        Args:
            df (pyspark.sql.DataFrame):
                DataFrame Spark alvo das transformações aplicadas.

            date_col (str):
                Referência da coluna de data (ou string capaz de ser convertida
                como um campo de data) a ser utilizada no processo de extração
                de atributos temporais.

            date_col_type (str):
                String que informa se a coluna de data passada no argumento
                `date_col` é do tipo date ou timestamp.

            date_format (str):
                Formato de conversão de string para data. Aplicável caso
                `convert_string_to_date=True`

            convert_string_to_date (bool):
                Flag que indica a conversão da coluna `date_col` para um
                formato aceitável de data.

        Keyword Args:
            year (bool): Flag para extração do ano da coluna alvo
            quarter (bool): Flag para extração do quadrimestre da coluna alvo
            month (bool): Flag para extração do mês da coluna alvo
            dayofmonth (bool): Flag para extração do dia do mês da coluna alvo
            dayofweek (bool): Flag para extração do dia da semana da coluna
            weekofyear (bool): Flag para extração da semana do ano da coluna

        Raises:
            ValueError: exceção lançada caso o parâmetro date_col_type seja\
            informado como algo diferente de "date" ou "timestamp"

        Returns:
            DataFrame Spark com novas colunas baseadas nas extrações de\
            atributos temporais da coluna de data passada como alvo.
        """

        try:
            # Criando expressões de conversão com base no tipo do campo
            date_col_type = date_col_type.strip().lower()
            if convert_string_to_date:
                if date_col_type == "date":
                    conversion_expr = f"to_date({date_col},\
                        '{date_format}') AS {date_col}_{date_col_type}"
                elif date_col_type == "timestamp":
                    conversion_expr = f"to_timestamp({date_col},\
                        '{date_format}') AS {date_col}_{date_col_type}"
                else:
                    raise ValueError("Argumento date_col_type invalido! "
                                     "Insira 'date' ou 'timestamp'")

                # Aplicando consulta para transformação dos dados
                df = df.selectExpr(
                    "*",
                    conversion_expr
                ).drop(date_col)\
                    .withColumnRenamed(f"{date_col}_{date_col_type}", date_col)

        except Exception as e:
            logger.error('Erro ao configurar e realizar conversao de campo'
                         f'{date_col} para {date_col_type} via expressao'
                         f'{conversion_expr}. Exception: {e}')
            raise e

        # Criando lista de atributos possíveis de data a serem extraídos
        possible_date_attribs = ["year", "quarter", "month", "dayofmonth",
                                 "dayofweek", "dayofyear", "weekofyear"]

        try:
            # Iterando sobre atributos e construindo expressão completa
            for attrib in possible_date_attribs:
                if attrib in kwargs and bool(kwargs[attrib]):
                    df = df.withColumn(
                        f"{attrib}_{date_col}",
                        expr(f"{attrib}({date_col})")
                    )

            return df

        except Exception as e:
            logger.error('Erro ao adicionar colunas em DataFrame com'
                         f'novos atributos de data. Exception: {e}')
            raise e

    def extract_aggregate_statistics(self,
                                     df: DataFrame,
                                     numeric_col: str,
                                     group_by: str or list,
                                     round_result: bool = False,
                                     n_round: int = 2,
                                     **kwargs) -> DataFrame:
        """
        Extração de atributos estatísticos de coluna numérica.

        Método responsável por consolidar a extração de diferentes atributos
        estatísticos de uma coluna numérica presente em um DataFrame Spark.
        Com esta funcionalidade, os usuários podem obter uma série de atributos
        estatísticos enriquecidos em um DataFrame para futuras análises.

        Examples:
            ```python
            # Gerando estatísticas
            df_stats = spark_manager.extract_aggregate_statistics(
                df=df_orders,
                numeric_col="order_value",
                group_by=["order_id", "order_year"],
                sum=True,
                mean=True,
                max=True,
                min=True
            )

            # O resultado será um novo DataFrame com as colunas:
            # order_id e order_year (agrupamento)
            # sum_order_value (soma de order_value)
            # mean_order_value (média de order_value)
            # max_order_value (máximo de order_value)
            # min_order_value (mínimo de order_value)
            ```

        Args:
            df (pyspark.sql.DataFrame):
                DataFrame Spark alvo das transformações aplicadas.

            numeric_col (str):
                Referência da coluna numérica presente no DataFrame para servir
                como alvo das extrações estatísticas consideradas.

            group_by (str or list):
                Coluna nominal ou lista de colunas utilizadas como parâmetros
                do agrupamento consolidado pelas extrações estatísticas.

            round_result (bool):
                Flag para indicar o arredondamento dos valores resultantes
                dos atributos estatísticos gerados pelo agrupamento.

            n_round (int):
                Indicador de casas decimais de arredondamento. Aplicável caso
                `round_result=True`.

        Tip: Sobre os argumentos de chave e valor kwargs:
            Para proporcionar uma funcionalidade capaz de consolidar a
            extração dos atributos estatísticos mais comuns para os usuários
            com apenas uma linha de código, uma lista específica de funções
            foi selecionada para fazer parte dos métodos estatísticos aceitos
            neste método.

            A referência sobre qual estatística extrair pode ser fornecida
            pelo usuário através dos `**kwargs` em um formato de chave=valor,
            onde a chave referencia a referência nominal para a função
            estatística (em formato de string) e valor é um flag booleano.

            Em outras palavras, caso o usuário deseje enriquecer seu DataFrame
            com os valores da média, máximo e mínimo de um campo, o método
            deve ser parametrizado, para além dos argumentos detalhados,
            com `avg=True`, `min=True`, `max=True`.

            Detalhes sobre a lista de funções estatísticas aceitas neste
            método poderão ser vistas logo abaixo.

        Keyword Args:
            sum (bool): Extração da soma dos valores agregados
            mean (bool): Extração da média dos valores agregados
            max (bool): Extração do máximo do valor agregado
            min (bool): Extração do mínimo do valor agregado
            countDistinct (bool): Contagem distinta do valore agregado
            variance (bool): Extração da variância do valor agregado
            stddev (bool): Extração do Desvio padrão do valor agregado
            kurtosis (bool): Kurtosis (curtose) do valor agregado
            skewness (bool): Assimetria do valor agregado

        Returns:
            DataFrame Spark criado após processo de agregação estatística\
            parametrizado no método.

        Raises:
            Exception: exceção genérica lançada na impossibilidade de\
            gerar e executar a query SQL (SparkSQL) para extração de\
            atributos estatísticos do DataFrame.
        """

        # Desempacotando colunas de agrupamento em caso de múltiplas colunas
        if type(group_by) == list and len(group_by) > 1:
            group_by = ",".join(group_by)

        # Criando tabela temporária para extração de estatísticas
        df.createOrReplaceTempView("tmp_extract_aggregate_statistics")

        possible_functions = ["sum", "mean", "max", "min", "count",
                              "countDistinct", "variance", "stddev",
                              "kurtosis", "skewness"]
        try:
            # Iterando sobre atributos e construindo expressão completa
            agg_query = ""
            for f in possible_functions:
                if f in kwargs and bool(kwargs[f]):
                    if round_result:
                        agg_function = f"round({f}({numeric_col}), {n_round})"
                    else:
                        agg_function = f"{f}({numeric_col})"

                    agg_query += f"{agg_function} AS {f}_{numeric_col},"

            # Retirando última vírgula do comando de agregação
            agg_query = agg_query[:-1]

            # Construindo consulta definitiva
            final_query = f"""
                SELECT
                    {group_by},
                    {agg_query}
                FROM tmp_extract_aggregate_statistics
                GROUP BY
                    {group_by}
            """

            return self.spark.sql(final_query)

        except Exception as e:
            logger.error('Erro ao extrair estatísticas de DataFrame'
                         f'Exception: {e}')
            raise e

    @staticmethod
    def add_partition_column(df: DataFrame,
                             partition_name: str,
                             partition_value) -> DataFrame:
        """
        Adição de coluna de partição em um DataFrame Spark.

        Método responsável por adicionar uma coluna ao DataFrame alvo para
        funcionar como partição da tabela gerada. Na prática, este método
        utiliza o método `.withColumn()` do pyspark para adição de uma coluna
        considerando um nome de atributo (partition_name) e seu respectivo
        valor (partition_value).

        Examples
            ```python
            # Definindo informações de partição de data da tabela final
            partition_name = "anomesdia"
            partition_value = int(datetime.strftime('%Y%m%d'))

            # Adicionando coluna de partição a um DataFrame
            df_partitioned = spark_manager.add_partition(
                df=df_orders,
                partition_name=partition_name,
                partition_value=partition_value
            )

            # O resultado é um novo DataFrame contendo a coluna anomesdia
            # e seu respectivo valor definido utilizando a lib datetime
            ```

        Args:
            df (pyspark.sql.DataFrame): DataFrame Spark alvo.
            partition_name (str): Nome da coluna de partição a ser adicionada.
            partition_value (Any): Valor da partição atribuído à coluna.

        Returns:
            DataFrame Spark criado após processo de agregação estatística\
            parametrizado no método.

        Raises:
            Exception: exceção genérica lançada ao obter um erro na tentativa\
            de execução do método `.withColumn()` para adição da coluna de\
            partição no DataFrame alvo.
        """

        logger.info("Adicionando coluna de partição no DataFrame final "
                    f"({partition_name}={str(partition_value)})")
        try:
            df_partitioned = df.withColumn(partition_name,
                                           lit(partition_value))
        except Exception as e:
            logger.error("Erro ao adicionar coluna de partição ao DataFrame "
                         f"via método .withColumn(). Exception: {e}")

        # Retornando DataFrame com coluna de partição
        return df_partitioned

    @staticmethod
    def repartition_dataframe(df: DataFrame, num_partitions: int) -> DataFrame:
        """
        Reparticionamento de um DataFrame para otimização de armazenamento.

        Método responsável por aplicar processo de reparticionamento de
        DataFrames Spark visando a otimização do armazenamento dos arquivos
        físicos no sistema de armazenamento distribuído. O método contempla
        algumas validações importantes em termos do uso dos métodos
        `coalesce()` e `repartition()` com base no número atual das partições
        existentes no DataFrame passado como argumento.

        Tip: Detalhes adicionais sobre o funcionamento do método:
            Seguindo as melhores práticas de reparticionamento, caso
            o número desejado de partições (num_partitions) seja MENOR
            que o número atual de partições coletadas, então o método
            utilizado será o `coalesce()`.

            Por outro lado, caso o número desejado de partições seja MAIOR que
            o número atual, então o método utilizado será o `repartition()`,
            considerando a escrita de uma mensagem de log com a tag warning
            para o usuário, dados os possíveis impactos. Por fim, se o número
            de partições desejadas for igual ao número atual, nenhuma
            operação é realizada e o DataFrame original é retornado
            intacto ao usuário.

        Examples:
            ```python
            # Reparticionando DataFrame para otimizar armazenamento
            df_repartitioned = spark_manager.repartition_dataframe(
                df=df_orders,
                num_partitions=10
            )
            ```

        Args:
            df (pyspark.sql.DataFrame): DataFrame Spark alvo do processo.
            num_partitions (int): Número de partições físicas desejadas.

        Returns:
            DataFrame Spark reparticionado de acordo com o número de\
            partições especificadas pelo usuário.

        Raises:
            Exception: exceção genérica lançada ao obter um erro na tentativa\
            de realizar os procedimentos de reparticionamento de um DataFrame,\
            seja este dado pelo método `colaesce()` ou pelo método\
            `repartition()`
        """

        # Corrigindo argumento num_partitions
        num_partitions = int(num_partitions)

        # Coletando informações atuais de partições físicas do DataFrame
        logger.info("Coletando o número atual de partições do DataFrame")
        try:
            current_partitions = df.rdd.getNumPartitions()

        except Exception as e:
            logger.error("Erro ao coletar número atual de partições do "
                         "DataFrame via df.rdd.getNumPartitions. "
                         f"Exception: {e}")
            raise e

        # Se o número de partições desejadas for igual ao atual, não operar
        if num_partitions == current_partitions:
            logger.warning(f"Número de partições atuais ({current_partitions})"
                           f" é igual ao número de partições desejadas "
                           f"({num_partitions}). Nenhuma operação de "
                           "repartitionamento será realizada e o DataFrame "
                           "original será retornado sem alterações")
            sleep(0.01)
            return df

        # Se o número de partições desejadas for MENOR, usar coalesce()
        elif num_partitions < current_partitions:
            logger.info("Iniciando reparticionamento de DataFrame via "
                        f"coalesce() de {current_partitions} para "
                        f"{num_partitions} partições")
            try:
                df_repartitioned = df.coalesce(num_partitions)

            except Exception as e:
                logger.warning("Erro ao reparticionar DataFrame via "
                               "repartition(). Nenhuma operação de "
                               "repartitionamento será realizada e "
                               "o DataFrame original será retornado sem "
                               f"alterações. Exceção: {e}")
                return df

        # Se o número de partições desejadas for MAIOR, utilizar repartition()
        elif num_partitions > current_partitions:
            logger.warning("O número de partições desejadas "
                           f"({num_partitions})  é maior que o atual "
                           f"({current_partitions}) e, portanto, a operação "
                           "deverá ser realizada pelo método repartition(), "
                           "podendo impactar diretamente na performance do "
                           "processo dada a natureza do método e sua "
                           "característica de full shuffle. Como sugestão, "
                           "avalie se as configurações de reparticionamento "
                           "realmente estão conforme o esperado.")
            try:
                df_repartitioned = df.repartition(num_partitions)

            except Exception as e:
                logger.warning("Erro ao reparticionar DataFrame via "
                               "repartition(). Nenhuma operação de "
                               "repartitionamento será realizada e "
                               "o DataFrame original será retornado sem "
                               f"alterações. Exceção: {e}")
                return df

        # Validando possível inconsistência no processo de reparticionamento
        updated_partitions = df_repartitioned.rdd.getNumPartitions()
        if updated_partitions != num_partitions:
            logger.warning(f"Por algum motivo desconhecido, o número de "
                           "partições atual do DataFrame "
                           f"({updated_partitions}) não corresponde ao "
                           f"número estabelecido ({num_partitions}) mesmo "
                           "após o processo de reparticionamento ter sido "
                           "executado sem nenhuma exceção lançada. "
                           "Como sugestão, investigue se o Glue possui alguma "
                           "limitação de transferência de arquivos entre os "
                           "nós dadas as características das origens usadas.")
            sleep(0.01)

        return df_repartitioned
