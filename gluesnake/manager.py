"""Gerenciamento de operações no serviço AWS Glue.

Módulo criado para consolidar classes, métodos, atributos, funções e
implementações no geral que possam facilitar a construção de jobs Glue
utilizando a linguagem Python como principal ferramenta.

___
"""

# Importando bibliotecas
import sys
from time import sleep
from gluesnake.utils.log import log_config

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit


# Configurando objeto de logger
logger = log_config(logger_name=__file__)


# Classe para gerenciamento de insumos de um job Glue
class GlueJobManager():
    """Gerenciamento de insumos característicos de jobs Glue.

    Classe responsável por gerenciar e fornecer todos os insumos
    necessários para a utilização de um job do Glue na dinâmica
    de processamento de dados. Em essência, todos os atributos e
    métodos declarados na classe `GlueJobManager` são *herdados*
    na classe `GlueETLManager`, também definida neste mesmo módulo.

    Para visualizar um exemplo básico de utilização, consulte a
    documentação da classe `GlueETLManager`.

    Args:
        argv_list (list):
            Lista contendo os nomes dos argumentos utilizados
            no job.

        data_dict (dict):
            Dicionário contendo todas as referências de origens
            de dados utilizadas nos processos de transformação
            consolidados no job. Navegue até o exemplo de uso
            da classe GlueETLManager para detalhes adicionais.

    A classe considera a obtenção de alguns atributos de forma
    interna através da execução de outros métodos e funções,
    sendo eles:

    Attributes:
        args (dict):
            Dicionário de argumentos de job listados pelo usuário
            adicionados aos argumentos dos sitema (`sys.argv`)
            passados automaticamente ao runner do job.

        sc (SparkContext):
            Contexto Spark utilizado para criação de contexto do Glue

        glueContext (GlueContext):
            Contexto Glue utilizado para criação de sessão Spark

        spark (SparkSession):
            Sessão Spark utilizada como ponto central de operações
            realizadas no job
    """

    def __init__(self, argv_list: list, data_dict: dict) -> None:
        self.argv_list = argv_list
        self.data_dict = data_dict

        # Obtendo argumentos do job
        self.args = getResolvedOptions(sys.argv, self.argv_list)

    def job_initial_log_message(self) -> None:
        """Preparação e log de mensagem de início de jobs Glue.

        Método responsável por compor uma mensagem inicial de log a ser
        consolidada no CloudWatch. A mensagem visa declarar todas as
        origens utilizadas no job Glue, além de fornecer detalhes
        sobre os push down predicates (se utilizados) em cada
        processo de leitura de dados.
        """
        # Definindo strings iniciais para composição da mensagem
        welcome_msg = f"Iniciando execução de job {self.args['JOB_NAME']}. "\
                      "Origens presentes no processo de ETL:\n\n"
        initial_msg = ""

        # Iterando sobre dicionário de dados para extração de parâmetros
        for _, params in self.data_dict.items():
            # Obtendo tabela e iniciando construção da mensagem
            tbl_ref = f"{params['database']}.{params['table_name']}"
            table_msg = f"Tabela {tbl_ref} "

            # Validando existência de push_down_predicate
            if "push_down_predicate" in params:
                table_msg += "com push down predicate definido por "\
                             f"{params['push_down_predicate']}\n"
            else:
                table_msg += "sem push down predicate definido\n"

            # Concatenando mensagem final
            initial_msg += table_msg

        # Adicionando mensagem de boas vindas
        logger.info(welcome_msg + initial_msg)

    def print_args(self) -> None:
        """
        Obtendo e logando argumentos do job.

        Método responsável por mostrar ao usuário, como uma mensagem
        de log, todos os argumentos utilizados no referido job e
        seus respectivos valores.
        """

        # Formatando e logando argumentos do job
        args_formatted = "".join([f'--{k}="{v}"\n'
                                  for k, v in self.args.items()])
        logger.info(f"Argumentos do job:\n\n{args_formatted}")
        sleep(0.01)

    def get_context_and_session(self) -> None:
        """
        Obtendo elementos de contexto e sessão da aplicação.

        Método responsável por criar e associar atributos da classe
        para os elementos SparkContext, GlueContext e SparkSession.
        """
        logger.info("Criando SparkContext, GlueContext e SparkSession")
        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session

    def init_job(self):
        """
        Inicializando objeto de job a partir de contexto do Glue.

        Método criado para consolidar todas as etapas de inicialização
        de um job do Glue a partir da visualização dos argumentos e
        obtenção dos elementos de contexto e sessão. Com a execução
        deste método, o usuário poderá ter uma porta única de entrada
        para todos os processos relativamente burocráticos de configuração
        de um job do Glue.
        """
        # Obtendo argumentos e consolidando mensagens de log
        self.job_initial_log_message()
        self.print_args()

        # Obtendo elementos de sessão e conteto Spark e Glue
        self.get_context_and_session()

        # Inicializando objeto de Job do Glue
        self.job = Job(self.glueContext)
        self.job.init(self.args['JOB_NAME'], self.args)


# Classe para o gerenciamento de transformações Spark em um job
class GlueETLManager(GlueJobManager):
    """
    Gerenciamento de métodos úteis para transformações no Glue.

    Classe responsável por gerenciar e fornecer métodos típicos
    de transformação de um job do Glue a serem detalhadamente
    adaptados por seus usuários para que as operações nos dados
    possam ser aplicadas de acordo com as necessidades exigidas.

    Considerando a relação de herança entre as classes estabelecidas,
    o usuário final poderá utilizar apenas a classe GlueETLManager
    para toda e qualquer operação utilizando as funcionalidades
    da biblioteca gluesnake.

    Example: Exemplo básico de utilização da classe `GlueETLManager`
        ```python
        # Importando classe
        from gluesnake.manager import GlueETLManager

        # Definindo argumentos do job
        ARGV_LIST = ["JOB_NAME", "S3_OUTPUT_PATH"]

        # Definindo dicionário de origens do processo
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

        # Instanciando classe e inicializando job
        glue_manager = GlueETLManager(ARGV_LIST, DATA_DICT)
        glue_manager.init_job()

        # Obtendo DataFrames Spark com base em dicionário DATA_DICT mapeado
        dfs_dict = glue_manager.generate_dataframes_dict()

        # Desempacotando DataFrames através de dicionário obtido
        df_orders = dfs_dict["orders"]

        # Eliminando partição de data (caso existente)
        glue_manager.drop_partition(
            partition_name="anomesdia",
            partition_value="20230101"
        )

        # Adicionando coluna de partição ao DataFrame
        df_orders_partitioned = glue_manager.add_partition(
            partition_name="anomesdia",
            partition_value="20230101"
        )

        # Reparticionando DataFrame para otimizar armazenamento
        df_orders_repartitioned = glue_manager.repartition_dataframe(
            df=df_orders_partitioned,
            num_partitions=10
        )

        # Escrevendo dados no S3 e catalogando no Data Catalog
        glue_manager.write_data_to_catalog(df=df_orders_repartitioned)

        # Commitando job
        glue_manager.job.commit()
        ```

    Args:
        argv_list (list):
            Lista contendo os nomes dos argumentos utilizados
            no job.

        data_dict (dict):
            Dicionário contendo todas as referências de origens
            de dados utilizadas nos processos de transformação
            consolidados no job.

    Note: Sobre as possibilidades de configuração do dicionário data_dict
        O dicionário data_dict, passado como parâmetro de inicialização
        da classe `GlueETLManager` (e também da classe `GlueJobManager`),
        por natureza, deve ser definido de acordo com algumas premissas.

        Sua principal função é proporcionar uma forma única de mapear a leitura
        de todas as origens utilizadas no job. Dessa forma, sua composição é
        fundamental para garantir que os processos de leitura de dados sejam
        realizados com sucesso, independente do formato (DynamicFrame ou
        DataFrame Spark).

        Dessa forma, o conteúdo do dicionário data_dict está habilitado para
        suportar toda e qualquer possibilidade presente no método nativo
        `glueContext.create_dynamic_frame.from_catalog` utilizado pelo Glue
        para leitura de DynamicFrames.

        :star: Em outras palavras, caso o usuário queira configurar o
        dicionário data_dict para leitura de uma tabela particionada, a chave
        push_down_predicate pode ser inclusa no dicionário com o devido valor
        a ser utilizado na filtragem. E assim, outros parâmetros como, por
        exemplo, additional_options e catalog_id, podem ser mapeados como
        chaves do dicionário data_dict para endereçar as mais variadas
        possibilidades de mapeamento de origens no Glue.

        Caso algum parâmetro aceito pelo método
        `glueContext.create_dynamic_frame.from_catalog` não seja inserido como
        chave do dicionário data_dict, seu respectivo valor _default_ será
        considerado nos processos de leitura internos da classe.
    """

    def __init__(self, argv_list: list, data_dict: dict) -> None:
        self.argv_list = argv_list
        self.data_dict = data_dict

        # Herdando atributos de classe de gerenciamento de job
        GlueJobManager.__init__(self, argv_list=self.argv_list,
                                data_dict=self.data_dict)

        # Gerando URI de tabela no s3 caso existam alguns argumentos
        self.s3_table_uri = f"s3://{self.args['OUTPUT_BUCKET']}/"\
            f"{self.args['OUTPUT_TABLE']}"

    def generate_dynamicframes_dict(self) -> dict:
        """
        Gerando dicionário de DynamicFrames do projeto.

        Método responsável por utilizar o atributo data_dict da classe
        para leitura e obtenção de todos os DynamicFrames configurados
        no referido dicionário de acordo com as especificações
        fornecidas. A grande vantagem deste método é a disponibilização
        dos DynamicFrames como elementos de um dicionário Python que,
        posteriormente, podem ser acessados em outras etapas do código
        para o mapeamento das operações. Esta dinâmica evita que o
        usuário precise codificar um bloco específico de leitura para
        cada origem de dados do job, possibilitando que o usuário apenas
        acesse cada uma das suas origens através de uma indexação.

        Example: Exemplo de aplicação do método
            ```python
            # Importando classe e inicializando objeto
            from gluesnake.manager import GlueETLManager
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Obtendo lista de dynamicframes Glue
            dyfs_dict = glue_manager.generate_dynamicframes_dict()

            # Desempacotando dicionário para obter origens individualmente
            dyf_orders = dyfs_dict["orders"]
            dyf_customers = dyfs_dict["customers"]
            ```

        Returns:
            Dicionário Python contendo um mapeamento de cada uma das\
            origens configuradas no atributo self.data_dict e seus\
            respectivos objetos do tipo DynamicFrame.

        Note: Detalhes sobre o dicionário de DynamicFrames resultante
            Para proporcionar uma visão clara sobre o retorno deste método,
            considere, como exemplo, a seguinte configuração para o atributo
            `self.data_dict` utilizado na inicialização da classe:

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

            Todos os parâmetros presentes no método
            `glueContext.create_dynamic_frame.from_catalog()` são
            aceitos na construção do dicionário `self.data_dict`.
            Além disso, alguns parâmetros adicionais foram inclusos
            visando proporcionar uma maior facilidade aos usuários,
            como por exemplo:

            - "create_temp_view": bool -> configura a criação
                de uma tabela temporária (view) para a tabela
                em questão

            O retorno do método `generate_dynamicframes_dict()` será
            no seguinte formato:

            ```python
            {
                "orders": <DynamicFrame>
                "customers": <DynamicFrame>
            }
            ```

            onde as tags <DynamicFrame> representam o objeto do tipo
            DynamicFrame lido para cada origem, utilizando as
            configurações apresentadas no dicionário do atributo
            self.data_dict e disponibilizado ao usuário dentro da
            respectiva chave que representa a origem.
        """

        logger.info("Iterando sobre dicionário de dados fornecido para "
                    "leitura de DynamicFrames do Glue")
        try:
            dynamic_frames = []
            for t in self.data_dict.keys():
                # Coletando argumentos obrigatórios: database, table_name, ctx
                database = self.data_dict[t]["database"]
                table_name = self.data_dict[t]["table_name"]
                transformation_ctx = self.data_dict[t]["transformation_ctx"]

                # Coletando argumento não obrigatório: push_down_predicate
                push_down_predicate = self.data_dict[t]["push_down_predicate"]\
                    if "push_down_predicate" in self.data_dict[t].keys()\
                    else ""

                # Coletando argumento não obrigatório: additional_options
                additional_options = self.data_dict[t]["additional_options"] \
                    if "additional_options" in self.data_dict[t].keys()\
                    else {}

                # Coletando argumento não obrigatório: catalog_id
                catalog_id = self.data_dict[t]["catalog_id"] \
                    if "catalog_id" in self.data_dict[t].keys()\
                    else None

                # Lendo DynamicFrame
                dyf = self.glueContext.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table_name,
                    transformation_ctx=transformation_ctx,
                    push_down_predicate=push_down_predicate,
                    additional_options=additional_options,
                    catalog_id=catalog_id
                )

                # Adicionando à lista de DynamicFrames
                dynamic_frames.append(dyf)

        except Exception as e:
            logger.error("Erro ao gerar lista de DynamicFrames com base "
                         f"em dicionário. Exception: {e}")
            raise e

        logger.info("Mapeando DynamicFrames às chaves do dicionário")
        sleep(0.01)

        # Criando dicionário de Dynamic Frames
        dynamic_dict = {k: dyf for k, dyf
                        in zip(self.data_dict.keys(), dynamic_frames)}
        logger.info("Dados gerados com sucesso. Total de DynamicFrames: "
                    f"{len(dynamic_dict.values())}")

        # Retornando dicionário de DynamicFrames
        sleep(0.01)
        return dynamic_dict

    def generate_dataframes_dict(self) -> dict:
        """
        Gerando dicionário de DataFrames Spark do projeto.

        Método responsável por consolidar os processos necessários
        para disponibilização, ao usuário, de objetos do tipo DataFrame
        Spark capazes de serem utilizados nas mais variadas etapas
        de transformação do job Glue. Na prática, este método chama o
        método `generate_dynamic_frames_dict()` para coleta dos objetos do
        tipo DynamicFrame (ver documentação acima) e, na sequência, aplica
        o método `.toDF()` para transformação de tais objetos em objetos
        do tipo DataFrame Spark.

        Example: Exemplo de aplicação do método
            ```python
            # Importando classe e inicializando objeto
            from gluesnake.manager import GlueETLManager
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Obtendo lista de DataFrames Spark
            dfs_dict = glue_manager.generate_dataframes_dict()

            # Desempacotando dicionário para obter origens individualmente
            df_orders = dfs_dict["orders"]
            df_customers = dfs_dict["customers"]
            ```

        Returns:
            Dicionário Python contendo um mapeamento de cada uma das\
            origens configuradas no atributo self.data_dict e seus\
            respectivos objetos do tipo DataFrame.

        Note: Detalhes sobre o dicionário de DataFrames resultante
            Para proporcionar uma visão clara sobre o retorno deste método,
            considere, como exemplo, a seguinte configuração para o atributo
            `self.data_dict` utilizado na inicialização da classe:

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

            Todos os parâmetros presentes no método
            `glueContext.create_dynamic_frame.from_catalog()` são
            aceitos na construção do dicionário `self.data_dict`.
            Além disso, alguns parâmetros adicionais foram inclusos
            visando proporcionar uma maior facilidade aos usuários,
            como por exemplo:

            - "create_temp_view": bool -> configura a criação
                de uma tabela temporária (view) para a tabela
                em questão

            O retorno do método `generate_dataframes_dict()` será
            no seguinte formato:

            ```python
            {
                "orders": <DataFrame>
                "customers": <DataFrame>
            }
            ```

            onde as tags <DataFrame> representam o objeto do tipo
            DataFrame lido para cada origem, utilizando as
            configurações apresentadas no dicionário do atributo
            self.data_dict e disponibilizado ao usuário dentro da
            respectiva chave que representa a origem.
        """

        # Gerando dicionário de DynamicFrames
        dyf_dict = self.generate_dynamicframes_dict()

        # Transformando DynamicFrames em DataFrames
        logger.info(f"Transformando os {len(dyf_dict.keys())} "
                    "DynamicFrames em DataFrames Spark")
        try:
            df_dict = {k: dyf.toDF() for k, dyf in dyf_dict.items()}
            logger.info("DataFrames Spark gerados com sucesso")
            sleep(0.01)

        except Exception as e:
            logger.error("Erro ao transformar DynamicFrames em "
                         f"DataFrames Spark. Exception: {e}")
            raise e

        # Iterando sobre dicionário de dados para validar criação de temp views
        for table_key, params in self.data_dict.items():
            try:
                # Extraindo variáveis
                df = df_dict[table_key]
                table_name = params["table_name"]

                # Criando tabela temporária (se aplicável)
                if "create_temp_view" in params\
                        and bool(params["create_temp_view"]):
                    df.createOrReplaceTempView(table_name)

                    logger.info(f"Tabela temporária (view) {table_name} "
                                "criada com sucesso.")

            except Exception as e:
                logger.error("Erro ao criar tabela temporária "
                             f"{table_name}. Exception: {e}")
                raise e

        # Retornando dicionário de DataFrames Spark convertidos
        sleep(0.01)
        return df_dict

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

        Example: Exemplo de aplicação do método
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Extraindo atributos temporais de uma coluna de data em um df
            df_date_prep = glue_manager.extract_date_attributes(
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

    def extract_aggregate_statistics(self, df: DataFrame,
                                     numeric_col: str,
                                     group_by: str | list,
                                     round_result: bool = False,
                                     n_round: int = 2,
                                     **kwargs) -> DataFrame:
        """
        Extração de atributos estatísticos de coluna numérica.

        Método responsável por consolidar a extração de diferentes atributos
        estatísticos de uma coluna numérica presente em um DataFrame Spark.
        Com esta funcionalidade, os usuários podem obter uma série de atributos
        estatísticos enriquecidos em um DataFrame para futuras análises.

        Example: Exemplo de aplicação do método:
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Lendo e desempacotando DataFrames
            dfs_dict = glue_manager.generate_dataframes_dict()
            df_orders = dfs_dict["orders"]

            # Gerando estatísticas
            df_stats = glue_manager.extract_aggregate_statistics(
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

        Example: Exemplo de aplicação do método:
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

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

    @staticmethod
    def add_partition(df: DataFrame,
                      partition_name: str,
                      partition_value) -> DataFrame:
        """
        Adição de coluna de partição em um DataFrame Spark.

        Método responsável por adicionar uma coluna ao DataFrame alvo para
        funcionar como partição da tabela gerada. Na prática, este método
        utiliza o método `.withColumn()` do pyspark para adição de uma coluna
        considerando um nome de atributo (partition_name) e seu respectivo
        valor (partition_value).

        Example: Exemplo de aplicação do método:
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager
            from datetime import datetime

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Lendo e desempacotando DataFrames
            dfs_dict = glue_manager.generate_dataframes_dict()
            df_orders = dfs_dict["orders"]

            # Definindo informações de partição de data da tabela final
            partition_name = "anomesdia"
            partition_value = int(datetime.strftime('%Y%m%d'))

            # Adicionando coluna de partição a um DataFrame
            df_partitioned = glue_manager.add_partition(
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

        Example: Exemplo de aplicação do método:
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager
            from datetime import datetime

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Lendo e desempacotando DataFrames
            dfs_dict = glue_manager.generate_dataframes_dict()
            df_orders = dfs_dict["orders"]

            # Reparticionando DataFrame para otimizar armazenamento
            df_repartitioned = glue_manager.repartition_dataframe(
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

    def write_data_to_catalog(
            self,
            df: DataFrame | DynamicFrame,
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

        Example: Exemplo de aplicação do método:
            ```python
            # Importando classe
            from gluesnake.manager import GlueETLManager
            from datetime import datetime

            # Inicializando objeto da classe
            glue_manager = GlueETLManager(argv_list, data_dict)

            # Lendo e desempacotando DataFrames
            dfs_dict = glue_manager.generate_dataframes_dict()
            df_orders = dfs_dict["orders"]

            # Escrevendo e catalogando dados
            glue_manager.write_data_to_catalog(
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
