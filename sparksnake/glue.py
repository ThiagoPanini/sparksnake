# Importando bibliotecas
import sys
from time import sleep
from sparksnake.utils.log import log_config

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import DataFrame


# Configurando objeto de logger
logger = log_config(logger_name=__file__)


# Classe para gerenciamento de insumos de um job Glue
class GlueJobManager():
    """Gerenciamento de insumos característicos de jobs Glue.

    Classe responsável por gerenciar e fornecer todos os insumos
    necessários para a utilização de um job do Glue na dinâmica
    de processamento de dados. Em essência, todos os atributos e
    métodos declarados na classe `GlueJobManager` são *herdados*
    na classe `SparkETLManager`, também definida neste mesmo módulo.

    Para visualizar um exemplo básico de utilização, consulte a
    documentação da classe `SparkETLManager`.

    Args:
        argv_list (list):
            Lista contendo os nomes dos argumentos utilizados
            no job.

        data_dict (dict):
            Dicionário contendo todas as referências de origens
            de dados utilizadas nos processos de transformação
            consolidados no job. Navegue até o exemplo de uso
            da classe SparkETLManager para detalhes adicionais.

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
        """Obtendo e logando argumentos do job.

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
        """Obtendo elementos de contexto e sessão da aplicação.

        Método responsável por criar e associar atributos da classe
        para os elementos SparkContext, GlueContext e SparkSession.
        """
        logger.info("Criando SparkContext, GlueContext e SparkSession")
        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session

    def init_job(self):
        """Inicializando objeto de job a partir de contexto do Glue.

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

    def generate_dynamicframes_dict(self) -> dict:
        """Gerando dicionário de DynamicFrames do projeto.

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

        Examples:
            ```python
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

        Tip: Detalhes sobre o dicionário de DynamicFrames resultante
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

        Examples
            ```python
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

        Tip: Detalhes sobre o dicionário de DataFrames resultante
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
