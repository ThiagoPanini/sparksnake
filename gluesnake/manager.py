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

from pyspark.context import SparkContext


# Configurando objeto de logger
logger = log_config(logger_name=__file__)


# Classe para gerenciamento de insumos de um job Glue
class GlueJobManager():
    """Gerenciamento de insumos característicos de jobs Glue.

    Classe responsável por gerenciar e fornecer todos os insumos
    necessários para a utilização de um job do Glue na dinâmica
    de processamento de dados. Com as funcionalidades desta classe,
    os usuários podem usufruir de um ponto de entrada amigável para
    obter elementos essenciais para a execução de jobs Glue, como
    por exemplo, variáveis de contexto (GlueContext, SparkContext)
    e sessão (GlueCntext.spark_session).

    Example: Exemplo básico de utilização da classe GlueJobManager
        ```python
        # Importando classe
        from gluesnake.manager import GlueJobManager

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
        job_manager = GlueJobManager(ARGV_LIST, DATA_DICT)
        job = job_manager.init_job()
        ```

    Args:
        argv_list (list):
            Lista contendo os nomes dos argumentos utilizados
            no job.

        data_dict (dict):
            Dicionário contendo todas as referências de origens
            de dados utilizadas nos processos de transformação
            consolidados no job. Navegue até o exemplo acima para
            visualizar detalhes adicionais.

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
        try:
            args_formatted = "".join([f'--{k}="{v}"\n'
                                      for k, v in self.args.items()])
            logger.info(f"Argumentos do job:\n\n{args_formatted}")
            sleep(0.01)
        except Exception as e:
            logger.error("Erro ao retornar argumentos do job dentro da "
                         f"lista informada. Exception: {e}")
            raise e

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

    def init_job(self) -> Job:
        """
        Inicializando objeto de job a partir de contexto do Glue.

        Método criado para consolidar todas as etapas de inicialização
        de um job do Glue a partir da visualização dos argumentos e
        obtenção dos elementos de contexto e sessão. Com a execução
        deste método, o usuário poderá ter uma porta única de entrada
        para todos os processos relativamente burocráticos de configuração
        de um job do Glue.

        Returns:
            Elemento do tipo awsglue.job.Job para consolidar ações\
            relacionadas à processos de jobs do Glue
        """
        # Obtendo argumentos e consolidando mensagens de log
        self.job_initial_log_message()
        self.print_args()

        # Obtendo elementos de sessão e conteto Spark e Glue
        self.get_context_and_session()

        # Inicializando objeto de Job do Glue
        job = Job(self.glueContext)
        job.init(self.args['JOB_NAME'], self.args)

        return job
