"""Consolidação de testes unitários da classe GlueJobManager.

Neste arquivo, serão consolidados testes unitários
utilizados para validar as funcionalidades presentes na
classe GlueJobManager, visando garantir que todos os insumos
essenciais para execução de jobs do Glue na AWS estejam
sendo entregues com eficiência ao usuário.

___
"""

# Importando bibliotecas
import pytest
import logging

from tests.helpers.user_inputs import FAKE_ARGV_LIST

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job


@pytest.mark.glue_job_manager
def test_atributos_args_contem_um_dicionario_de_argumentos_do_job(
    job_manager
):
    """
    G: dado que o usuário deseja inicializar um job do Glue na AWS
    W: quando a classe GlueJobManager for instanciada
    T: o objeto resultante deve conter um atributo chamado args que
       contém um dicionário Python com argumentos do job
    """

    assert type(job_manager.args) is dict


@pytest.mark.glue_job_manager
def test_atributos_args_contem_argumentos_definidos_pelo_usuario(
    job_manager
):
    """
    G: dado que o usuário deseja inicializar um job do Glue na AWS
    W: quando a classe GlueJobManager for instanciada
    T: a lista de argumentos definida pelo usuário utilizada para
       criar a instância de objeto deve estar contida no atributo
       self.args
    """

    # Coletando argumentos do job obtidos via getResolvedOptions
    job_arg_names = list(job_manager.args.keys())

    assert all(a in job_arg_names for a in FAKE_ARGV_LIST)


@pytest.mark.glue_job_manager
@pytest.mark.job_inital_log_message
def test_mensagem_de_log_contendo_detalhes_sobre_as_origens_do_job(
    job_manager, caplog
):
    """
    G: dado que o usuário deseja iniciar um job Glue na AWS
    W: quando o método job_initial_message() for executado a
       partir de um objeto da classe GlueJobManager ou de outro
       objeto que herde suas funcionalidades
    T: então a mensagem de log capturada deve condizer com as
       características das origens definidas no atributo data_dict
       da classe
    """

    # Definindo mensagem de log esperada do método
    expected_log_msg = "Initializing the execution of a-fake-arg-value job. "\
        "Data sources used in this ETL process:\n\n"\
        "Table some-fake-database.orders-fake-table without push down "\
        "predicate\n"\
        "Table some-fake-database.customers-fake-table with the following "\
        "push down predicate info: anomesdia=20221201\n"

    # Executando método para escrita de mensagem de log
    with caplog.at_level(logging.INFO):
        job_manager.job_initial_log_message()

    assert caplog.records[-1].message == expected_log_msg


@pytest.mark.glue_job_manager
@pytest.mark.print_args
def test_mensagem_de_log_contendo_argumentos_do_job(
    job_manager, caplog
):
    """
    G: dado que o usuário deseja iniciar um job Glue na AWS
    W: quando o método print_args() for executado a
       partir de um objeto da classe GlueJobManager ou de outro
       objeto que herde suas funcionalidades
    T: então a mensagem de log capturada deve conter minimamente
       os argumentos definidos pelo usuário em self.argv_list
    """

    # Preparando validador de mensagem de log com argumentos
    expected_argv_msg = [f'--{arg}="a-fake-arg-value"'
                         for arg in FAKE_ARGV_LIST]

    # Executando método para escrita de mensagem de log
    with caplog.at_level(logging.INFO):
        job_manager.print_args()

    assert all(a in caplog.text.split("\n") for a in expected_argv_msg)


@pytest.mark.glue_job_manager
@pytest.mark.get_context_and_session
def test_obtencao_de_elementos_de_contexto_e_sessao(job_manager):
    """
    G: dado que o usuário deseja obter os elementos de contexto e sessão no Job
    W: quando o método get_context_and_session() for executado
    T: então os atributos self.sc, self.glueContext e self.spark devem existir
       na classe GlueJobManager
    """

    # Executando método de coleta de elementos de contexto e sessão do job
    job_manager.get_context_and_session()

    # Coletando atributos da classe e definindo lista de validação
    class_attribs = job_manager.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Validando se a lista de atributos existem na classe
    assert all(a in list(class_attribs.keys()) for a in attribs_names)


@pytest.mark.glue_job_manager
@pytest.mark.get_context_and_session
def test_tipos_primitivos_de_elementos_de_contexto_e_sessao(job_manager):
    """
    G: dado que o usuário deseja obter os elementos de contexto e sessão no Job
    W: quando o método get_context_and_session() for executado
    T: então os atributos self.sc, self.glueContext e self.spark devem
       representar elementos do tipo SparkContext, GlueContext e SparkSession,
       respectivamente
    """

    # Executando método de coleta de elementos de contexto e sessão do job
    job_manager.get_context_and_session()

    # Coletando atributos da classe e definindo lista de validação
    class_attribs = job_manager.__dict__

    # Validando tipos primitivos dos atributos de contexto e sessão
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.glue_job_manager
@pytest.mark.init_job
def test_metodo_de_inicializacao_do_job_gera_contexto_e_sessao(job_manager):
    """
    G: dado que deseja-se inicializar um job Glue pela classe GlueJobManager
    W: quando o método init_job() for chamado
    T: então os elementos de contexto e sessão (Spark e Glue) devem existir
       e apresentarem os tipos primitivos adequados
    """

    # Executando método de inicialização do job
    job_manager.init_job()

    # Coletando atributos da classe e definindo lista de validação
    class_attribs = job_manager.__dict__
    attribs_names = ["sc", "glueContext", "spark"]

    # Validando existência de atributos da classe
    assert all(a in list(class_attribs.keys()) for a in attribs_names)

    # Validando tipos primitivos dos atributos de contexto e sessão
    assert type(class_attribs["sc"]) == SparkContext
    assert type(class_attribs["glueContext"]) == GlueContext
    assert type(class_attribs["spark"]) == SparkSession


@pytest.mark.glue_job_manager
@pytest.mark.init_job
def test_metodo_de_inicializacao_do_job_gera_atributo_do_tipo_job(job_manager):
    """
    G: dado que deseja-se inicializar um job Glue pela classe GlueJobManager
    W: quando o método init_job() for chamado
    T: então deve existir um atributo "job" do tipo awsglue.job.Job no objeto
       da classe
    """

    # Executando método de inicialização do job
    job_manager.init_job()

    # Coletando atributos da classe
    class_attribs = job_manager.__dict__

    assert "job" in class_attribs.keys()
    assert type(class_attribs["job"]) is Job
