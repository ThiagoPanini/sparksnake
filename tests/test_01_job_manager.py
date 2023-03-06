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
def test_mensagem_inicial_de_log_esperada_ao_executar_metodo(
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
    expected_log_msg = "Iniciando execução de job a-fake-arg-value. "\
        "Origens presentes no processo de ETL:\n\n"\
        "Tabela some-fake-database.orders-fake-table sem push down predicate "\
        "definido\n"\
        "Tabela some-fake-database.customers-fake-table com push down "\
        "predicate definido por anomesdia=20221201\n"

    # Executando método para escrita de mensagem de log
    with caplog.at_level(logging.INFO):
        job_manager.job_initial_log_message()

    assert caplog.records[-1].message == expected_log_msg
