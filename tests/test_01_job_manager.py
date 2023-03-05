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
from tests.helpers.user_input import FAKE_ARGV_LIST


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
