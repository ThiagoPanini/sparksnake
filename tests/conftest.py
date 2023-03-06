"""Arquivo de gerenciamento de insumos do pytest.

Neste arquivo, serão consolidados elementos essenciais
para aplicação de testes unitários, como fixtures.

___
"""

# Importando bibliotecas
import sys
import pytest

from gluesnake.manager import GlueJobManager

from tests.helpers.user_inputs import FAKE_ARGV_LIST, FAKE_DATA_DICT


# Objeto pré configurado da classe GlueJobManager
@pytest.fixture()
def job_manager() -> GlueJobManager:
    # Adicionando argumentos de sistema
    for fake_arg in FAKE_ARGV_LIST:
        sys.argv.append(f"--{fake_arg}=a-fake-arg-value")

    # Instanciando objeto da classe
    job_manager = GlueJobManager(
        argv_list=FAKE_ARGV_LIST,
        data_dict=FAKE_DATA_DICT
    )

    return job_manager
